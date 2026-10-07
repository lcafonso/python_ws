"""
Testes de integração com um RabbitMQ real.
    AMQP_TEST_URL=amqp://guest:guest@localhost:5672/%2F pytest tests/test_rabbitmq.py
São ignorados se o broker não estiver acessível. NÃO correr contra o broker de produção
(os testes apagam as filas rco.ai.*).
"""
import json
import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pika  # noqa: E402

from rco_comm import AIState  # noqa: E402
from rco_comm.rabbitmq import (DEAD_QUEUE, EXCHANGE, REQUEST_QUEUE, RK_REQUEST, AIServiceWorker,  # noqa: E402
                               RabbitMQAIClient, declare_common, declare_response_queue, response_queue,
                               response_rk)
from rco_comm.simulated_ai import analisar_imagem  # noqa: E402
from test_unit import pedido  # noqa: E402

URL = os.environ.get("AMQP_TEST_URL", "amqp://guest:guest@localhost:5672/%2F")


def _broker_ok():
    try:
        pika.BlockingConnection(pika.URLParameters(URL)).close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _broker_ok(), reason="RabbitMQ de teste inacessível")


@pytest.fixture(autouse=True)
def filas_limpas():
    c = pika.BlockingConnection(pika.URLParameters(URL))
    ch = c.channel()
    for q in (REQUEST_QUEUE, DEAD_QUEUE, response_queue("cell_test")):
        ch.queue_delete(q)
    c.close()
    yield


def contar(queue):
    c = pika.BlockingConnection(pika.URLParameters(URL))
    n = c.channel().queue_declare(queue, passive=True).method.message_count
    c.close()
    return n


class Parceiro:
    def __init__(self, atraso=0.0, heartbeat=30):
        self.atraso = atraso
        self.w = AIServiceWorker(URL, self.analisar, heartbeat=heartbeat, log=lambda m: None)
        self.t = threading.Thread(target=self.w.run_forever, kwargs={"reconnect_delay_s": 0.5}, daemon=True)

    def analisar(self, img, req):
        time.sleep(self.atraso)
        return analisar_imagem(img, req.image.sha256)

    def __enter__(self):
        self.t.start()
        time.sleep(0.8)
        return self

    def __exit__(self, *a):
        self.w.stop()
        self.t.join(5)
        assert not self.t.is_alive(), "worker não parou"


def esperar_tratados(worker, n, limite=3.0):
    """O parceiro conta o pedido logo depois de publicar a resposta: dar-lhe tempo."""
    fim = time.time() + limite
    while worker.handled < n and time.time() < fim:
        time.sleep(0.05)
    return worker.handled


def cliente(**kw):
    kw.setdefault("timeout_s", 10)
    kw.setdefault("retry_delay_s", 0.1)
    return RabbitMQAIClient(URL, cell_id="cell_test", **kw)


def test_fluxo_completo():
    eventos = []
    with Parceiro(), cliente(on_event=lambda e: eventos.append(e.name)) as c:
        res = c.analyze(pedido())
    assert res.state == AIState.COMPLETED, res.errors
    assert len(res.response.points) >= 2 and res.response.object_id == "PLANT-000123"
    assert eventos == ["AI_ANALYSIS_REQUESTED", "AI_ANALYSIS_RECEIVED", "AI_ANALYSIS_VALIDATED"]


def test_varios_pedidos_seguidos():
    with Parceiro(), cliente() as c:
        resultados = [c.analyze(pedido()) for _ in range(5)]
    assert all(r.ok for r in resultados)
    assert len({r.response.analysis_id for r in resultados}) == 5


def test_parceiro_desligado_timeout_e_pedidos_expiram():
    with cliente(timeout_s=1.5, max_attempts=2) as c:
        res = c.analyze(pedido())
    assert res.state == AIState.FAILED and res.attempts == 2
    assert [s for s, _ in res.history].count("TIMEOUT") == 2
    time.sleep(0.5)
    # os pedidos não ficaram à espera de serem analisados tarde demais
    assert contar(REQUEST_QUEUE) == 0 and contar(DEAD_QUEUE) == 2


def test_resposta_atrasada_e_descartada():
    eventos = []
    with Parceiro(atraso=2.0), cliente(timeout_s=1.0, max_attempts=1,
                                       on_event=lambda e: eventos.append(e)) as c:
        r1 = c.analyze(pedido())          # expira antes de o parceiro responder
        assert r1.state == AIState.FAILED
        c.timeout_s = 10
        r2 = c.analyze(pedido())          # a resposta antiga chega primeiro e é ignorada
    assert r2.ok and r2.response.request_id == r2.request_id
    stale = [e for e in eventos if e.name == "AI_STALE_RESPONSE_DISCARDED"]
    assert stale and stale[0].data["stale_request_id"] == r1.request_id


def test_inferencia_longa_mantem_ligacao():
    # heartbeat de 2 s e análise de 7 s: sem a thread, a ligação do parceiro cairia
    with Parceiro(atraso=7.0, heartbeat=2) as p, cliente(timeout_s=15, max_attempts=1) as c:
        r1 = c.analyze(pedido())
        r2 = c.analyze(pedido())
        assert esperar_tratados(p.w, 2) == 2
    assert r1.ok and r2.ok


def test_pedido_mal_formado():
    with Parceiro():
        conn = pika.BlockingConnection(pika.URLParameters(URL))
        ch = conn.channel()
        declare_common(ch)
        q = declare_response_queue(ch, "cell_test")
        # com reply_to: o parceiro responde com erro INVALID_REQUEST
        ch.basic_publish(EXCHANGE, RK_REQUEST, b'{"lixo": true}',
                         pika.BasicProperties(correlation_id="AI_REQUEST-X", reply_to=response_rk("cell_test")))
        # sem reply_to: vai para a dead letter queue
        ch.basic_publish(EXCHANGE, RK_REQUEST, b"nem json")
        time.sleep(1.5)
        m, p, body = ch.basic_get(q, auto_ack=True)
        conn.close()
    r = json.loads(body)
    assert r["status"] == "error" and r["error"]["code"] == "INVALID_REQUEST" and not r["error"]["retryable"]
    assert contar(DEAD_QUEUE) == 1


def test_rco_religa_apos_perder_ligacao():
    with Parceiro(), cliente() as c:
        assert c.analyze(pedido()).ok
        c._conn.close()                   # simula queda da ligação
        assert c.analyze(pedido()).ok


def test_parceiro_religa_apos_perder_ligacao():
    with Parceiro() as p, cliente() as c:
        assert c.analyze(pedido()).ok
        # fecha todas as ligações do lado do broker (como se a rede caísse)
        os.system("rabbitmqctl -q close_all_connections teste >/dev/null 2>&1")
        time.sleep(2.0)
        res = c.analyze(pedido())
        assert res.ok, res.errors
        assert esperar_tratados(p.w, 2) == 2
