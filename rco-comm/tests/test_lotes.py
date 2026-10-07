"""Modo de teste por lotes + conversão de coordenadas."""
import os
import sys
import threading

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pika  # noqa: E402

from rco_comm.batch import BatchReceiver, BatchSender, PendingStore  # noqa: E402
from rco_comm.coordenadas import Calibration, response_to_robot  # noqa: E402
from rco_comm.rabbitmq import (BATCH_REQUEST_QUEUE, DEAD_QUEUE, AIServiceWorker,  # noqa: E402
                               batch_response_queue)
from rco_comm.service import run_analysis  # noqa: E402
from rco_comm.simulated_ai import analisar_imagem  # noqa: E402
from test_unit import pedido, planta_jpeg  # noqa: E402

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
URL = os.environ.get("AMQP_TEST_URL", "amqp://guest:guest@localhost:5672/%2F")


# ---------------------------------------------------------------- coordenadas (sem rede)
def cal():
    return Calibration.load(os.path.join(RAIZ, "calibracao_exemplo.json"))


def test_centro_da_imagem_fica_no_eixo_da_camara():
    c = cal()
    assert c.pixel_to_camera(512, 384) == (0.0, 0.0, 0.5)
    # câmara em (0.40, 0, 0.80) a olhar para baixo: plano a 0.5 m -> z = 0.30 no braço
    x, y, z = c.camera_to_robot((0.0, 0.0, 0.5))
    assert (round(x, 6), round(y, 6), round(z, 6)) == (0.4, 0.0, 0.3)


def test_pixel_para_braco_e_escala_de_resolucao():
    c = cal()
    # 90 px à direita do centro a 0.5 m com fx=900 -> 5 cm no eixo x da câmara -> -y no braço
    x, y, z = c.camera_to_robot(c.pixel_to_camera(602, 384))
    assert abs(y - (-0.05)) < 1e-9 and abs(x - 0.40) < 1e-9
    # a mesma posição relativa numa imagem com metade da resolução dá o mesmo ponto
    assert c.pixel_to_camera(301, 192, 512, 384) == pytest.approx(c.pixel_to_camera(602, 384))


def test_resposta_convertida():
    req = pedido(planta_jpeg())
    resp = run_analysis(req, lambda img, r: analisar_imagem(img))
    robot = response_to_robot(resp, cal())
    assert robot["frame_id"] == "robot_b_base" and len(robot["points"]) == len(resp.points)
    assert all(abs(p["position"]["z"] - 0.30) < 1e-6 for p in robot["points"])


def test_frame_errado_recusado():
    req = pedido()
    resp = run_analysis(req, lambda img, r: {**analisar_imagem(img), "frame_id": "outra_camara"})
    with pytest.raises(ValueError):
        response_to_robot(resp, cal())


# ---------------------------------------------------------------- lotes (com broker)
def _broker_ok():
    try:
        pika.BlockingConnection(pika.URLParameters(URL)).close()
        return True
    except Exception:
        return False


broker = pytest.mark.skipif(not _broker_ok(), reason="RabbitMQ de teste inacessível")


@pytest.fixture
def limpo(tmp_path):
    c = pika.BlockingConnection(pika.URLParameters(URL))
    ch = c.channel()
    for q in (BATCH_REQUEST_QUEUE, DEAD_QUEUE, batch_response_queue("cell_test")):
        ch.queue_delete(q)
    c.close()
    return PendingStore(str(tmp_path / "pendentes"))


def enviar(store, n):
    s = BatchSender(URL, "cell_test", store)
    ids = []
    for i in range(n):
        r = pedido(planta_jpeg())
        s.send(r, "img.jpg")
        ids.append(r.request_id)
    s.close()
    return ids


def parceiro_corre_e_sai():
    w = AIServiceWorker(URL, lambda img, req: analisar_imagem(img, req.image.sha256),
                        batch=True, idle_exit_s=1.5, log=lambda m: None)
    t = threading.Thread(target=w.run_forever, daemon=True)
    t.start()
    t.join(30)
    assert not t.is_alive(), "o parceiro devia terminar quando a fila fica vazia"
    return w


@broker
def test_dia1_envio_dia2_parceiro_dia3_rececao(limpo):
    store = limpo
    ids = enviar(store, 3)                      # dia 1: enviamos; o parceiro está desligado
    assert sorted(store.list()) == sorted(ids)

    rx = BatchReceiver(URL, "cell_test", store)
    assert rx.collect(wait_s=1.0) == []         # ainda nada para receber

    w = parceiro_corre_e_sai()                  # dia 2: o parceiro liga, processa tudo e sai
    assert w.handled == 3

    assert rx.pending_on_broker() == 3          # dia 3: as respostas esperaram por nós
    res = rx.collect(wait_s=1.0)
    assert sorted(r.request_id for r in res) == sorted(ids)
    assert all(r.known and r.validation.valid for r in res), [r.validation.errors for r in res]
    assert store.list() == [] and rx.pending_on_broker() == 0


@broker
def test_resposta_invalida_detetada_em_lote(limpo):
    store = limpo
    enviar(store, 1)

    def mau(img, req):
        out = analisar_imagem(img)
        out["confidence"] = 0.1
        return out

    w = AIServiceWorker(URL, mau, batch=True, idle_exit_s=1.0, log=lambda m: None)
    w.run_forever()
    res = BatchReceiver(URL, "cell_test", store).collect(wait_s=1.0)
    assert len(res) == 1 and not res[0].validation.valid
    assert any("confidence" in e for e in res[0].validation.errors)


@broker
def test_resposta_de_pedido_desconhecido(limpo):
    store = limpo
    enviar(store, 1)
    for f in os.listdir(store.folder):          # simula perda do registo local
        os.remove(os.path.join(store.folder, f))
    parceiro_corre_e_sai()
    res = BatchReceiver(URL, "cell_test", store).collect(wait_s=1.0)
    assert len(res) == 1 and res[0].known is False
