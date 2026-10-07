"""
Transporte RabbitMQ (AMQP 0-9-1) da camada de comunicação RCO <-> IA.

Topologia (declarada de forma idempotente pelos dois lados):

    exchange "rco.ai" (direct, durável)
      ├── rk "analysis.request"           -> fila "rco.ai.requests"          (lida pelo parceiro)
      └── rk "analysis.response.<cell>"   -> fila "rco.ai.responses.<cell>"  (lida pelo RCO)
    exchange "rco.ai.dlx" (fanout)        -> fila "rco.ai.dead"  (pedidos expirados ou ilegíveis)

  - Os pedidos levam 'expiration' = timeout do RCO: se o parceiro estiver parado, o pedido
    expira no broker e vai para "rco.ai.dead" em vez de ser analisado tarde demais.
  - O RCO descarta respostas de pedidos que já não está à espera (evento AI_STALE_RESPONSE_DISCARDED).
  - O parceiro corre o modelo numa thread para manter o heartbeat da ligação vivo.
"""
from __future__ import annotations

import concurrent.futures as cf
import json
import os
import time
from typing import Callable, Optional

import pika
from pika.exceptions import AMQPError, UnroutableError

from .client import EV_STALE, AIRequestStateMachine, BaseAIClient, TransportError
from .messages import AIRequest, MessageFormatError
from .service import Analyzer, error_response, run_analysis

EXCHANGE = "rco.ai"
DLX = "rco.ai.dlx"
DEAD_QUEUE = "rco.ai.dead"
REQUEST_QUEUE = "rco.ai.requests"
RK_REQUEST = "analysis.request"
MAX_MESSAGE_BYTES = 20 * 1024 * 1024


def response_queue(cell_id: str) -> str:
    return f"rco.ai.responses.{cell_id}"


def response_rk(cell_id: str) -> str:
    return f"analysis.response.{cell_id}"


def declare_common(ch):
    ch.exchange_declare(EXCHANGE, exchange_type="direct", durable=True)
    ch.exchange_declare(DLX, exchange_type="fanout", durable=True)
    ch.queue_declare(DEAD_QUEUE, durable=True)
    ch.queue_bind(DEAD_QUEUE, DLX)
    ch.queue_declare(REQUEST_QUEUE, durable=True, arguments={"x-dead-letter-exchange": DLX})
    ch.queue_bind(REQUEST_QUEUE, EXCHANGE, routing_key=RK_REQUEST)


# --- Modo de teste por lotes: filas sem expiração (as mensagens esperam dias se for preciso)
BATCH_REQUEST_QUEUE = "rco.ai.batch.requests"
RK_BATCH_REQUEST = "batch.request"


def batch_response_queue(cell_id: str) -> str:
    return f"rco.ai.batch.responses.{cell_id}"


def batch_response_rk(cell_id: str) -> str:
    return f"batch.response.{cell_id}"


def declare_batch(ch, cell_id: Optional[str] = None):
    ch.exchange_declare(EXCHANGE, exchange_type="direct", durable=True)
    ch.exchange_declare(DLX, exchange_type="fanout", durable=True)
    ch.queue_declare(DEAD_QUEUE, durable=True)
    ch.queue_bind(DEAD_QUEUE, DLX)
    ch.queue_declare(BATCH_REQUEST_QUEUE, durable=True, arguments={"x-dead-letter-exchange": DLX})
    ch.queue_bind(BATCH_REQUEST_QUEUE, EXCHANGE, routing_key=RK_BATCH_REQUEST)
    if cell_id:
        q = batch_response_queue(cell_id)
        ch.queue_declare(q, durable=True)
        ch.queue_bind(q, EXCHANGE, routing_key=batch_response_rk(cell_id))
        return q


def declare_response_queue(ch, cell_id: str):
    q = response_queue(cell_id)
    # respostas não lidas ao fim de 24 h são descartadas (o RCO já não as espera)
    ch.queue_declare(q, durable=True, arguments={"x-message-ttl": 24 * 3600 * 1000})
    ch.queue_bind(q, EXCHANGE, routing_key=response_rk(cell_id))
    return q


def connection_params(url: str, heartbeat: int = 30) -> pika.URLParameters:
    p = pika.URLParameters(url)
    p.heartbeat = heartbeat
    p.blocked_connection_timeout = 30
    p.connection_attempts = 3
    p.retry_delay = 2
    p.client_properties = {"connection_name": "rco-comm"}
    return p


def _text(v):
    return v.decode("utf-8", "replace") if isinstance(v, bytes) else v


# ---------------------------------------------------------------------------
# Lado RCO
# ---------------------------------------------------------------------------
class RabbitMQAIClient(BaseAIClient):
    def __init__(self, url: str, cell_id: str = "cell_01", **kw):
        super().__init__(**kw)
        self.url = url
        self.cell_id = cell_id
        self._conn: Optional[pika.BlockingConnection] = None
        self._ch = None

    def _ensure(self):
        if self._conn is not None and self._conn.is_open and self._ch is not None and self._ch.is_open:
            return
        self.close()
        try:
            self._conn = pika.BlockingConnection(connection_params(self.url))
            self._ch = self._conn.channel()
            declare_common(self._ch)
            self._queue = declare_response_queue(self._ch, self.cell_id)
            self._ch.confirm_delivery()
            self._ch.basic_qos(prefetch_count=10)
        except AMQPError as e:
            self.close()
            raise TransportError(f"não foi possível ligar ao broker: {type(e).__name__} {e}")

    def _transmit(self, request: AIRequest):
        body = request.to_json()
        if len(body) > MAX_MESSAGE_BYTES:
            raise TransportError(f"mensagem demasiado grande ({len(body)} bytes)")
        props = pika.BasicProperties(
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=2,
            correlation_id=request.request_id,
            message_id=request.message_id,
            reply_to=response_rk(self.cell_id),
            expiration=str(int(self.timeout_s * 1000)),
            type=request.to_dict()["message_type"],
            headers={"schema_version": request.to_dict()["schema_version"],
                     "object_id": request.object_id, "attempt": request.attempt},
        )
        for tentativa in (1, 2):  # uma religação automática se a ligação caiu
            try:
                self._ensure()
                self._ch.basic_publish(EXCHANGE, RK_REQUEST, body, props, mandatory=True)
                return
            except UnroutableError:
                raise TransportError("pedido não encaminhado (fila de pedidos inexistente)")
            except (AMQPError, OSError) as e:
                self.close()
                if tentativa == 2:
                    raise TransportError(f"falha ao publicar: {type(e).__name__} {e}")

    def _receive(self, request: AIRequest, timeout_s: float,
                 sm: AIRequestStateMachine) -> Optional[bytes]:
        fim = time.monotonic() + timeout_s
        try:
            self._ensure()
            for method, props, body in self._ch.consume(self._queue, inactivity_timeout=0.25):
                if method is None:
                    if time.monotonic() >= fim:
                        return None
                    continue
                self._ch.basic_ack(method.delivery_tag)
                rid = props.correlation_id
                if not rid:
                    try:
                        rid = json.loads(body).get("request_id")
                    except ValueError:
                        rid = None
                if rid == request.request_id:
                    return body
                sm.emit(EV_STALE, stale_request_id=rid)
                if time.monotonic() >= fim:
                    return None
        except (AMQPError, OSError) as e:
            self.close()
            raise TransportError(f"ligação perdida à espera da resposta: {type(e).__name__} {e}")
        finally:
            try:
                if self._ch is not None and self._ch.is_open:
                    self._ch.cancel()
            except (AMQPError, OSError):
                self.close()
        return None

    def close(self):
        try:
            if self._conn is not None and self._conn.is_open:
                self._conn.close()
        except (AMQPError, OSError):
            pass
        self._conn = self._ch = None


# ---------------------------------------------------------------------------
# Lado parceiro (serviço de IA)
# ---------------------------------------------------------------------------
class AIServiceWorker:
    """
    Consome pedidos de "rco.ai.requests", chama o analisador e publica a resposta
    na routing key indicada em reply_to.
    """

    def __init__(self, url: str, analyzer: Analyzer, save_dir: Optional[str] = None,
                 heartbeat: int = 30, log: Callable[[str], None] = print,
                 batch: bool = False, idle_exit_s: Optional[float] = None):
        """
        batch=True       -> lê a fila de lotes (rco.ai.batch.requests) em vez da de tempo real
        idle_exit_s=N    -> termina sozinho quando não há pedidos há N segundos
        """
        self.url = url
        self.analyzer = analyzer
        self.save_dir = save_dir
        self.heartbeat = heartbeat
        self.log = log
        self.queue = BATCH_REQUEST_QUEUE if batch else REQUEST_QUEUE
        self._declare = declare_batch if batch else declare_common
        self.idle_exit_s = idle_exit_s
        self._stop = False
        self._pool = cf.ThreadPoolExecutor(max_workers=1)
        self.handled = 0

    def stop(self):
        self._stop = True

    def run_forever(self, reconnect_delay_s: float = 5.0):
        while not self._stop:
            try:
                self._run_once()
            except (AMQPError, OSError) as e:
                if self._stop:
                    break
                self.log(f"[ligação] perdida ({type(e).__name__}); nova tentativa em {reconnect_delay_s:.0f}s")
                time.sleep(reconnect_delay_s)

    def _run_once(self):
        conn = pika.BlockingConnection(connection_params(self.url, self.heartbeat))
        try:
            ch = conn.channel()
            self._declare(ch)
            ch.basic_qos(prefetch_count=1)
            ch.confirm_delivery()
            pendentes = ch.queue_declare(self.queue, passive=True).method.message_count
            self.log(f"À escuta em '{self.queue}' ({pendentes} pedido(s) pendente(s)). Ctrl+C para sair.")
            ultimo = time.monotonic()
            for method, props, body in ch.consume(self.queue, inactivity_timeout=1.0):
                if self._stop:
                    break
                if method is None:
                    if self.idle_exit_s is not None and time.monotonic() - ultimo >= self.idle_exit_s:
                        self.log(f"Sem pedidos há {self.idle_exit_s:.0f}s: a terminar "
                                 f"({self.handled} tratado(s)).")
                        self._stop = True
                        break
                    continue
                self._handle(conn, ch, method, props, body)
                ultimo = time.monotonic()
            try:
                ch.cancel()
            except AMQPError:
                pass
        finally:
            if conn.is_open:
                conn.close()

    def _handle(self, conn, ch, method, props, body):
        reply_to = props.reply_to
        try:
            req = AIRequest.from_json(body)
        except MessageFormatError as e:
            self.log(f"[pedido ilegível] {e}")
            rid = props.correlation_id
            if reply_to and rid:
                resp = error_response(rid, _text((props.headers or {}).get("object_id")) or "", "",
                                      "INVALID_REQUEST", str(e), retryable=False)
                self._publish(ch, reply_to, resp)
                ch.basic_ack(method.delivery_tag)
            else:
                ch.basic_reject(method.delivery_tag, requeue=False)  # -> rco.ai.dead
            return

        self.log(f"Pedido {req.request_id} (tentativa {req.attempt}): object={req.object_id} "
                 f"image={req.image.image_id} ({len(req.image.data) / 1024:.0f} KB)")
        self._save(req.request_id, req.image.image_id, req.image.encoding, req.image.data)

        # o modelo corre numa thread; o ciclo mantém a ligação viva (heartbeats)
        fut = self._pool.submit(run_analysis, req, self.analyzer)
        while not fut.done():
            conn.sleep(0.2)
        resp = fut.result()

        if reply_to:
            self._publish(ch, reply_to, resp)
        ch.basic_ack(method.delivery_tag)
        self.handled += 1
        self._save(req.request_id, "resposta", "json", resp.to_json(indent=2))
        if resp.status == "ok":
            self.log(f"  -> {resp.analysis_id}: {len(resp.points)} ponto(s), conf={resp.confidence} "
                     f"({resp.processing_ms} ms)")
        else:
            self.log(f"  -> ERRO {resp.error.code}: {resp.error.message}")

    def _publish(self, ch, reply_to, resp):
        ch.basic_publish(
            EXCHANGE, reply_to, resp.to_json(),
            pika.BasicProperties(content_type="application/json", content_encoding="utf-8",
                                 delivery_mode=2, correlation_id=resp.request_id,
                                 message_id=resp.message_id, type="AI_ANALYSIS_RESPONSE"))

    def _save(self, request_id, name, ext, data: bytes):
        if not self.save_dir:
            return
        os.makedirs(self.save_dir, exist_ok=True)
        ext = {"jpeg": "jpg"}.get(ext, ext)
        safe = "".join(c for c in f"{request_id}_{name}" if c.isalnum() or c in "-_.")
        with open(os.path.join(self.save_dir, f"{safe}.{ext}"), "wb") as f:
            f.write(data)
