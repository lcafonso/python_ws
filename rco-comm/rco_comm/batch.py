"""
Modo de teste por lotes (assíncrono).

    RCO:      BatchSender.send()      -> envia e termina; guarda o pedido em 'pendentes/'
    Parceiro: AIServiceWorker(batch=True) -> processa quando for ligado
    RCO:      BatchReceiver.collect() -> recolhe as respostas que houver, valida-as
                                         contra o pedido guardado e grava os resultados

Usa o mesmo contrato (AIRequest/AIResponse) e a mesma validação do modo em tempo real,
mas as filas não têm expiração: as mensagens esperam no broker o tempo que for preciso.
"""
from __future__ import annotations

import glob
import json
import os
import time
from dataclasses import dataclass
from typing import Optional

import pika
from pika.exceptions import AMQPError, UnroutableError

from .client import TransportError
from .messages import AIRequest, AIResponse, ImagePayload, MessageFormatError, utc_now
from .rabbitmq import (EXCHANGE, MAX_MESSAGE_BYTES, RK_BATCH_REQUEST, batch_response_rk,
                       connection_params, declare_batch)
from .validation import ValidationResult, ValidationRules, validate_response


class PendingStore:
    """Guarda em disco os pedidos enviados (sem a imagem) para validar as respostas mais tarde."""

    def __init__(self, folder: str = "pendentes"):
        self.folder = folder
        os.makedirs(folder, exist_ok=True)

    def _path(self, request_id):
        return os.path.join(self.folder, f"{request_id}.json")

    def add(self, req: AIRequest, image_path: str):
        d = req.to_dict()
        d["image"].pop("data_base64")
        d["_local"] = {"image_path": os.path.abspath(image_path), "sent_at": utc_now()}
        with open(self._path(req.request_id), "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2, ensure_ascii=False)

    def get(self, request_id: str) -> Optional[tuple]:
        p = self._path(request_id)
        if not os.path.exists(p):
            return None
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
        img = d["image"]
        req = AIRequest(
            object_id=d["object_id"],
            image=ImagePayload(img["image_id"], img["camera_id"], img["encoding"], b"",
                               img.get("width"), img.get("height"), sha256=img["sha256"]),
            object_type=d["object_context"]["object_type"],
            frame_id=d["object_context"]["frame_id"],
            request_id=d["request_id"], timestamp=d["timestamp"])
        return req, d["_local"]

    def remove(self, request_id: str):
        try:
            os.remove(self._path(request_id))
        except FileNotFoundError:
            pass

    def list(self) -> list:
        return sorted(os.path.splitext(os.path.basename(p))[0]
                      for p in glob.glob(os.path.join(self.folder, "*.json")))


class BatchSender:
    def __init__(self, url: str, cell_id: str = "cell_01", store: Optional[PendingStore] = None):
        self.url, self.cell_id = url, cell_id
        self.store = store or PendingStore()
        try:
            self.conn = pika.BlockingConnection(connection_params(url))
            self.ch = self.conn.channel()
            declare_batch(self.ch, cell_id)
            self.ch.confirm_delivery()
        except AMQPError as e:
            raise TransportError(f"não foi possível ligar ao broker: {type(e).__name__} {e}")

    def send(self, req: AIRequest, image_path: str):
        body = req.to_json()
        if len(body) > MAX_MESSAGE_BYTES:
            raise TransportError(f"imagem demasiado grande ({len(body)} bytes)")
        try:
            self.ch.basic_publish(
                EXCHANGE, RK_BATCH_REQUEST, body,
                pika.BasicProperties(content_type="application/json", content_encoding="utf-8",
                                     delivery_mode=2, correlation_id=req.request_id,
                                     message_id=req.message_id, reply_to=batch_response_rk(self.cell_id),
                                     type="AI_ANALYSIS_REQUEST",
                                     headers={"object_id": req.object_id, "mode": "batch"}),
                mandatory=True)
        except UnroutableError:
            raise TransportError("pedido não encaminhado")
        except AMQPError as e:
            raise TransportError(f"falha ao publicar: {type(e).__name__} {e}")
        self.store.add(req, image_path)  # só depois de o broker confirmar a receção

    def close(self):
        if self.conn.is_open:
            self.conn.close()


@dataclass
class CollectedResult:
    request_id: str
    response: Optional[AIResponse]
    validation: Optional[ValidationResult]
    local: Optional[dict]            # caminho da imagem original, hora de envio
    known: bool                      # o pedido estava nos pendentes?
    format_error: Optional[str] = None
    raw: bytes = b""


class BatchReceiver:
    def __init__(self, url: str, cell_id: str = "cell_01", store: Optional[PendingStore] = None,
                 rules: Optional[ValidationRules] = None):
        self.url, self.cell_id = url, cell_id
        self.store = store or PendingStore()
        self.rules = rules or ValidationRules()

    def pending_on_broker(self) -> int:
        conn = pika.BlockingConnection(connection_params(self.url))
        try:
            ch = conn.channel()
            q = declare_batch(ch, self.cell_id)
            return ch.queue_declare(q, passive=True).method.message_count
        finally:
            conn.close()

    def collect(self, wait_s: float = 3.0, on_result=None) -> list:
        """
        Recolhe todas as respostas disponíveis. Termina quando não chega nada há `wait_s` segundos
        (wait_s=None: fica à escuta até Ctrl+C).
        """
        out = []
        try:
            conn = pika.BlockingConnection(connection_params(self.url))
        except AMQPError as e:
            raise TransportError(f"não foi possível ligar ao broker: {type(e).__name__} {e}")
        try:
            ch = conn.channel()
            q = declare_batch(ch, self.cell_id)
            ch.basic_qos(prefetch_count=20)
            ultimo = time.monotonic()
            for method, props, body in ch.consume(q, inactivity_timeout=0.5):
                if method is None:
                    if wait_s is not None and time.monotonic() - ultimo >= wait_s:
                        break
                    continue
                r = self._process(props, body)
                if on_result:
                    on_result(r)  # grava resultados ANTES do ack
                ch.basic_ack(method.delivery_tag)
                if r.known:
                    self.store.remove(r.request_id)
                out.append(r)
                ultimo = time.monotonic()
            ch.cancel()
        finally:
            if conn.is_open:
                conn.close()
        return out

    def _process(self, props, body) -> CollectedResult:
        rid = props.correlation_id
        try:
            resp = AIResponse.from_json(body)
            rid = resp.request_id
        except MessageFormatError as e:
            return CollectedResult(rid or "desconhecido", None, None, None,
                                   bool(rid and self.store.get(rid)), str(e), body)
        found = self.store.get(rid)
        if not found:
            return CollectedResult(rid, resp, None, None, False, raw=body)
        req, local = found
        return CollectedResult(rid, resp, validate_response(resp, req, self.rules), local, True, raw=body)
