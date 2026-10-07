"""
Lado da IA (parceiro): transforma o resultado do analisador numa AIResponse do contrato.

O parceiro só implementa uma função:

    def analisar(imagem: bytes, pedido: AIRequest) -> dict:
        return {
            "intervention_type": "cut",
            "confidence": 0.94,
            "points": [{"id": 1, "type": "cut_node", "pixel": {"u": 423, "v": 218}, "confidence": 0.96}],
            # opcionais:
            "frame_id": "intervention_camera",
            "image_size": {"width": 1024, "height": 768},
            "model": {"name": "...", "version": "..."},
        }

Erros:
    raise InvalidImageError("...")  -> erro definitivo (o RCO não repete)
    qualquer outra exceção          -> erro temporário (o RCO pode repetir)
"""
from __future__ import annotations

import json
import time
import uuid
from typing import Callable

from .messages import (AIError, AIRequest, AIResponse, MessageFormatError, MSG_RESPONSE,
                       SCHEMA_VERSION, new_id, utc_now)
from .simulated_ai import InvalidImageError

Analyzer = Callable[[bytes, AIRequest], dict]


def error_response(request_id: str, object_id: str, image_id: str,
                   code: str, message: str, retryable: bool) -> AIResponse:
    return AIResponse(request_id=request_id, object_id=object_id, image_id=image_id,
                      status="error", error=AIError(code, message, retryable))


def run_analysis(req: AIRequest, analyzer: Analyzer) -> AIResponse:
    t0 = time.monotonic()
    try:
        out = analyzer(req.image.data, req)
    except InvalidImageError as e:
        return error_response(req.request_id, req.object_id, req.image.image_id,
                              "INVALID_IMAGE", str(e), retryable=False)
    except Exception as e:  # falha do modelo: pode valer a pena repetir
        return error_response(req.request_id, req.object_id, req.image.image_id,
                              "MODEL_FAILURE", f"{type(e).__name__}: {e}", retryable=True)

    ms = int((time.monotonic() - t0) * 1000)
    if not isinstance(out, dict):
        return error_response(req.request_id, req.object_id, req.image.image_id,
                              "MODEL_OUTPUT_INVALID", "o analisador não devolveu um dict", False)

    # Monta a resposta e confirma que respeita o contrato ANTES de a enviar
    size = out.get("image_size") or {"width": req.image.width, "height": req.image.height}
    d = {
        "schema_version": SCHEMA_VERSION,
        "message_type": MSG_RESPONSE,
        "message_id": uuid.uuid4().hex,
        "analysis_id": out.get("analysis_id") or new_id("ANALYSIS"),
        "timestamp": utc_now(),
        "request_id": req.request_id,
        "object_id": req.object_id,
        "image_id": req.image.image_id,
        "status": "ok",
        "intervention_type": out.get("intervention_type"),
        "confidence": out.get("confidence"),
        "frame_id": out.get("frame_id", req.frame_id),
        "image_size": size if size.get("width") else None,
        "points": out.get("points", []),
        "model": out.get("model"),
        "processing_ms": ms,
    }
    try:
        resp = AIResponse.from_json(json.dumps({k: v for k, v in d.items() if v is not None}).encode())
    except MessageFormatError as e:
        return error_response(req.request_id, req.object_id, req.image.image_id,
                              "MODEL_OUTPUT_INVALID", f"saída do analisador fora do contrato: {e}", False)
    return resp
