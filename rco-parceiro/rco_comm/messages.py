"""
Modelo de mensagens RCO <-> IA externa (contrato v1.0).

Alinhado com as secções 11 e 12 do relatório "Arquitetura do Robot Cell Orchestrator":
  - AIRequest  : pedido de análise da planta na zona de intervenção (inclui a imagem);
  - AIResponse : pontos de intervenção devolvidos pela IA (ou um erro).

As mensagens são JSON UTF-8, independentes do transporte (RabbitMQ hoje, REST/ROS 2 amanhã).
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

SCHEMA_VERSION = "1.0"
MSG_REQUEST = "AI_ANALYSIS_REQUEST"
MSG_RESPONSE = "AI_ANALYSIS_RESPONSE"


class MessageFormatError(ValueError):
    """A mensagem não respeita o contrato (campos em falta, tipos errados, JSON inválido)."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12].upper()}"


def _req(d: dict, key: str, typ, ctx: str):
    if not isinstance(d, dict) or key not in d or d[key] is None:
        raise MessageFormatError(f"campo obrigatório em falta: {ctx}{key}")
    v = d[key]
    if typ is float and isinstance(v, int) and not isinstance(v, bool):
        v = float(v)
    if not isinstance(v, typ) or (typ in (int, float) and isinstance(v, bool)):
        raise MessageFormatError(f"tipo inválido em {ctx}{key}: esperado {typ.__name__}")
    return v


def _opt(d: dict, key: str, typ, ctx: str, default=None):
    if not isinstance(d, dict) or d.get(key) is None:
        return default
    return _req(d, key, typ, ctx)


# ---------------------------------------------------------------------------
# Tipos geométricos
# ---------------------------------------------------------------------------
@dataclass
class Pixel:
    u: float  # coluna (x), origem no canto superior esquerdo
    v: float  # linha (y), para baixo


@dataclass
class Position:
    x: float
    y: float
    z: float


@dataclass
class Orientation:
    qx: float
    qy: float
    qz: float
    qw: float


@dataclass
class InterventionPoint:
    id: int
    type: str                                  # ex.: "cut_node"
    confidence: float
    pixel: Optional[Pixel] = None              # coordenadas na imagem
    position: Optional[Position] = None        # 3D no frame_id (se a IA tiver profundidade)
    orientation: Optional[Orientation] = None  # orientação da ferramenta (opcional)

    @staticmethod
    def from_dict(d: dict, i: int) -> "InterventionPoint":
        ctx = f"points[{i}]."
        p = InterventionPoint(
            id=_req(d, "id", int, ctx),
            type=_req(d, "type", str, ctx),
            confidence=_req(d, "confidence", float, ctx),
        )
        if d.get("pixel") is not None:
            px = d["pixel"]
            p.pixel = Pixel(_req(px, "u", float, ctx + "pixel."), _req(px, "v", float, ctx + "pixel."))
        if d.get("position") is not None:
            ps = d["position"]
            p.position = Position(*(_req(ps, k, float, ctx + "position.") for k in "xyz"))
        if d.get("orientation") is not None:
            o = d["orientation"]
            p.orientation = Orientation(*(_req(o, k, float, ctx + "orientation.")
                                          for k in ("qx", "qy", "qz", "qw")))
        if p.pixel is None and p.position is None:
            raise MessageFormatError(f"{ctx}: cada ponto precisa de 'pixel' ou 'position'")
        return p


# ---------------------------------------------------------------------------
# Pedido
# ---------------------------------------------------------------------------
@dataclass
class ImagePayload:
    image_id: str
    camera_id: str
    encoding: str          # "jpeg" | "png"
    data: bytes            # bytes da imagem (vai em base64 no JSON)
    width: Optional[int] = None
    height: Optional[int] = None
    sha256: str = ""

    def __post_init__(self):
        if not self.sha256:
            self.sha256 = hashlib.sha256(self.data).hexdigest()


@dataclass
class AIRequest:
    object_id: str
    image: ImagePayload
    object_type: str = "plant"
    frame_id: str = "intervention_camera"
    request_id: str = field(default_factory=lambda: new_id("AI_REQUEST"))
    attempt: int = 1
    timestamp: str = field(default_factory=utc_now)
    deadline: str = ""     # até quando o RCO espera a resposta (informativo para a IA)
    message_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def set_deadline(self, timeout_s: float):
        self.deadline = (datetime.now(timezone.utc) + timedelta(seconds=timeout_s)) \
            .isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def to_dict(self) -> dict:
        return {
            "schema_version": SCHEMA_VERSION,
            "message_type": MSG_REQUEST,
            "message_id": self.message_id,
            "request_id": self.request_id,
            "attempt": self.attempt,
            "object_id": self.object_id,
            "image": {
                "image_id": self.image.image_id,
                "camera_id": self.image.camera_id,
                "encoding": self.image.encoding,
                "width": self.image.width,
                "height": self.image.height,
                "sha256": self.image.sha256,
                "data_base64": base64.b64encode(self.image.data).decode("ascii"),
            },
            "object_context": {"object_type": self.object_type, "frame_id": self.frame_id},
            "timestamp": self.timestamp,
            "deadline": self.deadline or None,
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), ensure_ascii=False).encode("utf-8")

    @staticmethod
    def from_json(raw: bytes) -> "AIRequest":
        try:
            d = json.loads(raw)
        except (ValueError, UnicodeDecodeError) as e:
            raise MessageFormatError(f"JSON inválido: {e}")
        if not isinstance(d, dict):
            raise MessageFormatError("o pedido tem de ser um objeto JSON")
        if d.get("message_type") != MSG_REQUEST:
            raise MessageFormatError(f"message_type inesperado: {d.get('message_type')}")
        _check_version(d)
        img = _req(d, "image", dict, "")
        try:
            data = base64.b64decode(_req(img, "data_base64", str, "image."), validate=True)
        except (ValueError, TypeError):
            raise MessageFormatError("image.data_base64 não é base64 válido")
        sha = _req(img, "sha256", str, "image.")
        if hashlib.sha256(data).hexdigest() != sha:
            raise MessageFormatError("sha256 da imagem não coincide (imagem corrompida)")
        ctx = _opt(d, "object_context", dict, "", {}) or {}
        return AIRequest(
            object_id=_req(d, "object_id", str, ""),
            image=ImagePayload(
                image_id=_req(img, "image_id", str, "image."),
                camera_id=_req(img, "camera_id", str, "image."),
                encoding=_req(img, "encoding", str, "image."),
                data=data,
                width=_opt(img, "width", int, "image."),
                height=_opt(img, "height", int, "image."),
                sha256=sha,
            ),
            object_type=ctx.get("object_type", "plant"),
            frame_id=ctx.get("frame_id", "intervention_camera"),
            request_id=_req(d, "request_id", str, ""),
            attempt=_opt(d, "attempt", int, "", 1),
            timestamp=_req(d, "timestamp", str, ""),
            deadline=_opt(d, "deadline", str, "", "") or "",
            message_id=_opt(d, "message_id", str, "", "") or "",
        )


# ---------------------------------------------------------------------------
# Resposta
# ---------------------------------------------------------------------------
@dataclass
class AIError:
    code: str          # ex.: "INVALID_IMAGE", "MODEL_FAILURE", "OBJECT_NOT_FOUND"
    message: str
    retryable: bool = False


@dataclass
class AIResponse:
    request_id: str
    object_id: str
    image_id: str
    status: str                                   # "ok" | "error"
    analysis_id: str = field(default_factory=lambda: new_id("ANALYSIS"))
    intervention_type: Optional[str] = None       # ex.: "cut"
    confidence: Optional[float] = None
    frame_id: Optional[str] = None
    image_width: Optional[int] = None
    image_height: Optional[int] = None
    points: list = field(default_factory=list)    # list[InterventionPoint]
    model_name: Optional[str] = None
    model_version: Optional[str] = None
    processing_ms: Optional[int] = None
    error: Optional[AIError] = None
    timestamp: str = field(default_factory=utc_now)
    message_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def to_dict(self) -> dict:
        d = {
            "schema_version": SCHEMA_VERSION,
            "message_type": MSG_RESPONSE,
            "message_id": self.message_id,
            "request_id": self.request_id,
            "object_id": self.object_id,
            "image_id": self.image_id,
            "analysis_id": self.analysis_id,
            "status": self.status,
            "intervention_type": self.intervention_type,
            "confidence": self.confidence,
            "frame_id": self.frame_id,
            "image_size": ({"width": self.image_width, "height": self.image_height}
                           if self.image_width else None),
            "points": [_clean(asdict(p)) for p in self.points],
            "model": ({"name": self.model_name, "version": self.model_version}
                      if self.model_name else None),
            "processing_ms": self.processing_ms,
            "error": asdict(self.error) if self.error else None,
            "timestamp": self.timestamp,
        }
        return {k: v for k, v in d.items() if v is not None}

    def to_json(self, indent: Optional[int] = None) -> bytes:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent).encode("utf-8")

    @staticmethod
    def from_json(raw: bytes) -> "AIResponse":
        try:
            d = json.loads(raw)
        except (ValueError, UnicodeDecodeError) as e:
            raise MessageFormatError(f"JSON inválido: {e}")
        if not isinstance(d, dict):
            raise MessageFormatError("a resposta tem de ser um objeto JSON")
        if d.get("message_type") != MSG_RESPONSE:
            raise MessageFormatError(f"message_type inesperado: {d.get('message_type')}")
        _check_version(d)
        status = _req(d, "status", str, "")
        if status not in ("ok", "error"):
            raise MessageFormatError(f"status inválido: {status}")
        size = _opt(d, "image_size", dict, "", {}) or {}
        model = _opt(d, "model", dict, "", {}) or {}
        err = None
        if status == "error":
            e = _req(d, "error", dict, "")
            err = AIError(_req(e, "code", str, "error."), _req(e, "message", str, "error."),
                          bool(e.get("retryable", False)))
        pts_raw = _opt(d, "points", list, "", []) or []
        return AIResponse(
            request_id=_req(d, "request_id", str, ""),
            object_id=_req(d, "object_id", str, ""),
            image_id=_req(d, "image_id", str, ""),
            status=status,
            analysis_id=_opt(d, "analysis_id", str, "", "") or "",
            intervention_type=_opt(d, "intervention_type", str, ""),
            confidence=_opt(d, "confidence", float, ""),
            frame_id=_opt(d, "frame_id", str, ""),
            image_width=_opt(size, "width", int, "image_size."),
            image_height=_opt(size, "height", int, "image_size."),
            points=[InterventionPoint.from_dict(p, i) for i, p in enumerate(pts_raw)],
            model_name=model.get("name"),
            model_version=model.get("version"),
            processing_ms=_opt(d, "processing_ms", int, ""),
            error=err,
            timestamp=_opt(d, "timestamp", str, "", "") or "",
            message_id=_opt(d, "message_id", str, "", "") or "",
        )


def _check_version(d: dict):
    v = str(d.get("schema_version", ""))
    if v.split(".")[0] != SCHEMA_VERSION.split(".")[0]:
        raise MessageFormatError(f"schema_version incompatível: {v!r} (esperado {SCHEMA_VERSION})")


def _clean(o: Any):
    """Remove chaves com None (recursivo) para JSON mais limpo."""
    if isinstance(o, dict):
        return {k: _clean(v) for k, v in o.items() if v is not None}
    return o


def is_finite(*vals) -> bool:
    return all(isinstance(v, (int, float)) and math.isfinite(v) for v in vals)
