"""
Validação da resposta da IA (secção 12 do relatório).

A IA é um componente externo e probabilístico: nenhuma resposta é aceite sem validação.
Aqui ficam as verificações que dependem só do pedido e da resposta. As condições da célula
(Robot B disponível, zona segura) são verificadas pelo RCO através de `extra_checks`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional

from .messages import AIRequest, AIResponse, is_finite


@dataclass
class ValidationRules:
    known_frames: set = field(default_factory=lambda: {"intervention_camera"})
    allowed_intervention_types: set = field(default_factory=lambda: {"cut"})
    allowed_point_types: set = field(default_factory=lambda: {"cut_node"})
    min_confidence: float = 0.70          # confiança global mínima
    min_point_confidence: float = 0.50    # confiança mínima de cada ponto
    min_points: int = 1
    max_points: int = 10
    # limites de trabalho em 3D (metros, no frame_id), se a IA enviar 'position'
    workspace_min: tuple = (-2.0, -2.0, 0.0)
    workspace_max: tuple = (2.0, 2.0, 3.0)


@dataclass
class ValidationResult:
    valid: bool
    errors: list

    def __bool__(self):
        return self.valid


def validate_response(resp: AIResponse, req: AIRequest,
                      rules: Optional[ValidationRules] = None,
                      extra_checks: Iterable[Callable[[AIResponse], Optional[str]]] = ()
                      ) -> ValidationResult:
    r = rules or ValidationRules()
    e = []

    # Identificação
    if resp.request_id != req.request_id:
        e.append(f"request_id não corresponde ({resp.request_id} != {req.request_id})")
    if resp.object_id != req.object_id:
        e.append(f"object_id inválido ({resp.object_id} != {req.object_id})")
    if resp.image_id != req.image.image_id:
        e.append(f"image_id não corresponde ({resp.image_id} != {req.image.image_id})")
    if not resp.analysis_id:
        e.append("analysis_id em falta")

    if resp.status != "ok":
        e.append(f"IA devolveu erro: {resp.error.code if resp.error else '?'}")
        return ValidationResult(False, e)

    # Conteúdo
    if resp.intervention_type not in r.allowed_intervention_types:
        e.append(f"intervention_type inválido: {resp.intervention_type}")
    if resp.frame_id not in r.known_frames:
        e.append(f"frame_id desconhecido: {resp.frame_id}")
    if resp.confidence is None or not (r.min_confidence <= resp.confidence <= 1.0):
        e.append(f"confidence fora dos limites: {resp.confidence} (mín. {r.min_confidence})")

    n = len(resp.points)
    if n == 0:
        e.append("nenhum ponto de intervenção")
    elif not (r.min_points <= n <= r.max_points):
        e.append(f"número de pontos inaceitável: {n} (permitido {r.min_points}-{r.max_points})")

    # dimensões de referência para validar píxeis
    w = resp.image_width or req.image.width
    h = resp.image_height or req.image.height
    if req.image.width and resp.image_width and \
            (resp.image_width, resp.image_height) != (req.image.width, req.image.height):
        e.append("image_size da resposta difere da imagem enviada")

    ids = set()
    for p in resp.points:
        tag = f"ponto {p.id}"
        if p.id in ids:
            e.append(f"{tag}: id repetido")
        ids.add(p.id)
        if p.type not in r.allowed_point_types:
            e.append(f"{tag}: tipo inválido '{p.type}'")
        if not (r.min_point_confidence <= p.confidence <= 1.0):
            e.append(f"{tag}: confidence {p.confidence} abaixo do mínimo {r.min_point_confidence}")
        if p.pixel is not None:
            if not is_finite(p.pixel.u, p.pixel.v):
                e.append(f"{tag}: pixel não numérico")
            elif w and h and not (0 <= p.pixel.u < w and 0 <= p.pixel.v < h):
                e.append(f"{tag}: pixel ({p.pixel.u}, {p.pixel.v}) fora da imagem {w}x{h}")
        if p.position is not None:
            xyz = (p.position.x, p.position.y, p.position.z)
            if not is_finite(*xyz):
                e.append(f"{tag}: position não numérica")
            elif not all(lo <= v <= hi for v, lo, hi in zip(xyz, r.workspace_min, r.workspace_max)):
                e.append(f"{tag}: position {xyz} fora do espaço de trabalho")
        if p.orientation is not None:
            q = (p.orientation.qx, p.orientation.qy, p.orientation.qz, p.orientation.qw)
            if not is_finite(*q) or abs(sum(c * c for c in q) - 1.0) > 0.05:
                e.append(f"{tag}: quaternião não normalizado")

    # Condições da célula (Robot B disponível, zona segura, ...) fornecidas pelo RCO
    for check in extra_checks:
        msg = check(resp)
        if msg:
            e.append(msg)

    return ValidationResult(not e, e)
