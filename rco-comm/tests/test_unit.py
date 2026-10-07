"""Testes sem rede: contrato de mensagens, validação e máquina de estados (MockAIClient)."""
import io
import json
import os
import sys

import pytest
from PIL import Image, ImageDraw

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rco_comm import (AIRequest, AIResponse, AIState, ImagePayload, MessageFormatError,  # noqa: E402
                      MockAIClient, ValidationRules, validate_response)
from rco_comm.service import run_analysis  # noqa: E402
from rco_comm.simulated_ai import InvalidImageError, analisar_imagem  # noqa: E402


def planta_jpeg(w=800, h=600):
    im = Image.new("RGB", (w, h), (150, 120, 90))
    d = ImageDraw.Draw(im)
    d.line([(400, 480), (410, 350), (395, 220), (405, 110)], fill=(40, 140, 40), width=14)
    for x, y in [(330, 340), (470, 260), (330, 180), (470, 130)]:
        d.ellipse([x - 60, y - 22, x + 60, y + 22], fill=(60, 170, 60))
    b = io.BytesIO()
    im.save(b, "JPEG")
    return b.getvalue()


def pedido(data=None):
    data = data or planta_jpeg()
    return AIRequest(object_id="PLANT-000123",
                     image=ImagePayload("IMG-00891", "intervention_camera_01", "jpeg", data, 800, 600))


# ---------------------------------------------------------------- contrato
def test_request_roundtrip():
    r = pedido()
    r2 = AIRequest.from_json(r.to_json())
    assert r2.request_id == r.request_id and r2.image.data == r.image.data
    assert r2.object_id == "PLANT-000123" and r2.frame_id == "intervention_camera"


def test_request_corrompido_detectado():
    d = pedido().to_dict()
    d["image"]["sha256"] = "0" * 64
    with pytest.raises(MessageFormatError, match="sha256"):
        AIRequest.from_json(json.dumps(d).encode())


def test_versao_incompativel():
    d = pedido().to_dict()
    d["schema_version"] = "2.0"
    with pytest.raises(MessageFormatError, match="schema_version"):
        AIRequest.from_json(json.dumps(d).encode())


def test_response_roundtrip_e_formato_relatorio():
    req = pedido()
    resp = run_analysis(req, lambda img, r: analisar_imagem(img))
    d = json.loads(resp.to_json())
    for k in ("object_id", "analysis_id", "intervention_type", "confidence", "points", "frame_id", "timestamp"):
        assert k in d, k
    r2 = AIResponse.from_json(resp.to_json())
    assert r2.analysis_id == resp.analysis_id and len(r2.points) == len(resp.points) >= 2


def test_ponto_sem_coordenadas_rejeitado():
    req = pedido()
    resp = run_analysis(req, lambda img, r: {"intervention_type": "cut", "confidence": 0.9,
                                             "points": [{"id": 1, "type": "cut_node", "confidence": 0.9}]})
    assert resp.status == "error" and resp.error.code == "MODEL_OUTPUT_INVALID"


def test_imagem_invalida_erro_definitivo():
    req = pedido(b"nao sou imagem")
    resp = run_analysis(req, lambda img, r: analisar_imagem(img))
    assert resp.status == "error" and resp.error.code == "INVALID_IMAGE" and not resp.error.retryable


def test_excecao_do_modelo_erro_temporario():
    def mau(img, r):
        raise RuntimeError("GPU sem memória")
    resp = run_analysis(pedido(), mau)
    assert resp.error.code == "MODEL_FAILURE" and resp.error.retryable


# ---------------------------------------------------------------- validação
def resposta_ok(req, **over):
    out = {"intervention_type": "cut", "confidence": 0.94,
           "points": [{"id": 1, "type": "cut_node", "pixel": {"u": 423, "v": 218}, "confidence": 0.96},
                      {"id": 2, "type": "cut_node", "pixel": {"u": 510, "v": 276}, "confidence": 0.91}]}
    out.update(over)
    return run_analysis(req, lambda img, r: out)


def test_validacao_ok():
    req = pedido()
    assert validate_response(resposta_ok(req), req).valid


@pytest.mark.parametrize("over,trecho", [
    ({"confidence": 0.3}, "confidence fora"),
    ({"intervention_type": "paint"}, "intervention_type"),
    ({"frame_id": "camara_misteriosa"}, "frame_id"),
    ({"points": []}, "nenhum ponto"),
    ({"points": [{"id": 1, "type": "cut_node", "pixel": {"u": 9999, "v": 10}, "confidence": 0.9}]}, "fora da imagem"),
    ({"points": [{"id": 1, "type": "leaf", "pixel": {"u": 10, "v": 10}, "confidence": 0.9}]}, "tipo inválido"),
    ({"points": [{"id": 1, "type": "cut_node", "pixel": {"u": 10, "v": 10}, "confidence": 0.1}]}, "abaixo do mínimo"),
    ({"points": [{"id": 1, "type": "cut_node", "position": {"x": 9, "y": 0, "z": 0}, "confidence": 0.9}]},
     "espaço de trabalho"),
    ({"points": [{"id": 1, "type": "cut_node", "pixel": {"u": 10, "v": 10}, "confidence": 0.9,
                  "orientation": {"qx": 1, "qy": 1, "qz": 0, "qw": 0}}]}, "quaternião"),
])
def test_validacao_rejeita(over, trecho):
    req = pedido()
    v = validate_response(resposta_ok(req, **over), req)
    assert not v.valid and any(trecho in e for e in v.errors), v.errors


def test_validacao_object_id_trocado():
    req = pedido()
    resp = resposta_ok(req)
    resp.object_id = "PLANT-999"
    assert not validate_response(resp, req).valid


def test_extra_checks_do_rco():
    req = pedido()
    robot_b_livre = False
    v = validate_response(resposta_ok(req), req,
                          extra_checks=[lambda r: None if robot_b_livre else "Robot B indisponível"])
    assert not v.valid and "Robot B indisponível" in v.errors


# ---------------------------------------------------------------- máquina de estados
def estados(res):
    return [s for s, _ in res.history]


def test_fluxo_normal():
    eventos = []
    res = MockAIClient(on_event=lambda e: eventos.append(e.name)).analyze(pedido())
    assert res.state == AIState.COMPLETED and res.attempts == 1
    assert estados(res) == ["READY", "REQUEST_CREATED", "REQUEST_SENT", "WAITING_RESPONSE",
                            "RESPONSE_RECEIVED", "VALIDATING", "VALID", "COMPLETED"]
    assert eventos == ["AI_ANALYSIS_REQUESTED", "AI_ANALYSIS_RECEIVED", "AI_ANALYSIS_VALIDATED"]


def test_timeout_depois_sucesso():
    res = MockAIClient(faults=["timeout"]).analyze(pedido())
    assert res.ok and res.attempts == 2 and "TIMEOUT" in estados(res)


def test_timeout_esgota_tentativas():
    res = MockAIClient(faults=["timeout"] * 3, max_attempts=3).analyze(pedido())
    assert res.state == AIState.FAILED and res.attempts == 3 and res.response is None


def test_erro_transporte_repetido_e_recupera():
    res = MockAIClient(faults=["transport", "transport"], max_attempts=3).analyze(pedido())
    assert res.ok and res.attempts == 3


def test_erro_temporario_da_ia_repete():
    res = MockAIClient(faults=["error"]).analyze(pedido())
    assert res.ok and res.attempts == 2


def test_erro_definitivo_nao_repete():
    res = MockAIClient(faults=["fatal"], max_attempts=3).analyze(pedido())
    assert res.state == AIState.FAILED and res.attempts == 1


def test_resposta_fora_do_contrato_invalid():
    res = MockAIClient(faults=["garbage"]).analyze(pedido())
    assert res.state == AIState.INVALID and res.attempts == 1


def test_resposta_que_falha_validacao_nao_repete():
    res = MockAIClient(analyzer=lambda img, r: {"intervention_type": "cut", "confidence": 0.1,
                                                "points": [{"id": 1, "type": "cut_node",
                                                            "pixel": {"u": 1, "v": 1}, "confidence": 0.9}]}
                       ).analyze(pedido())
    assert res.state == AIState.INVALID and res.attempts == 1
