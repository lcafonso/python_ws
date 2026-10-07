"""
Lado do RCO: interface IAIClient e máquina de estados do pedido à IA (secções 18 e 21).

    READY -> REQUEST_CREATED -> REQUEST_SENT -> WAITING_RESPONSE -> RESPONSE_RECEIVED
          -> VALIDATING -> VALID -> COMPLETED
                        -> INVALID                       (final: nenhum comando ao Robot B)
    REQUEST_SENT / WAITING_RESPONSE -> TIMEOUT | AI_ERROR -> (retry) REQUEST_SENT
                                                          -> FAILED  (tentativas esgotadas)

O RCO usa sempre IAIClient. Trocar MockAIClient por RabbitMQAIClient (ou um futuro
cliente REST/ROS 2) não altera a máquina de estados global do RCO.
"""
from __future__ import annotations

import abc
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Iterable, Optional

from .messages import AIRequest, AIResponse, MessageFormatError, utc_now
from .validation import ValidationRules, validate_response


class AIState(str, Enum):
    READY = "READY"
    REQUEST_CREATED = "REQUEST_CREATED"
    REQUEST_SENT = "REQUEST_SENT"
    WAITING_RESPONSE = "WAITING_RESPONSE"
    RESPONSE_RECEIVED = "RESPONSE_RECEIVED"
    VALIDATING = "VALIDATING"
    VALID = "VALID"
    INVALID = "INVALID"
    TIMEOUT = "TIMEOUT"
    AI_ERROR = "AI_ERROR"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


S = AIState
TRANSITIONS = {
    S.READY: {S.REQUEST_CREATED},
    S.REQUEST_CREATED: {S.REQUEST_SENT, S.AI_ERROR},
    S.REQUEST_SENT: {S.WAITING_RESPONSE, S.AI_ERROR},
    S.WAITING_RESPONSE: {S.RESPONSE_RECEIVED, S.TIMEOUT, S.AI_ERROR},
    S.RESPONSE_RECEIVED: {S.VALIDATING, S.AI_ERROR, S.INVALID},
    S.VALIDATING: {S.VALID, S.INVALID},
    S.VALID: {S.COMPLETED},
    S.TIMEOUT: {S.REQUEST_SENT, S.AI_ERROR, S.FAILED},
    S.AI_ERROR: {S.REQUEST_SENT, S.AI_ERROR, S.FAILED},  # AI_ERROR->AI_ERROR: reenvio falhou
    S.INVALID: set(),
    S.COMPLETED: set(),
    S.FAILED: set(),
}
FINAL_STATES = {S.COMPLETED, S.INVALID, S.FAILED}

# Eventos publicados para o RCO (secção 15)
EV_REQUESTED = "AI_ANALYSIS_REQUESTED"
EV_RECEIVED = "AI_ANALYSIS_RECEIVED"
EV_VALIDATED = "AI_ANALYSIS_VALIDATED"
EV_INVALID = "AI_ANALYSIS_INVALID"
EV_TIMEOUT = "AI_ANALYSIS_TIMEOUT"
EV_ERROR = "ERROR_OCCURRED"
EV_STALE = "AI_STALE_RESPONSE_DISCARDED"


class TransportError(Exception):
    """Falha de comunicação (broker inacessível, ligação perdida, mensagem não encaminhada)."""


class IllegalTransition(RuntimeError):
    pass


@dataclass
class Event:
    name: str
    request_id: str
    data: dict = field(default_factory=dict)
    timestamp: str = field(default_factory=utc_now)


EventHandler = Callable[[Event], None]


class AIRequestStateMachine:
    def __init__(self, request_id: str, on_event: Optional[EventHandler] = None):
        self.request_id = request_id
        self.state = S.READY
        self.history = [(S.READY, utc_now())]
        self._on_event = on_event

    def to(self, new: AIState):
        if new not in TRANSITIONS[self.state]:
            raise IllegalTransition(f"{self.state.value} -> {new.value} não é permitido")
        self.state = new
        self.history.append((new, utc_now()))

    def emit(self, name: str, **data):
        if self._on_event:
            self._on_event(Event(name, self.request_id, data))


@dataclass
class AnalysisResult:
    request_id: str
    state: AIState                     # COMPLETED | INVALID | FAILED
    response: Optional[AIResponse]
    errors: list
    attempts: int
    history: list

    @property
    def ok(self) -> bool:
        return self.state == S.COMPLETED


class IAIClient(abc.ABC):
    """Abstração usada pelo RCO para pedir análises à IA (secção 18)."""

    @abc.abstractmethod
    def analyze(self, request: AIRequest) -> AnalysisResult:
        ...

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class BaseAIClient(IAIClient):
    """
    Implementa a máquina de estados, retries e validação.
    As subclasses só implementam o transporte: _transmit() e _receive().
    """

    def __init__(self, timeout_s: float = 30.0, max_attempts: int = 3, retry_delay_s: float = 1.0,
                 rules: Optional[ValidationRules] = None,
                 extra_checks: Iterable[Callable[[AIResponse], Optional[str]]] = (),
                 on_event: Optional[EventHandler] = None):
        self.timeout_s = timeout_s
        self.max_attempts = max_attempts
        self.retry_delay_s = retry_delay_s
        self.rules = rules or ValidationRules()
        self.extra_checks = list(extra_checks)
        self.on_event = on_event

    # --- transporte (a implementar) -------------------------------------------------
    @abc.abstractmethod
    def _transmit(self, request: AIRequest) -> None:
        """Envia o pedido. Levanta TransportError em caso de falha."""

    @abc.abstractmethod
    def _receive(self, request: AIRequest, timeout_s: float,
                 sm: AIRequestStateMachine) -> Optional[bytes]:
        """Espera pela resposta deste request_id. Devolve None se expirar."""

    # --- fluxo ------------------------------------------------------------------------
    def analyze(self, request: AIRequest) -> AnalysisResult:
        sm = AIRequestStateMachine(request.request_id, self.on_event)
        sm.to(S.REQUEST_CREATED)
        errors: list = []
        attempt = 0

        while True:
            attempt += 1
            request.attempt = attempt
            request.message_id = uuid.uuid4().hex
            request.set_deadline(self.timeout_s)

            try:
                self._transmit(request)
                sm.to(S.REQUEST_SENT)
                sm.emit(EV_REQUESTED, attempt=attempt, object_id=request.object_id,
                        image_id=request.image.image_id)
                sm.to(S.WAITING_RESPONSE)
                raw = self._receive(request, self.timeout_s, sm)
            except TransportError as e:
                errors.append(f"tentativa {attempt}: erro de comunicação: {e}")
                sm.to(S.AI_ERROR)
                sm.emit(EV_ERROR, attempt=attempt, error=str(e), retryable=True)
                if self._give_up(sm, attempt):
                    return self._result(request, sm, None, errors, attempt)
                continue

            if raw is None:
                errors.append(f"tentativa {attempt}: sem resposta em {self.timeout_s:.0f}s")
                sm.to(S.TIMEOUT)
                sm.emit(EV_TIMEOUT, attempt=attempt)
                if self._give_up(sm, attempt):
                    return self._result(request, sm, None, errors, attempt)
                continue

            sm.to(S.RESPONSE_RECEIVED)
            try:
                resp = AIResponse.from_json(raw)
            except MessageFormatError as e:
                errors.append(f"resposta fora do contrato: {e}")
                sm.to(S.INVALID)
                sm.emit(EV_INVALID, errors=errors)
                return self._result(request, sm, None, errors, attempt)
            sm.emit(EV_RECEIVED, analysis_id=resp.analysis_id, status=resp.status,
                    points=len(resp.points))

            if resp.status == "error":
                err = resp.error
                errors.append(f"tentativa {attempt}: IA devolveu {err.code}: {err.message}")
                sm.to(S.AI_ERROR)
                sm.emit(EV_ERROR, attempt=attempt, error=err.code, retryable=err.retryable)
                if not err.retryable or self._give_up(sm, attempt, already_in_error=True):
                    if sm.state != S.FAILED:
                        sm.to(S.FAILED)
                    return self._result(request, sm, resp, errors, attempt)
                continue

            sm.to(S.VALIDATING)
            v = validate_response(resp, request, self.rules, self.extra_checks)
            if v.valid:
                sm.to(S.VALID)
                sm.emit(EV_VALIDATED, analysis_id=resp.analysis_id, points=len(resp.points))
                sm.to(S.COMPLETED)
            else:
                errors.extend(v.errors)
                sm.to(S.INVALID)
                sm.emit(EV_INVALID, analysis_id=resp.analysis_id, errors=v.errors)
            return self._result(request, sm, resp, errors, attempt)

    def _give_up(self, sm: AIRequestStateMachine, attempt: int, already_in_error=False) -> bool:
        if attempt >= self.max_attempts:
            sm.to(S.FAILED)
            return True
        time.sleep(self.retry_delay_s * attempt)  # backoff linear
        return False

    @staticmethod
    def _result(request, sm, resp, errors, attempts) -> AnalysisResult:
        return AnalysisResult(request.request_id, sm.state, resp, errors, attempts,
                              [(s.value, t) for s, t in sm.history])


# ---------------------------------------------------------------------------
# Mock (Fase 1): sem rede, com injeção de falhas para testar o RCO
# ---------------------------------------------------------------------------
class MockAIClient(BaseAIClient):
    """
    faults: sequência de comportamentos por tentativa, ex. ["timeout", "ok"].
      "ok"        -> analisa normalmente
      "timeout"   -> não responde
      "transport" -> erro de comunicação
      "error"     -> IA devolve erro temporário (retryable)
      "fatal"     -> IA devolve erro definitivo
      "garbage"   -> resposta que não é JSON do contrato
    Depois de esgotada a lista, usa "ok".
    """

    def __init__(self, analyzer=None, faults=(), **kw):
        kw.setdefault("retry_delay_s", 0.0)
        super().__init__(**kw)
        from .simulated_ai import analisar_imagem
        self.analyzer = analyzer or (lambda img, req: analisar_imagem(img, req.image.sha256))
        self.faults = list(faults)
        self._pending: Optional[AIRequest] = None

    def _next_fault(self):
        return self.faults.pop(0) if self.faults else "ok"

    def _transmit(self, request):
        self._fault = self._next_fault()
        if self._fault == "transport":
            raise TransportError("broker simulado indisponível")
        self._pending = request

    def _receive(self, request, timeout_s, sm):
        from .service import error_response, run_analysis
        f = self._fault
        if f == "timeout":
            return None
        if f == "garbage":
            return b'{"isto": "nao e o contrato"}'
        if f in ("error", "fatal"):
            return error_response(request.request_id, request.object_id, request.image.image_id,
                                  "MODEL_FAILURE", "falha simulada", retryable=(f == "error")).to_json()
        return run_analysis(request, self.analyzer).to_json()
