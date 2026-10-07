"""Camada de comunicação RCO <-> IA externa (Robot Cell Orchestrator)."""
from .client import (AIState, AnalysisResult, BaseAIClient, Event, IAIClient, MockAIClient,
                     TransportError)
from .messages import (AIError, AIRequest, AIResponse, ImagePayload, InterventionPoint,
                       MessageFormatError, SCHEMA_VERSION)
from .validation import ValidationResult, ValidationRules, validate_response

__all__ = [
    "AIState", "AnalysisResult", "BaseAIClient", "Event", "IAIClient", "MockAIClient",
    "TransportError", "AIError", "AIRequest", "AIResponse", "ImagePayload", "InterventionPoint",
    "MessageFormatError", "SCHEMA_VERSION", "ValidationResult", "ValidationRules",
    "validate_response",
]
