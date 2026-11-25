"""Shared Data Contracts and canonical event schemas."""

from src.common.schemas.cpc_event import (
    ALLOWED_FIELDS,
    REQUIRED_NON_EMPTY_STRING_FIELDS,
    SUPPORTED_SCHEMA_VERSION,
    VALID_EVENT_TYPES,
    CpcEventV1,
    ErrorType,
    EventType,
    ValidationErrorDetail,
    ValidationResult,
    load_json_schema,
    validate_event,
)

__all__ = [
    "ALLOWED_FIELDS",
    "CpcEventV1",
    "ErrorType",
    "EventType",
    "REQUIRED_NON_EMPTY_STRING_FIELDS",
    "SUPPORTED_SCHEMA_VERSION",
    "VALID_EVENT_TYPES",
    "ValidationErrorDetail",
    "ValidationResult",
    "load_json_schema",
    "validate_event",
]
