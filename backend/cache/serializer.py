# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import json
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def _default_encoder(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return {"__type__": "datetime", "value": value.isoformat()}
    if isinstance(value, uuid.UUID):
        return {"__type__": "uuid", "value": str(value)}
    if isinstance(value, Decimal):
        return {"__type__": "decimal", "value": str(value)}
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _object_hook(value: dict[str, Any]) -> Any:
    marker = value.get("__type__")
    if marker == "datetime":
        raw = value.get("value")
        if isinstance(raw, str):
            try:
                return datetime.fromisoformat(raw)
            except Exception:
                return raw
    if marker == "uuid":
        raw = value.get("value")
        if isinstance(raw, str):
            try:
                return uuid.UUID(raw)
            except Exception:
                return raw
    if marker == "decimal":
        raw = value.get("value")
        if isinstance(raw, str):
            try:
                return Decimal(raw)
            except Exception:
                return raw
    return value


def dumps(payload: Any) -> str:
    return json.dumps(payload, default=_default_encoder, separators=(",", ":"))


def loads(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        return json.loads(raw, object_hook=_object_hook)
    return raw
