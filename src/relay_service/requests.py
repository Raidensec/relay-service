from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
from dataclasses import dataclass

from relay_service.config import Settings
from relay_service.errors import RequestError


@dataclass(frozen=True)
class ParsedRequest:
    caller_id: str
    idempotency_key: str
    destination: str
    payload_json: str
    request_hash: str


def authenticate(authorization: str | None, secret: str) -> bool:
    if authorization is None or not authorization.startswith("Bearer "):
        return False
    return hmac.compare_digest(authorization[7:].encode("utf-8"), secret.encode("utf-8"))


def parse_event_request(
    headers: dict[str, str], raw_body: bytes, settings: Settings
) -> ParsedRequest:
    if len(raw_body) > settings.request_limit_bytes:
        raise RequestError(413, "request body is too large")
    caller_id = headers.get("x-caller-id", "")
    idempotency_key = headers.get("idempotency-key", "")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", caller_id):
        raise RequestError(400, "X-Caller-ID is required and must be valid")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,199}", idempotency_key):
        raise RequestError(400, "Idempotency-Key is required and must be valid")
    try:
        decoded = raw_body.decode("utf-8")
        request = json.loads(
            decoded,
            parse_constant=reject_non_json_constant,
            parse_float=parse_finite_float,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise RequestError(400, "request body must be valid JSON") from exc
    if not isinstance(request, dict) or set(request) != {"destination", "payload"}:
        raise RequestError(400, "body must contain only destination and payload")
    destination = request["destination"]
    payload = request["payload"]
    if not isinstance(destination, str) or destination not in settings.destinations:
        raise RequestError(400, "destination is not configured")
    if not isinstance(payload, dict):
        raise RequestError(400, "payload must be a JSON object")
    try:
        payload_json = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        canonical_request = json.dumps(
            {"destination": destination, "payload": payload},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except UnicodeEncodeError as exc:
        raise RequestError(400, "payload must contain valid Unicode text") from exc
    request_hash = hashlib.sha256(canonical_request).hexdigest()
    return ParsedRequest(caller_id, idempotency_key, destination, payload_json, request_hash)


def reject_non_json_constant(constant: str) -> None:
    raise ValueError(f"invalid JSON constant: {constant}")


def parse_finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("JSON numbers must be finite")
    return parsed
