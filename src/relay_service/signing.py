from __future__ import annotations

import hashlib
import hmac
import re
import time


def sign_delivery(secret: str, timestamp: str, body: bytes) -> str:
    signed_message = timestamp.encode("ascii") + b"." + body
    digest = hmac.new(secret.encode("utf-8"), signed_message, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def verify_delivery_signature(
    secret: str,
    timestamp: str,
    body: bytes,
    signature: str,
    now: int | None = None,
    max_skew_seconds: int = 300,
) -> bool:
    if not re.fullmatch(r"[0-9]{1,12}", timestamp):
        return False
    if not re.fullmatch(r"sha256=[0-9a-f]{64}", signature):
        return False
    try:
        issued_at = int(timestamp)
    except ValueError:
        return False
    if abs((int(time.time()) if now is None else now) - issued_at) > max_skew_seconds:
        return False
    return hmac.compare_digest(sign_delivery(secret, timestamp, body), signature)
