from __future__ import annotations

import os
import time
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from relay_service.config import Destination, Settings
from relay_service.errors import DeliveryFailure
from relay_service.signing import sign_delivery
from relay_service.store import EventClaim


class RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


def load_destination_secret(destination: Destination, mode: str) -> str:
    if mode == "local":
        variable_name = destination.secret_ref.removeprefix("env:")
        secret = os.environ.get(variable_name, "")
        if not secret:
            raise RuntimeError(
                f"destination secret environment variable {variable_name} is required"
            )
        return secret
    import boto3

    secret_response = boto3.client("secretsmanager").get_secret_value(
        SecretId=destination.secret_ref
    )
    secret = secret_response.get("SecretString")
    if not isinstance(secret, str) or not secret:
        raise RuntimeError("destination secret must be stored as a non-empty SecretString")
    return secret


def send_delivery(claim: EventClaim, destination: Destination, settings: Settings) -> int:
    secret = load_destination_secret(destination, settings.mode)
    body = claim.payload_json.encode("utf-8")
    timestamp = str(int(time.time()))
    request = Request(destination.url, data=body, method="POST")
    request.add_header("Content-Type", "application/json")
    request.add_header("X-Relay-Timestamp", timestamp)
    request.add_header("X-Relay-Signature", sign_delivery(secret, timestamp, body))
    request.add_header("X-Relay-Event-ID", claim.event_id)
    request.add_header("X-Relay-Generation", str(claim.generation))
    opener = build_opener(RejectRedirects)
    try:
        with opener.open(request, timeout=settings.request_timeout_seconds) as response:
            status = response.getcode()
    except HTTPError as exc:
        exc.close()
        retryable = exc.code in {408, 429} or exc.code >= 500
        raise DeliveryFailure(f"HTTP {exc.code}", exc.code, retryable) from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise DeliveryFailure("network error") from exc
    if status < 200 or status >= 300:
        retryable = status in {408, 429} or status >= 500
        raise DeliveryFailure(f"HTTP {status}", status, retryable)
    return status
