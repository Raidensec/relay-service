from __future__ import annotations

import base64
import binascii
import json
import logging
import re
import time
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from relay_service.aws_backend import (
    EventClaimRequest,
    claim_event,
    enqueue_event,
    finish_event,
    get_event,
    load_aws_settings,
    load_destination_settings,
    pending_events,
    put_event,
    read_operator_secret,
    replay_event,
    table_name,
)
from relay_service.config import Settings
from relay_service.delivery import send_delivery
from relay_service.errors import DeliveryFailure, RequestError
from relay_service.requests import authenticate, parse_event_request

logger = logging.getLogger(__name__)
OPERATOR_AUDIT_ACTOR = "authenticated-operator"


def api_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    headers = normalize_headers(event.get("headers"))
    try:
        method = event.get("requestContext", {}).get("http", {}).get("method")
        path = event.get("rawPath", "")
        if not isinstance(method, str) or not isinstance(path, str):
            return api_response(404, {"error": "not found"})
        if method == "POST" and path == "/events":
            settings = load_aws_settings()
            if not authenticate(headers.get("authorization"), settings.ingress_secret):
                return api_response(401, {"error": "unauthorized"})
            return accept_api_event(event, headers, settings)
        replay_match = re.fullmatch(r"/events/([0-9a-fA-F-]{36})/replay", path)
        if method == "POST" and replay_match:
            operator_secret = read_operator_secret()
            if not authenticate(headers.get("authorization"), operator_secret):
                return api_response(401, {"error": "unauthorized"})
            return replay_api_event(event, replay_match.group(1))
        return api_response(404, {"error": "not found"})
    except RequestError as exc:
        return api_response(exc.status, {"error": exc.message})
    except (BotoCoreError, ClientError) as exc:
        logger.error("AWS API operation failed: %s", type(exc).__name__)
        return api_response(503, {"error": "service temporarily unavailable"})


def accept_api_event(
    event: dict[str, Any], headers: dict[str, str], settings: Settings
) -> dict[str, Any]:
    require_json_content_type(headers)
    raw_body = request_body(event, settings.request_limit_bytes)
    parsed = parse_event_request(headers, raw_body, settings)
    dynamodb = boto3.client("dynamodb")
    sqs = boto3.client("sqs")
    saved, created = put_event(dynamodb, table_name(), parsed, int(time.time()))
    if not saved.get("ever_accepted"):
        if not enqueue_event(dynamodb, sqs, saved, int(time.time())):
            return api_response(
                503, {"error": "event is durable; retry with the same idempotency key"}
            )
    return api_response(202, {
        "event_id": saved["event_id"], "state": saved["state"], "duplicate": not created,
    })


def replay_api_event(event: dict[str, Any], target_event_id: str) -> dict[str, Any]:
    require_json_content_type(normalize_headers(event.get("headers")))
    raw_body = request_body(event, 4096)
    try:
        request = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RequestError(400, "replay body must be valid JSON") from exc
    if not isinstance(request, dict) or set(request) != {"reason"}:
        raise RequestError(400, "replay body must contain only reason")
    reason = request["reason"]
    if not isinstance(reason, str):
        raise RequestError(400, "replay reason must be a string")
    dynamodb = boto3.client("dynamodb")
    sqs = boto3.client("sqs")
    event_record, generation = replay_event(
        dynamodb, table_name(), target_event_id, OPERATOR_AUDIT_ACTOR, reason,
        int(time.time()),
    )
    if not enqueue_event(dynamodb, sqs, event_record, int(time.time())):
        return api_response(503, {"error": "replay is durable; retry after reconciliation"})
    return api_response(202, {
        "event_id": target_event_id, "generation": generation, "state": "pending",
    })


def worker_handler(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
    settings = load_destination_settings()
    dynamodb = boto3.client("dynamodb")
    failures = []
    for record in event.get("Records", []):
        message_id = record.get("messageId")
        if not isinstance(message_id, str):
            raise ValueError("SQS record is missing messageId")
        try:
            completed = process_queue_record(record, dynamodb, settings)
        except (BotoCoreError, ClientError) as exc:
            logger.error("AWS worker operation failed: %s", type(exc).__name__)
            completed = False
        if not completed:
            failures.append({"itemIdentifier": message_id})
    return {"batchItemFailures": failures}


def process_queue_record(record: dict[str, Any], dynamodb: Any, settings: Settings) -> bool:
    try:
        message = json.loads(record["body"])
        key, message_event_id, generation = validate_queue_message(message)
    except (KeyError, TypeError, json.JSONDecodeError, ValueError):
        return False
    name = table_name()
    saved = get_event(dynamodb, name, key)
    if saved is None:
        return False
    if saved["event_id"] != message_event_id or saved["generation"] != generation:
        return True
    if saved["state"] in {"delivered", "dead"}:
        return True
    now = int(time.time())
    claim_request = EventClaimRequest(
        key, generation, now, int(settings.request_timeout_seconds) + 30,
        settings.max_attempts,
    )
    claim = claim_event(dynamodb, name, claim_request)
    if claim is None:
        current = get_event(dynamodb, name, key)
        if current is None or current["generation"] != generation:
            return True
        if current["state"] in {"delivered", "dead"}:
            return True
        if current.get("lease_until", 0) > now or current["available_at"] > now:
            return True
        return False
    return deliver_claim(dynamodb, name, claim, settings)


def validate_queue_message(message: Any) -> tuple[str, str, int]:
    if not isinstance(message, dict) or set(message) != {"event_key", "event_id", "generation"}:
        raise ValueError("queue message fields are invalid")
    key, event_id, generation = (
        message["event_key"], message["event_id"], message["generation"]
    )
    if not isinstance(key, str) or not re.fullmatch(r"[0-9a-f]{64}", key):
        raise ValueError("event key is invalid")
    if not isinstance(event_id, str) or not re.fullmatch(r"[0-9a-fA-F-]{36}", event_id):
        raise ValueError("event id is invalid")
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
        raise ValueError("event generation is invalid")
    return key, event_id, generation


def deliver_claim(
    dynamodb: Any, name: str, claim: dict[str, Any], settings: Settings
) -> bool:
    from relay_service.store import EventClaim

    delivery_claim = EventClaim(
        event_id=claim["event_id"], caller_id=claim["caller_id"],
        destination=claim["destination"], payload_json=claim["payload_json"],
        generation=claim["generation"], attempt_number=claim["attempts"],
        attempt_id=0, lease_token=claim["lease_token"],
    )
    destination = settings.destinations[claim["destination"]]
    try:
        status_code = send_delivery(delivery_claim, destination, settings)
    except DeliveryFailure as failure:
        new_state = finish_event(
            dynamodb, name, claim, int(time.time()), settings.max_attempts,
            retry_delay(claim["attempts"], settings), failure.status_code,
            str(failure), False, failure.retryable,
        )
        return new_state in {"pending", "dead"}
    finish_event(
        dynamodb, name, claim, int(time.time()), settings.max_attempts, 0,
        status_code, None, True, True,
    )
    return True


def retry_delay(attempt_number: int, settings: Settings) -> float:
    return min(settings.retry_max_seconds, settings.retry_base_seconds * 2 ** (attempt_number - 1))


def reconciler_handler(event: dict[str, Any], context: Any) -> dict[str, int]:
    dynamodb = boto3.client("dynamodb")
    sqs = boto3.client("sqs")
    name = table_name()
    candidates = pending_events(dynamodb, name, int(time.time()))
    enqueued = sum(
        enqueue_event(dynamodb, sqs, candidate, int(time.time()))
        for candidate in candidates
    )
    return {"candidates": len(candidates), "enqueued": enqueued}


def request_body(event: dict[str, Any], limit: int) -> bytes:
    encoded = event.get("body")
    if not isinstance(encoded, str):
        raise RequestError(400, "request body is required")
    encoded_flag = event.get("isBase64Encoded", False)
    if not isinstance(encoded_flag, bool):
        raise RequestError(400, "request body encoding flag is invalid")
    try:
        body = base64.b64decode(encoded, validate=True) if encoded_flag else encoded.encode("utf-8")
    except (binascii.Error, ValueError) as exc:
        raise RequestError(400, "request body encoding is invalid") from exc
    if len(body) > limit:
        raise RequestError(413, "request body is too large")
    return body


def require_json_content_type(headers: dict[str, str]) -> None:
    media_type = headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if media_type != "application/json":
        raise RequestError(415, "Content-Type must be application/json")


def normalize_headers(headers: Any) -> dict[str, str]:
    if not isinstance(headers, dict):
        return {}
    return {
        key.lower(): value
        for key, value in headers.items()
        if isinstance(key, str) and isinstance(value, str)
    }


def api_response(status: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body, separators=(",", ":")),
    }
