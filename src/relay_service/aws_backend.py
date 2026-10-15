from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import BotoCoreError, ClientError

from relay_service.config import (
    Settings,
    parse_destinations,
    parse_positive_float,
    parse_positive_int,
)
from relay_service.errors import RequestError
from relay_service.requests import ParsedRequest

EVENT_RECORD = "EVENT"
serializer = TypeSerializer()
deserializer = TypeDeserializer()


@dataclass(frozen=True)
class EventClaimRequest:
    event_key: str
    generation: int
    now: int
    lease_seconds: int
    max_attempts: int


def load_aws_settings() -> Settings:
    ingress_secret = read_secret(os.environ["RELAY_INGRESS_SECRET_ARN"])
    return build_aws_settings(ingress_secret)


def load_destination_settings() -> Settings:
    return build_aws_settings("")


def build_aws_settings(ingress_secret: str) -> Settings:
    try:
        entries = json.loads(os.environ["RELAY_DESTINATIONS_JSON"])
    except json.JSONDecodeError as exc:
        raise ValueError("RELAY_DESTINATIONS_JSON must contain valid JSON") from exc
    if not isinstance(entries, dict) or not entries:
        raise ValueError("RELAY_DESTINATIONS_JSON must be a non-empty object")
    destinations = parse_destinations(entries, "aws")
    return Settings(
        "aws", Path(":"), ingress_secret, destinations,
        max_attempts=parse_positive_int("RELAY_MAX_ATTEMPTS", 5),
        retry_base_seconds=parse_positive_float("RELAY_RETRY_BASE_SECONDS", 1.0),
        retry_max_seconds=parse_positive_float("RELAY_RETRY_MAX_SECONDS", 300.0),
        request_timeout_seconds=parse_positive_float("RELAY_REQUEST_TIMEOUT_SECONDS", 10.0),
    )


def read_secret(secret_arn: str) -> str:
    response = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)
    secret = response.get("SecretString")
    if not isinstance(secret, str) or not secret:
        raise RuntimeError("secrets must be stored as non-empty SecretString values")
    return secret


def read_operator_secret() -> str:
    operator_secret_arn = os.environ["RELAY_OPERATOR_SECRET_ARN"]
    ingress_secret_arn = os.environ["RELAY_INGRESS_SECRET_ARN"]
    if operator_secret_arn == ingress_secret_arn:
        raise RuntimeError("operator and ingress secrets must use distinct Secrets Manager ARNs")
    operator_secret = read_secret(operator_secret_arn)
    ingress_secret = read_secret(ingress_secret_arn)
    if hmac.compare_digest(operator_secret.encode(), ingress_secret.encode()):
        raise RuntimeError("operator and ingress bearer tokens must be different")
    return operator_secret


def event_key(caller_id: str, idempotency_key: str) -> str:
    material = f"{caller_id}\0{idempotency_key}".encode()
    return hashlib.sha256(material).hexdigest()


def table_name() -> str:
    name = os.environ.get("RELAY_TABLE_NAME", "")
    if not name:
        raise ValueError("RELAY_TABLE_NAME is required")
    return name


def queue_url() -> str:
    url = os.environ.get("RELAY_QUEUE_URL", "")
    if not url:
        raise ValueError("RELAY_QUEUE_URL is required")
    return url


def get_event(client: Any, name: str, key: str) -> dict[str, Any] | None:
    response = client.get_item(
        TableName=name,
        Key=marshal({"event_key": key, "record_key": EVENT_RECORD}),
        ConsistentRead=True,
    )
    raw_event = response.get("Item")
    return None if raw_event is None else unmarshal(raw_event)


def put_event(
    client: Any, name: str, request: ParsedRequest, now: int
) -> tuple[dict[str, Any], bool]:
    key = event_key(request.caller_id, request.idempotency_key)
    event = {
        "event_key": key,
        "record_key": EVENT_RECORD,
        "event_id": str(uuid.uuid4()),
        "caller_id": request.caller_id,
        "idempotency_key": request.idempotency_key,
        "request_hash": request.request_hash,
        "destination": request.destination,
        "payload_json": request.payload_json,
        "state": "pending",
        "attempts": 0,
        "generation": 1,
        "created_at": now,
        "available_at": now,
        "ever_accepted": False,
    }
    try:
        client.put_item(
            TableName=name,
            Item=marshal(event),
            ConditionExpression="attribute_not_exists(event_key)",
        )
        return event, True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            raise
    saved = get_event(client, name, key)
    if saved is None:
        raise RuntimeError("idempotency record disappeared during request")
    if saved["request_hash"] != request.request_hash:
        raise RequestError(409, "Idempotency-Key was already used with a different request")
    return saved, False


def enqueue_event(client: Any, sqs: Any, event: dict[str, Any], now: int) -> bool:
    if event["state"] != "pending" or event["available_at"] > now:
        return bool(event.get("ever_accepted"))
    token = str(uuid.uuid4())
    reserved = reserve_enqueue(client, event, token, now)
    if not reserved:
        return bool(event.get("ever_accepted"))
    try:
        sqs.send_message(
            QueueUrl=queue_url(),
            MessageBody=json.dumps(
                {"event_key": event["event_key"], "event_id": event["event_id"],
                 "generation": event["generation"]},
                separators=(",", ":"),
            ),
        )
    except BotoCoreError:
        release_enqueue(client, event, token)
        raise
    mark_enqueued(client, event, token, now)
    return True


def reserve_enqueue(client: Any, event: dict[str, Any], token: str, now: int) -> bool:
    try:
        client.update_item(
            TableName=table_name(),
            Key=marshal({"event_key": event["event_key"], "record_key": EVENT_RECORD}),
            UpdateExpression="SET enqueue_token=:token, enqueue_lease_until=:lease",
            ConditionExpression=(
                "#state=:pending AND available_at<=:now AND "
                "(attribute_not_exists(enqueue_lease_until) OR enqueue_lease_until<=:now)"
            ),
            ExpressionAttributeNames={"#state": "state"},
            ExpressionAttributeValues=marshal_values(
                token=token, lease=now + 60, pending="pending", now=now
            ),
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def release_enqueue(client: Any, event: dict[str, Any], token: str) -> None:
    try:
        client.update_item(
            TableName=table_name(),
            Key=marshal({"event_key": event["event_key"], "record_key": EVENT_RECORD}),
            UpdateExpression="REMOVE enqueue_token, enqueue_lease_until",
            ConditionExpression="enqueue_token=:token",
            ExpressionAttributeValues=marshal_values(token=token),
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            raise


def mark_enqueued(client: Any, event: dict[str, Any], token: str, now: int) -> None:
    visibility = parse_positive_int("RELAY_QUEUE_VISIBILITY_SECONDS", 210)
    try:
        client.update_item(
            TableName=table_name(),
            Key=marshal({"event_key": event["event_key"], "record_key": EVENT_RECORD}),
            UpdateExpression=(
                "SET ever_accepted=:yes, queue_accepted_at=:now, enqueue_lease_until=:lease "
                "REMOVE enqueue_token"
            ),
            ConditionExpression="enqueue_token=:token",
            ExpressionAttributeValues=marshal_values(
                yes=True, now=now, lease=now + visibility, token=token
            ),
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            raise


def claim_event(
    client: Any, name: str, request: EventClaimRequest
) -> dict[str, Any] | None:
    event = get_event(client, name, request.event_key)
    if event is None or event["generation"] != request.generation:
        return None
    if event["state"] not in {"pending", "processing"}:
        return None
    if event["available_at"] > request.now or event.get("lease_until", 0) > request.now:
        return None
    if event["attempts"] >= request.max_attempts:
        mark_exhausted_event(client, name, event, request.now)
        return None
    attempt_number = event["attempts"] + 1
    lease_token = str(uuid.uuid4())
    transact_items = [
        {
            "Update": {
                "TableName": name,
                "Key": marshal({"event_key": request.event_key, "record_key": EVENT_RECORD}),
                "UpdateExpression": (
                    "SET #state=:processing, attempts=:next_attempt, lease_until=:lease, "
                    "lease_token=:token, ever_accepted=:ever_accepted "
                    "REMOVE queue_accepted_at, enqueue_lease_until, enqueue_token"
                ),
                "ConditionExpression": (
                    "#state=:current_state AND generation=:generation AND attempts=:attempts "
                    "AND available_at<=:now AND "
                    "(attribute_not_exists(lease_until) OR lease_until<=:now)"
                ),
                "ExpressionAttributeNames": {"#state": "state"},
                "ExpressionAttributeValues": marshal_values(
                    processing="processing", next_attempt=attempt_number,
                    lease=request.now + request.lease_seconds, token=lease_token,
                    ever_accepted=True, current_state=event["state"],
                    generation=request.generation, attempts=event["attempts"],
                    now=request.now,
                ),
            }
        },
        {
            "Put": {
                "TableName": name,
                "Item": marshal(attempt_record(event, attempt_number, request.now)),
                "ConditionExpression": (
                    "attribute_not_exists(event_key) AND attribute_not_exists(record_key)"
                ),
            }
        },
    ]
    add_abandoned_attempt(name, event, request.now, transact_items)
    try:
        client.transact_write_items(TransactItems=transact_items)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "TransactionCanceledException":
            return None
        raise
    event.update(
        state="processing", attempts=attempt_number,
        lease_until=request.now + request.lease_seconds, lease_token=lease_token,
    )
    return event


def mark_exhausted_event(
    client: Any, name: str, event: dict[str, Any], now: int
) -> None:
    update = {
        "TableName": name,
        "Key": marshal({"event_key": event["event_key"], "record_key": EVENT_RECORD}),
        "UpdateExpression": (
            "SET #state=:dead, available_at=:now, last_error=:error "
            "REMOVE lease_until, lease_token, queue_accepted_at, enqueue_lease_until, enqueue_token"
        ),
        "ConditionExpression": (
            "#state=:current_state AND generation=:generation AND attempts=:attempts "
            "AND available_at<=:now AND "
            "(attribute_not_exists(lease_until) OR lease_until<=:now)"
        ),
        "ExpressionAttributeNames": {"#state": "state"},
        "ExpressionAttributeValues": marshal_values(
            dead="dead", now=now, error="attempt budget exhausted before another delivery",
            current_state=event["state"], generation=event["generation"],
            attempts=event["attempts"],
        ),
    }
    updates = [{"Update": update}]
    add_abandoned_attempt(name, event, now, updates)
    try:
        client.transact_write_items(TransactItems=updates)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "TransactionCanceledException":
            raise


def attempt_record(event: dict[str, Any], attempt_number: int, now: int) -> dict[str, Any]:
    return {
        "event_key": event["event_key"],
        "record_key": f"ATTEMPT#{event['generation']:010d}#{attempt_number:010d}",
        "event_id": event["event_id"], "generation": event["generation"],
        "attempt_number": attempt_number, "status": "started", "started_at": now,
    }


def add_abandoned_attempt(
    name: str, event: dict[str, Any], now: int, transact_items: list[dict[str, Any]],
) -> None:
    if event["state"] != "processing":
        return
    key = {
        "event_key": event["event_key"],
        "record_key": f"ATTEMPT#{event['generation']:010d}#{event['attempts']:010d}",
    }
    transact_items.append(
        {
            "Update": {
                "TableName": name,
                "Key": marshal(key),
                "UpdateExpression": "SET #status=:abandoned, ended_at=:now, #error=:error",
                "ConditionExpression": "#status=:started",
                "ExpressionAttributeNames": {"#status": "status", "#error": "error"},
                "ExpressionAttributeValues": marshal_values(
                    abandoned="abandoned", now=now,
                    error="lease expired before completion", started="started",
                ),
            }
        }
    )


def finish_event(
    client: Any, name: str, claim: dict[str, Any], now: int, max_attempts: int,
    retry_delay: float, status_code: int | None, error: str | None,
    succeeded: bool, retryable: bool,
) -> str:
    state = "delivered" if succeeded else (
        "pending" if retryable and claim["attempts"] < max_attempts else "dead"
    )
    attempt_status = "delivered" if succeeded else ("retry" if state == "pending" else "dead")
    attempt_key = {
        "event_key": claim["event_key"],
        "record_key": f"ATTEMPT#{claim['generation']:010d}#{claim['attempts']:010d}",
    }
    event_update, attempt_update = completion_updates(
        name, claim, now, state, attempt_status, retry_delay, status_code, error
    )
    try:
        client.transact_write_items(TransactItems=[
            {"Update": event_update},
            {"Update": dict(attempt_update, Key=marshal(attempt_key))},
        ])
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "TransactionCanceledException":
            raise RuntimeError("event lease expired before attempt could be recorded") from exc
        raise
    return state


def completion_updates(
    name: str, claim: dict[str, Any], now: int, state: str, attempt_status: str,
    retry_delay: float, status_code: int | None, error: str | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    values = {
        "state": state, "lease_token": claim["lease_token"],
        "generation": claim["generation"],
    }
    if state == "delivered":
        values["now"] = now
        event_expression = (
            "SET #state=:state, delivered_at=:now REMOVE lease_until, lease_token, "
            "queue_accepted_at, enqueue_lease_until, enqueue_token, last_error"
        )
    else:
        values.update(available=now + math_ceil(retry_delay) if state == "pending" else now,
                      error=error or "delivery rejected")
        event_expression = (
            "SET #state=:state, available_at=:available, last_error=:error "
            "REMOVE lease_until, lease_token, queue_accepted_at, enqueue_lease_until, enqueue_token"
        )
    event_update = {
        "TableName": name,
        "Key": marshal({"event_key": claim["event_key"], "record_key": EVENT_RECORD}),
        "UpdateExpression": event_expression,
        "ConditionExpression": (
            "#state=:processing AND lease_token=:lease_token AND generation=:generation"
        ),
        "ExpressionAttributeNames": {"#state": "state"},
        "ExpressionAttributeValues": marshal_values(**values, processing="processing"),
    }
    attempt_update = {
        "TableName": name,
        "UpdateExpression": (
            "SET #status=:status, ended_at=:now, http_status=:http_status, #error=:error"
        ),
        "ConditionExpression": "#status=:started",
        "ExpressionAttributeNames": {"#status": "status", "#error": "error"},
        "ExpressionAttributeValues": marshal_values(
            status=attempt_status, now=now, http_status=status_code,
            error=error, started="started",
        ),
    }
    return event_update, attempt_update


def math_ceil(duration: float) -> int:
    return math.ceil(duration)


def replay_event(
    client: Any, name: str, event_id: str, actor: str, reason: str, now: int
) -> tuple[dict[str, Any], int]:
    if not actor or len(actor) > 128:
        raise RequestError(400, "replay actor is invalid")
    if len(reason.strip()) < 5 or len(reason) > 500:
        raise RequestError(400, "replay reason must contain 5 to 500 characters")
    response = client.query(
        TableName=name,
        IndexName="event-id-index",
        KeyConditionExpression="event_id=:event_id AND record_key=:record_key",
        ExpressionAttributeValues=marshal_values(event_id=event_id, record_key=EVENT_RECORD),
        Limit=1,
    )
    rows = response.get("Items", [])
    if not rows:
        raise RequestError(404, "event not found")
    event = unmarshal(rows[0])
    if event["state"] != "dead":
        raise RequestError(409, "only dead events can be replayed")
    generation = event["generation"] + 1
    audit = {
        "event_key": event["event_key"], "record_key": f"REPLAY#{generation:010d}",
        "event_id": event_id, "generation": generation, "actor": actor,
        "reason": reason.strip(), "replayed_at": now,
    }
    client.transact_write_items(TransactItems=[
        {"Update": {
            "TableName": name,
            "Key": marshal({"event_key": event["event_key"], "record_key": EVENT_RECORD}),
            "UpdateExpression": (
                "SET #state=:pending, generation=:new_generation, attempts=:zero, "
                "available_at=:now, ever_accepted=:no REMOVE delivered_at, last_error, "
                "lease_until, lease_token, queue_accepted_at, enqueue_lease_until, enqueue_token"
            ),
            "ConditionExpression": "#state=:dead AND generation=:old_generation",
            "ExpressionAttributeNames": {"#state": "state"},
            "ExpressionAttributeValues": marshal_values(
                pending="pending", new_generation=generation, zero=0, now=now,
                no=False, dead="dead", old_generation=event["generation"],
            ),
        }},
        {"Put": {
            "TableName": name,
            "Item": marshal(audit),
            "ConditionExpression": (
                "attribute_not_exists(event_key) AND attribute_not_exists(record_key)"
            ),
        }},
    ])
    event.update(state="pending", generation=generation, attempts=0, available_at=now,
                 ever_accepted=False)
    return event, generation


def pending_events(client: Any, name: str, now: int) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    last_key = None
    for _ in range(10):
        page = client.query(**pending_query(name, now, last_key))
        candidates.extend(unmarshal(row) for row in page.get("Items", []))
        if len(candidates) >= 100 or "LastEvaluatedKey" not in page:
            return candidates[:100]
        last_key = page["LastEvaluatedKey"]
    return candidates[:100]


def pending_query(name: str, now: int, last_key: dict[str, Any] | None) -> dict[str, Any]:
    request: dict[str, Any] = {
        "TableName": name,
        "IndexName": "state-available-index",
        "KeyConditionExpression": "#state=:pending AND available_at<=:now",
        "FilterExpression": (
            "attribute_not_exists(enqueue_lease_until) OR enqueue_lease_until<=:now"
        ),
        "ExpressionAttributeNames": {"#state": "state"},
        "ExpressionAttributeValues": marshal_values(pending="pending", now=now),
        "Limit": 100,
    }
    if last_key is not None:
        request["ExclusiveStartKey"] = last_key
    return request


def marshal(value: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {key: serializer.serialize(entry) for key, entry in value.items()}


def marshal_values(**values: Any) -> dict[str, dict[str, Any]]:
    return {f":{key}": serializer.serialize(value) for key, value in values.items()}


def unmarshal(value: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {key: deserializer.deserialize(entry) for key, entry in value.items()}
