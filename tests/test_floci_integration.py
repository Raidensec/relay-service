from __future__ import annotations

import io
import json
import os
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlsplit

import boto3
import pytest
from botocore.exceptions import BotoCoreError

from relay_service.aws_backend import EventClaimRequest, claim_event, unmarshal
from relay_service.aws_handlers import api_handler, reconciler_handler, worker_handler
from relay_service.signing import verify_delivery_signature


@dataclass(frozen=True)
class FlociResources:
    dynamodb: Any
    sqs: Any
    secrets: Any
    table_name: str
    queue_url: str
    dead_letter_url: str
    ingress_secret_arn: str
    operator_secret_arn: str
    destination_secret_arn: str
    ingress_secret: str
    operator_secret: str
    destination_secret: str


@pytest.fixture
def floci_resources(monkeypatch: pytest.MonkeyPatch) -> Iterator[FlociResources]:
    endpoint = require_floci_endpoint()
    set_test_aws_environment(monkeypatch, endpoint)
    clients = create_aws_clients(endpoint)
    require_handler_endpoint(endpoint)
    wait_for_floci(clients.dynamodb)
    suffix = uuid.uuid4().hex[:12]
    resources = create_floci_resources(clients, suffix)
    configure_relay(monkeypatch, resources)
    yield resources
    delete_floci_resources(resources)


@dataclass(frozen=True)
class AwsClients:
    dynamodb: Any
    sqs: Any
    secrets: Any


def require_floci_endpoint() -> str:
    endpoint = os.environ.get("RELAY_FLOCI_ENDPOINT_URL")
    if not endpoint and os.environ.get("RELAY_FLOCI_REQUIRED") == "true":
        pytest.fail("RELAY_FLOCI_ENDPOINT_URL is required for this verification job")
    if not endpoint:
        pytest.skip("Set RELAY_FLOCI_ENDPOINT_URL to run AWS integration checks against Floci")
    parsed = urlsplit(endpoint)
    if parsed.scheme != "http" or parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        pytest.fail("Floci endpoint must be local plain HTTP; AWS endpoints are never used")
    return endpoint.rstrip("/")


def set_test_aws_environment(monkeypatch: pytest.MonkeyPatch, endpoint: str) -> None:
    monkeypatch.setenv("AWS_ENDPOINT_URL", endpoint)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "floci-test-access-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "floci-test-secret-key")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


def create_aws_clients(endpoint: str) -> AwsClients:
    client_options = {"endpoint_url": endpoint, "region_name": "us-east-1"}
    return AwsClients(
        dynamodb=boto3.client("dynamodb", **client_options),
        sqs=boto3.client("sqs", **client_options),
        secrets=boto3.client("secretsmanager", **client_options),
    )


def require_handler_endpoint(endpoint: str) -> None:
    handler_client = boto3.client("dynamodb")
    if handler_client.meta.endpoint_url.rstrip("/") != endpoint:
        pytest.fail("boto3 handler clients are not pinned to the Floci endpoint")


def wait_for_floci(dynamodb: Any) -> None:
    last_error: BotoCoreError | None = None
    for _ in range(60):
        try:
            dynamodb.list_tables()
            return
        except BotoCoreError as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError("Floci did not answer DynamoDB ListTables within 60 seconds") from last_error


def create_floci_resources(clients: AwsClients, suffix: str) -> FlociResources:
    table_name = f"relay-events-{suffix}"
    dead_letter_name = f"relay-dead-letter-{suffix}"
    queue_name = f"relay-events-{suffix}"
    ingress_secret = f"floci-ingress-{suffix}"
    operator_secret = f"floci-operator-{suffix}"
    destination_secret = f"floci-destination-{suffix}"
    ingress_arn = create_secret(clients.secrets, ingress_secret, "test-ingress-secret")
    operator_arn = create_secret(clients.secrets, operator_secret, "test-operator-secret")
    destination_arn = create_secret(clients.secrets, destination_secret, "test-destination-secret")
    create_event_table(clients.dynamodb, table_name)
    dead_letter_url = clients.sqs.create_queue(QueueName=dead_letter_name)["QueueUrl"]
    dead_letter_arn = clients.sqs.get_queue_attributes(
        QueueUrl=dead_letter_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    queue_url = clients.sqs.create_queue(
        QueueName=queue_name,
        Attributes={
            "VisibilityTimeout": "210",
            "MessageRetentionPeriod": "1209600",
            "RedrivePolicy": json.dumps(
                {
                    "deadLetterTargetArn": dead_letter_arn,
                    "maxReceiveCount": "2",
                }
            ),
        },
    )["QueueUrl"]
    return FlociResources(
        clients.dynamodb,
        clients.sqs,
        clients.secrets,
        table_name,
        queue_url,
        dead_letter_url,
        ingress_arn,
        operator_arn,
        destination_arn,
        "test-ingress-secret",
        "test-operator-secret",
        "test-destination-secret",
    )


def delete_floci_resources(resources: FlociResources) -> None:
    resources.sqs.delete_queue(QueueUrl=resources.queue_url)
    resources.sqs.delete_queue(QueueUrl=resources.dead_letter_url)
    resources.dynamodb.delete_table(TableName=resources.table_name)
    for secret_arn in (
        resources.ingress_secret_arn,
        resources.operator_secret_arn,
        resources.destination_secret_arn,
    ):
        resources.secrets.delete_secret(SecretId=secret_arn, ForceDeleteWithoutRecovery=True)


def create_secret(secrets: Any, name: str, secret: str) -> str:
    return secrets.create_secret(Name=name, SecretString=secret)["ARN"]


def create_event_table(dynamodb: Any, table_name: str) -> None:
    dynamodb.create_table(
        TableName=table_name,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "event_key", "AttributeType": "S"},
            {"AttributeName": "record_key", "AttributeType": "S"},
            {"AttributeName": "state", "AttributeType": "S"},
            {"AttributeName": "available_at", "AttributeType": "N"},
            {"AttributeName": "event_id", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "event_key", "KeyType": "HASH"},
            {"AttributeName": "record_key", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "state-available-index",
                "KeySchema": [
                    {"AttributeName": "state", "KeyType": "HASH"},
                    {"AttributeName": "available_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "event-id-index",
                "KeySchema": [
                    {"AttributeName": "event_id", "KeyType": "HASH"},
                    {"AttributeName": "record_key", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
    )
    wait_for_table(dynamodb, table_name)


def wait_for_table(dynamodb: Any, table_name: str) -> None:
    for _ in range(60):
        description = dynamodb.describe_table(TableName=table_name)["Table"]
        indexes = description.get("GlobalSecondaryIndexes", [])
        if description["TableStatus"] == "ACTIVE" and all(
            index["IndexStatus"] == "ACTIVE" for index in indexes
        ):
            return
        time.sleep(0.5)
    raise RuntimeError(f"Floci table {table_name} did not become active within 30 seconds")


def configure_relay(monkeypatch: pytest.MonkeyPatch, resources: FlociResources) -> None:
    monkeypatch.setenv("RELAY_TABLE_NAME", resources.table_name)
    monkeypatch.setenv("RELAY_QUEUE_URL", resources.queue_url)
    monkeypatch.setenv("RELAY_QUEUE_VISIBILITY_SECONDS", "210")
    monkeypatch.setenv("RELAY_INGRESS_SECRET_ARN", resources.ingress_secret_arn)
    monkeypatch.setenv("RELAY_OPERATOR_SECRET_ARN", resources.operator_secret_arn)
    monkeypatch.setenv(
        "RELAY_DESTINATIONS_JSON",
        json.dumps(
            {
                "demo": {
                    "url": "https://receiver.example.test/webhook",
                    "secret_ref": resources.destination_secret_arn,
                }
            }
        ),
    )
    monkeypatch.setenv("RELAY_MAX_ATTEMPTS", "3")
    monkeypatch.setenv("RELAY_RETRY_BASE_SECONDS", "0.01")
    monkeypatch.setenv("RELAY_RETRY_MAX_SECONDS", "0.1")
    monkeypatch.setenv("RELAY_REQUEST_TIMEOUT_SECONDS", "1")


def test_floci_ingress_reconciles_delivery_and_audited_replay(
    floci_resources: FlociResources, monkeypatch: pytest.MonkeyPatch
) -> None:
    opener = RecordingOpener(floci_resources.destination_secret, [204, 400, 204])
    monkeypatch.setattr("relay_service.delivery.build_opener", lambda _: opener)
    payload = {"destination": "demo", "payload": {"order_id": "order-42"}}

    assert call_ingress(payload, floci_resources.operator_secret, "wrong-route")[
        "statusCode"
    ] == 401
    accepted = call_ingress(payload, floci_resources.ingress_secret, "event-42")
    duplicate = call_ingress(payload, floci_resources.ingress_secret, "event-42")
    conflict = call_ingress(
        {**payload, "payload": {"order_id": "changed"}},
        floci_resources.ingress_secret,
        "event-42",
    )

    assert accepted["statusCode"] == 202
    accepted_body = json.loads(accepted["body"])
    assert duplicate["statusCode"] == 202
    assert json.loads(duplicate["body"]) == {**accepted_body, "duplicate": True}
    assert conflict["statusCode"] == 409
    queued = receive_one(floci_resources.sqs, floci_resources.queue_url)
    assert json.loads(queued["Body"])["event_id"] == accepted_body["event_id"]
    assert worker_handler({"Records": [sqs_record(queued)]}, None) == {"batchItemFailures": []}
    floci_resources.sqs.delete_message(
        QueueUrl=floci_resources.queue_url, ReceiptHandle=queued["ReceiptHandle"]
    )
    delivered = event_record(
        floci_resources.dynamodb, floci_resources.table_name, accepted_body["event_id"]
    )
    assert delivered["state"]["S"] == "delivered"
    assert opener.deliveries[0]["verified"] is True
    assert (
        attempt_record(
            floci_resources.dynamodb, floci_resources.table_name, delivered["event_key"]["S"], 1, 1
        )["status"]["S"]
        == "delivered"
    )

    monkeypatch.setenv("RELAY_QUEUE_URL", f"{floci_resources.queue_url}-unavailable")
    recovery = call_ingress(
        {"destination": "demo", "payload": {"order_id": "order-43"}},
        floci_resources.ingress_secret,
        "event-43",
    )
    assert recovery["statusCode"] == 503
    monkeypatch.setenv("RELAY_QUEUE_URL", floci_resources.queue_url)
    assert reconciler_handler({}, None) == {"candidates": 1, "enqueued": 1}
    rejected = receive_one(floci_resources.sqs, floci_resources.queue_url)
    rejected_id = json.loads(rejected["Body"])["event_id"]
    assert worker_handler({"Records": [sqs_record(rejected)]}, None) == {"batchItemFailures": []}
    floci_resources.sqs.delete_message(
        QueueUrl=floci_resources.queue_url, ReceiptHandle=rejected["ReceiptHandle"]
    )
    dead = event_record(floci_resources.dynamodb, floci_resources.table_name, rejected_id)
    assert dead["state"]["S"] == "dead"

    assert call_replay(rejected_id, floci_resources.ingress_secret)["statusCode"] == 401
    replayed = call_replay(rejected_id, floci_resources.operator_secret)
    assert replayed["statusCode"] == 202
    replay_body = json.loads(replayed["body"])
    assert replay_body["generation"] == 2
    replay_queue_message = receive_one(floci_resources.sqs, floci_resources.queue_url)
    assert worker_handler({"Records": [sqs_record(replay_queue_message)]}, None) == {
        "batchItemFailures": []
    }
    replayed_event = event_record(floci_resources.dynamodb, floci_resources.table_name, rejected_id)
    audit = floci_resources.dynamodb.get_item(
        TableName=floci_resources.table_name,
        Key={
            "event_key": replayed_event["event_key"],
            "record_key": {"S": "REPLAY#0000000002"},
        },
        ConsistentRead=True,
    )["Item"]
    audit_record = unmarshal(audit)
    assert audit_record["reason"] == "receiver was corrected"
    assert audit_record["actor"] == "authenticated-operator"
    assert replayed_event["state"]["S"] == "delivered"
    assert [delivery["generation"] for delivery in opener.deliveries] == ["1", "1", "2"]
    assert all(delivery["verified"] for delivery in opener.deliveries)


def test_floci_expired_final_lease_becomes_dead_without_another_delivery(
    floci_resources: FlociResources, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RELAY_MAX_ATTEMPTS", "1")
    opener = RecordingOpener(floci_resources.destination_secret, [204])
    monkeypatch.setattr("relay_service.delivery.build_opener", lambda _: opener)
    accepted = call_ingress(
        {"destination": "demo", "payload": {"order_id": "expired-final-lease"}},
        floci_resources.ingress_secret,
        "expired-final-lease",
    )
    event_id = json.loads(accepted["body"])["event_id"]
    queued = receive_one(floci_resources.sqs, floci_resources.queue_url)
    message = json.loads(queued["Body"])
    now = int(time.time())
    claim = claim_event(
        floci_resources.dynamodb,
        floci_resources.table_name,
        EventClaimRequest(message["event_key"], message["generation"], now, 1, 1),
    )
    assert claim is not None
    floci_resources.dynamodb.update_item(
        TableName=floci_resources.table_name,
        Key={"event_key": {"S": message["event_key"]}, "record_key": {"S": "EVENT"}},
        UpdateExpression="SET lease_until=:expired",
        ConditionExpression="#state=:processing AND attempts=:last_attempt",
        ExpressionAttributeNames={"#state": "state"},
        ExpressionAttributeValues={
            ":expired": {"N": str(now - 1)},
            ":processing": {"S": "processing"},
            ":last_attempt": {"N": "1"},
        },
    )
    monkeypatch.setattr("relay_service.aws_handlers.time.time", lambda: now + 2)

    assert worker_handler({"Records": [sqs_record(queued)]}, None) == {
        "batchItemFailures": []
    }
    floci_resources.sqs.delete_message(
        QueueUrl=floci_resources.queue_url, ReceiptHandle=queued["ReceiptHandle"]
    )
    event = event_record(floci_resources.dynamodb, floci_resources.table_name, event_id)
    attempt = attempt_record(
        floci_resources.dynamodb, floci_resources.table_name, message["event_key"], 1, 1
    )
    assert event["state"]["S"] == "dead"
    assert event["attempts"]["N"] == "1"
    assert attempt["status"]["S"] == "abandoned"
    assert opener.deliveries == []
    reconciler_handler({}, None)
    assert floci_resources.sqs.receive_message(
        QueueUrl=floci_resources.queue_url, MaxNumberOfMessages=1
    ).get("Messages", []) == []


def test_floci_returns_poison_messages_to_dlq_after_partial_failure(
    floci_resources: FlociResources,
) -> None:
    floci_resources.sqs.send_message(QueueUrl=floci_resources.queue_url, MessageBody="{}")
    for _ in range(6):
        received = floci_resources.sqs.receive_message(
            QueueUrl=floci_resources.queue_url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=0,
        ).get("Messages", [])
        if not received:
            break
        message = received[0]
        result = worker_handler({"Records": [sqs_record(message)]}, None)
        assert result == {"batchItemFailures": [{"itemIdentifier": message["MessageId"]}]}
        floci_resources.sqs.change_message_visibility(
            QueueUrl=floci_resources.queue_url,
            ReceiptHandle=message["ReceiptHandle"],
            VisibilityTimeout=0,
        )

    dead_letter = floci_resources.sqs.receive_message(
        QueueUrl=floci_resources.dead_letter_url, MaxNumberOfMessages=1
    ).get("Messages", [])
    assert len(dead_letter) == 1
    assert dead_letter[0]["Body"] == "{}"


def receive_one(sqs: Any, queue_url: str) -> dict[str, Any]:
    messages = sqs.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=0
    ).get("Messages", [])
    assert len(messages) == 1
    return messages[0]


class RecordingOpener:
    def __init__(self, secret: str, statuses: list[int]) -> None:
        self.secret = secret
        self.statuses = iter(statuses)
        self.deliveries: list[dict[str, Any]] = []

    def open(self, request: Any, timeout: float) -> FakeResponse:
        headers = {key.lower(): value for key, value in request.header_items()}
        raw_body = request.data
        timestamp = headers["x-relay-timestamp"]
        verified = verify_delivery_signature(
            self.secret, timestamp, raw_body, headers["x-relay-signature"]
        )
        self.deliveries.append(
            {
                "event_id": headers["x-relay-event-id"],
                "generation": headers["x-relay-generation"],
                "verified": verified,
            }
        )
        status = next(self.statuses)
        if status >= 400:
            raise HTTPError(request.full_url, status, "receiver response", None, io.BytesIO())
        return FakeResponse(status)


class FakeResponse:
    def __init__(self, status: int) -> None:
        self.status = status

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        return None

    def getcode(self) -> int:
        return self.status


def call_ingress(payload: dict[str, Any], secret: str, key: str) -> dict[str, Any]:
    return api_handler(api_event("POST", "/events", payload, secret, key), None)


def call_replay(event_id: str, secret: str) -> dict[str, Any]:
    return api_handler(
        api_event(
            "POST",
            f"/events/{event_id}/replay",
            {"reason": "receiver was corrected"},
            secret,
            "replay-43",
            "caller-selected-audit-actor",
        ),
        None,
    )


def api_event(
    method: str, path: str, payload: dict[str, Any], secret: str, key: str,
    caller_id: str = "floci-integration-client",
) -> dict[str, Any]:
    return {
        "headers": {
            "authorization": f"Bearer {secret}",
            "content-type": "application/json",
            "x-caller-id": caller_id,
            "idempotency-key": key,
        },
        "requestContext": {"http": {"method": method}},
        "rawPath": path,
        "body": json.dumps(payload),
        "isBase64Encoded": False,
    }


def sqs_record(message: dict[str, Any]) -> dict[str, str]:
    return {"messageId": message["MessageId"], "body": message["Body"]}


def event_record(dynamodb: Any, table: str, event_id: str) -> dict[str, Any]:
    response = dynamodb.query(
        TableName=table,
        IndexName="event-id-index",
        KeyConditionExpression="event_id=:event_id AND record_key=:record_key",
        ExpressionAttributeValues={
            ":event_id": {"S": event_id},
            ":record_key": {"S": "EVENT"},
        },
        Limit=1,
    )
    assert len(response["Items"]) == 1
    return response["Items"][0]


def attempt_record(
    dynamodb: Any, table: str, event_key: str, generation: int, attempt_number: int
) -> dict[str, Any]:
    key = f"ATTEMPT#{generation:010d}#{attempt_number:010d}"
    return dynamodb.get_item(
        TableName=table,
        Key={"event_key": {"S": event_key}, "record_key": {"S": key}},
        ConsistentRead=True,
    )["Item"]
