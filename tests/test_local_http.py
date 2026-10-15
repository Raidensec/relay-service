from __future__ import annotations

import contextlib
import json
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError

import pytest

from relay_service.config import Destination, Settings
from relay_service.http_api import build_server
from relay_service.requests import parse_event_request
from relay_service.signing import verify_delivery_signature
from relay_service.store import SQLiteStore
from relay_service.worker import run_once


def test_http_idempotency_and_retry_deliver_signed_payload_once_per_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination_secret = "local-test-destination-secret"
    monkeypatch.setenv("RELAY_TEST_DEST_SECRET", destination_secret)
    receiver, received = start_receiver(destination_secret, [503, 204])
    receiver_thread = threading.Thread(target=receiver.serve_forever, daemon=True)
    receiver_thread.start()
    destination = Destination(
        "demo", f"http://127.0.0.1:{receiver.server_address[1]}/webhook",
        "env:RELAY_TEST_DEST_SECRET",
    )
    settings = Settings(
        "local", tmp_path / "relay.sqlite3", "local-test-ingress-secret",
        {"demo": destination}, retry_base_seconds=0.01, retry_max_seconds=0.1,
        request_timeout_seconds=2,
    )
    store = SQLiteStore(settings.database_path)
    api = build_server("127.0.0.1", 0, store, settings)
    api_thread = threading.Thread(target=api.serve_forever, daemon=True)
    api_thread.start()
    try:
        accepted = post_local_event(api.server_address, settings.ingress_secret, "same-key")
        duplicate = post_local_event(api.server_address, settings.ingress_secret, "same-key")
        assert accepted["status"] == 202
        assert duplicate["status"] == 202
        assert duplicate["body"]["event_id"] == accepted["body"]["event_id"]
        assert duplicate["body"]["duplicate"] is True
        with pytest.raises(HTTPError) as conflict:
            post_local_event(
                api.server_address, settings.ingress_secret, "same-key", "changed-order"
            )
        assert conflict.value.code == 409

        first = run_once(store, settings)
        time.sleep(0.02)
        second = run_once(store, settings)

        assert first is not None and first.state == "pending"
        assert second is not None and second.state == "delivered"
        saved = store.inspect(accepted["body"]["event_id"])[0]
        assert saved["state"] == "delivered"
        assert saved["attempts"] == 2
        assert len(received) == 2
        assert all(item["signature_verified"] for item in received)
        assert {item["event_id"] for item in received} == {saved["event_id"]}
        assert {item["generation"] for item in received} == {"1"}
    finally:
        stop_server(api, api_thread)
        stop_server(receiver, receiver_thread)


def test_expired_final_lease_marks_event_dead_without_claiming_again(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "lease-expiry.sqlite3")
    settings = Settings(
        "local",
        tmp_path / "lease-expiry.sqlite3",
        "ingress-secret",
        {"demo": Destination("demo", "http://127.0.0.1:9000/webhook", "env:DEST_SECRET")},
        max_attempts=1,
    )
    request = parse_event_request(
        {"x-caller-id": "lease-test", "idempotency-key": "expired-final-attempt"},
        b'{"destination":"demo","payload":{"order_id":"lease-test"}}',
        settings,
    )
    submission = store.submit(request, 100.0)

    first_claim = store.claim(100.0, 1.0, settings.max_attempts)
    assert first_claim is not None
    assert first_claim.attempt_number == 1
    assert store.claim(102.0, 1.0, settings.max_attempts) is None

    event = store.inspect(submission.event_id)[0]
    with contextlib.closing(store.connect()) as connection:
        attempts = connection.execute(
            "SELECT attempt_number, status, ended_at FROM attempts WHERE event_id=?",
            (submission.event_id,),
        ).fetchall()
    assert event["state"] == "dead"
    assert event["attempts"] == 1
    assert len(attempts) == 1
    assert attempts[0]["attempt_number"] == 1
    assert attempts[0]["status"] == "abandoned"
    assert attempts[0]["ended_at"] == 102.0


def start_receiver(
    secret: str, statuses: list[int]
) -> tuple[ThreadingHTTPServer, list[dict[str, object]]]:
    received: list[dict[str, object]] = []

    class ReceiverHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            timestamp = self.headers.get("X-Relay-Timestamp", "")
            signature = self.headers.get("X-Relay-Signature", "")
            received.append({
                "event_id": self.headers.get("X-Relay-Event-ID", ""),
                "generation": self.headers.get("X-Relay-Generation", ""),
                "signature_verified": verify_delivery_signature(
                    secret, timestamp, body, signature
                ),
            })
            status = statuses.pop(0) if statuses else 204
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            return

    return ThreadingHTTPServer(("127.0.0.1", 0), ReceiverHandler), received


def post_local_event(
    address: tuple[str, int], token: str, idempotency_key: str,
    order_id: str = "order-71",
) -> dict[str, object]:
    body = json.dumps({
        "destination": "demo",
        "payload": {"order_id": order_id},
    }).encode("utf-8")
    request = urllib.request.Request(
        f"http://{address[0]}:{address[1]}/events",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-Caller-ID": "local-test-client",
            "Idempotency-Key": idempotency_key,
        },
    )
    with urllib.request.urlopen(request, timeout=3) as response:
        return {"status": response.status, "body": json.loads(response.read())}


def stop_server(server: ThreadingHTTPServer, thread: threading.Thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=3)
