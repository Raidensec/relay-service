from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from relay_service.config import Destination, Settings
from relay_service.http_api import build_server
from relay_service.signing import verify_delivery_signature
from relay_service.store import SQLiteStore
from relay_service.worker import run_once

DEMO_TOKEN = "local-demo-ingress-token"
DEMO_SECRET = "local-demo-destination-secret"


def run_demo() -> dict[str, object]:
    previous_secret = os.environ.get("RELAY_DEMO_UNUSED")
    os.environ["RELAY_DEMO_UNUSED"] = DEMO_SECRET
    try:
        with tempfile.TemporaryDirectory(prefix="relay-service-") as temporary:
            receiver = build_receiver(DEMO_SECRET)
            receiver_thread = start_server(receiver)
            try:
                settings, store, api, api_thread = start_relay(
                    Path(temporary), receiver.server_address
                )
                try:
                    accepted = post_demo_event(api.server_address, settings.ingress_secret)
                    first_attempt = run_once(store, settings)
                    time.sleep(settings.retry_base_seconds + 0.05)
                    second_attempt = run_once(store, settings)
                    saved_event = store.inspect(accepted["event_id"])[0]
                    return {
                        "http_status": accepted["http_status"],
                        "event_id": accepted["event_id"],
                        "attempts": [first_attempt.state, second_attempt.state],
                        "final_state": saved_event["state"],
                        "receiver_calls": receiver.receiver_calls,
                        "signature_verified": receiver.signature_verified,
                    }
                finally:
                    stop_server(api, api_thread)
            finally:
                stop_server(receiver, receiver_thread)
    finally:
        if previous_secret is None:
            os.environ.pop("RELAY_DEMO_UNUSED", None)
        else:
            os.environ["RELAY_DEMO_UNUSED"] = previous_secret


def start_relay(
    directory: Path, receiver_address: tuple[str, int]
) -> tuple[Settings, SQLiteStore, ThreadingHTTPServer, threading.Thread]:
    destination = Destination(
        "demo", f"http://{receiver_address[0]}:{receiver_address[1]}/webhook",
        "env:RELAY_DEMO_UNUSED",
    )
    settings = Settings(
        "local", directory / "demo.sqlite3", DEMO_TOKEN, {"demo": destination},
        retry_base_seconds=0.1,
    )
    store = SQLiteStore(settings.database_path)
    api = build_server("127.0.0.1", 0, store, settings)
    return settings, store, api, start_server(api)


def build_receiver(secret: str) -> ThreadingHTTPServer:
    class ReceiverHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            verified = verify_delivery_signature(
                secret,
                self.headers.get("X-Relay-Timestamp", ""),
                body,
                self.headers.get("X-Relay-Signature", ""),
            )
            self.server.signature_verified = self.server.signature_verified and verified
            self.server.receiver_calls += 1
            status = 503 if self.server.receiver_calls == 1 else (204 if verified else 401)
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            return

    receiver = ThreadingHTTPServer(("127.0.0.1", 0), ReceiverHandler)
    receiver.receiver_calls = 0
    receiver.signature_verified = True
    return receiver


def post_demo_event(address: tuple[str, int], token: str) -> dict[str, object]:
    body = json.dumps(
        {"destination": "demo", "payload": {"example": "real local delivery"}},
        separators=(",", ":"),
    ).encode("utf-8")
    request = urllib.request.Request(
        f"http://{address[0]}:{address[1]}/events", data=body, method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Idempotency-Key": "demo-1",
            "X-Caller-ID": "demo-client",
        },
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        accepted = json.loads(response.read().decode("utf-8"))
        accepted["http_status"] = response.status
        return accepted


def start_server(server: ThreadingHTTPServer) -> threading.Thread:
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return thread


def stop_server(server: ThreadingHTTPServer, thread: threading.Thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)
