from __future__ import annotations

import argparse
import getpass
import json
import time

from relay_service.config import load_settings
from relay_service.demo import run_demo
from relay_service.http_api import build_server
from relay_service.store import SQLiteStore
from relay_service.worker import run_once


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="relay-service")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="run the local ingestion HTTP server")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    worker = commands.add_parser("worker", help="deliver pending events from SQLite")
    worker.add_argument("--once", action="store_true")
    worker.add_argument("--poll-seconds", type=float, default=1.0)
    inspect = commands.add_parser("inspect", help="show event delivery state")
    inspect.add_argument("--event-id")
    replay = commands.add_parser("replay", help="requeue a dead event as a new generation")
    replay.add_argument("event_id")
    replay.add_argument("--reason", required=True)
    commands.add_parser("demo", help="run an authenticated delivery and retry demo")
    return parser


def main() -> None:
    arguments = build_parser().parse_args()
    if arguments.command == "demo":
        print(json.dumps(run_demo(), indent=2))
        return
    settings = load_settings()
    store = SQLiteStore(settings.database_path)
    if arguments.command == "serve":
        serve_events(arguments.host, arguments.port, store, settings)
    elif arguments.command == "worker":
        run_worker(store, settings, arguments.once, arguments.poll_seconds)
    elif arguments.command == "inspect":
        print(json.dumps(store.inspect(arguments.event_id), indent=2))
    elif arguments.command == "replay":
        generation = store.replay(
            arguments.event_id, getpass.getuser(), arguments.reason, time.time()
        )
        print(json.dumps({"event_id": arguments.event_id, "generation": generation}))


def serve_events(host: str, port: int, store: SQLiteStore, settings) -> None:
    server = build_server(host, port, store, settings)
    print(f"relay-service listening on http://{host}:{server.server_address[1]}")
    try:
        server.serve_forever()
    finally:
        server.server_close()


def run_worker(store: SQLiteStore, settings, once: bool, poll_seconds: float) -> None:
    if poll_seconds <= 0:
        raise ValueError("--poll-seconds must be positive")
    while True:
        outcome = run_once(store, settings)
        print(json.dumps({"outcome": None if outcome is None else outcome.__dict__}))
        if once:
            return
        if outcome is None:
            time.sleep(poll_seconds)
