from __future__ import annotations

import sqlite3
import uuid
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from relay_service.errors import RequestError
from relay_service.requests import ParsedRequest


@dataclass(frozen=True)
class Submission:
    event_id: str
    state: str
    duplicate: bool


@dataclass(frozen=True)
class EventClaim:
    event_id: str
    caller_id: str
    destination: str
    payload_json: str
    generation: int
    attempt_number: int
    attempt_id: int
    lease_token: str


class SQLiteStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def initialize(self) -> None:
        with closing(self.connect()) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            with connection:
                connection.executescript(SCHEMA)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def submit(self, request: ParsedRequest, now: float) -> Submission:
        event_id = str(uuid.uuid4())
        with closing(self.connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """INSERT INTO events
                   (event_id, caller_id, idempotency_key, request_hash, destination,
                    payload_json, state, created_at, available_at)
                   VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                   ON CONFLICT(caller_id, idempotency_key) DO NOTHING""",
                (event_id, request.caller_id, request.idempotency_key,
                 request.request_hash, request.destination, request.payload_json, now, now),
            )
            existing = connection.execute(
                """SELECT event_id, request_hash, state FROM events
                   WHERE caller_id=? AND idempotency_key=?""",
                (request.caller_id, request.idempotency_key),
            ).fetchone()
            if existing["request_hash"] != request.request_hash:
                raise RequestError(409, "Idempotency-Key was already used with a different request")
            inserted = existing["event_id"] == event_id
            return Submission(existing["event_id"], existing["state"], not inserted)

    def claim(
        self, now: float, lease_seconds: float, max_attempts: int
    ) -> EventClaim | None:
        with closing(self.connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            event = connection.execute(
                """SELECT * FROM events
                   WHERE state='pending' AND available_at <= ?
                     AND (lease_until IS NULL OR lease_until <= ?)
                   ORDER BY available_at, created_at LIMIT 1""",
                (now, now),
            ).fetchone()
            if event is None:
                return None
            if event["attempts"] >= max_attempts:
                connection.execute(
                    """UPDATE attempts SET status='abandoned', ended_at=?,
                       error='lease expired before completion'
                       WHERE event_id=? AND generation=? AND status='started'""",
                    (now, event["event_id"], event["generation"]),
                )
                changed = connection.execute(
                    """UPDATE events SET state='dead', available_at=?,
                       last_error='attempt budget exhausted before another delivery',
                       lease_until=NULL, lease_token=NULL
                       WHERE event_id=? AND state='pending' AND attempts=? AND
                       (lease_until IS NULL OR lease_until<=?)""",
                    (now, event["event_id"], event["attempts"], now),
                ).rowcount
                if changed != 1:
                    raise RuntimeError("event changed while its attempt budget was exhausted")
                return None
            attempt_number = event["attempts"] + 1
            lease_token = str(uuid.uuid4())
            changed = connection.execute(
                """UPDATE events SET attempts=?, lease_until=?, lease_token=?
                   WHERE event_id=? AND state='pending'""",
                (attempt_number, now + lease_seconds, lease_token, event["event_id"]),
            ).rowcount
            if changed != 1:
                return None
            connection.execute(
                """UPDATE attempts SET status='abandoned', ended_at=?,
                   error='lease expired before completion'
                   WHERE event_id=? AND generation=? AND status='started'""",
                (now, event["event_id"], event["generation"]),
            )
            attempt_id = connection.execute(
                """INSERT INTO attempts(event_id, generation, attempt_number, status, started_at)
                   VALUES (?, ?, ?, 'started', ?)""",
                (event["event_id"], event["generation"], attempt_number, now),
            ).lastrowid
            return EventClaim(
                event["event_id"], event["caller_id"], event["destination"],
                event["payload_json"], event["generation"], attempt_number,
                attempt_id, lease_token,
            )

    def finish(
        self, claim: EventClaim, succeeded: bool, now: float, max_attempts: int,
        retry_delay: float, status_code: int | None, error: str | None,
        retryable: bool = True,
    ) -> str:
        state = "delivered" if succeeded else (
            "dead" if not retryable or claim.attempt_number >= max_attempts else "pending"
        )
        available_at = now if succeeded or state == "dead" else now + retry_delay
        with closing(self.connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                """UPDATE events SET state=?, available_at=?, delivered_at=?, last_error=?,
                   lease_until=NULL, lease_token=NULL
                   WHERE event_id=? AND lease_token=?""",
                (state, available_at, now if succeeded else None, error,
                 claim.event_id, claim.lease_token),
            ).rowcount
            if changed != 1:
                raise RuntimeError("event lease expired before attempt could be recorded")
            changed = connection.execute(
                """UPDATE attempts SET status=?, ended_at=?, http_status=?, error=?
                   WHERE attempt_id=? AND status='started'""",
                (state, now, status_code, error, claim.attempt_id),
            ).rowcount
            if changed != 1:
                raise RuntimeError("attempt ledger entry changed before completion")
        return state

    def replay(self, event_id: str, actor: str, reason: str, now: float) -> int:
        if not actor or len(actor) > 128:
            raise ValueError("replay actor must contain 1 to 128 characters")
        if len(reason.strip()) < 5 or len(reason) > 500:
            raise ValueError("replay reason must contain 5 to 500 characters")
        with closing(self.connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            event = connection.execute(
                "SELECT generation FROM events WHERE event_id=? AND state='dead'", (event_id,)
            ).fetchone()
            if event is None:
                raise ValueError("only dead events can be replayed")
            generation = event["generation"] + 1
            changed = connection.execute(
                """UPDATE events SET state='pending', generation=?, attempts=0,
                   available_at=?, delivered_at=NULL, last_error=NULL,
                   lease_until=NULL, lease_token=NULL
                   WHERE event_id=? AND state='dead'""",
                (generation, now, event_id),
            ).rowcount
            if changed != 1:
                raise RuntimeError("dead event changed during replay")
            connection.execute(
                """INSERT INTO replay_audit(event_id, generation, actor, reason, replayed_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (event_id, generation, actor, reason.strip(), now),
            )
            return generation

    def inspect(self, event_id: str | None = None) -> list[dict[str, object]]:
        with closing(self.connect()) as connection:
            if event_id:
                rows = connection.execute(
                    "SELECT * FROM events WHERE event_id=?", (event_id,)
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM events ORDER BY created_at DESC LIMIT 100"
                ).fetchall()
            summaries = [self.summary(row) for row in rows]
            for summary in summaries:
                summary["replays"] = self.replay_history(str(summary["event_id"]), connection)
            return summaries

    @staticmethod
    def replay_history(event_id: str, connection: sqlite3.Connection) -> list[dict[str, object]]:
        rows = connection.execute(
            """SELECT generation, actor, reason, replayed_at FROM replay_audit
               WHERE event_id=? ORDER BY generation""",
            (event_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def check_ready(self) -> None:
        with closing(self.connect()) as connection:
            connection.execute("SELECT 1").fetchone()

    @staticmethod
    def summary(row: sqlite3.Row) -> dict[str, object]:
        return {
            "event_id": row["event_id"], "caller_id": row["caller_id"],
            "destination": row["destination"], "state": row["state"],
            "attempts": row["attempts"], "generation": row["generation"],
            "created_at": row["created_at"], "delivered_at": row["delivered_at"],
            "last_error": row["last_error"],
        }


SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    caller_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    destination TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','delivered','dead')),
    attempts INTEGER NOT NULL DEFAULT 0,
    generation INTEGER NOT NULL DEFAULT 1,
    created_at REAL NOT NULL,
    available_at REAL NOT NULL,
    delivered_at REAL,
    last_error TEXT,
    lease_until REAL,
    lease_token TEXT,
    UNIQUE(caller_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS events_ready ON events(state, available_at, lease_until);
CREATE TABLE IF NOT EXISTS attempts (
    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL REFERENCES events(event_id),
    generation INTEGER NOT NULL,
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('started','delivered','pending','dead','abandoned')),
    started_at REAL NOT NULL,
    ended_at REAL,
    http_status INTEGER,
    error TEXT
);
CREATE TABLE IF NOT EXISTS replay_audit (
    replay_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL REFERENCES events(event_id),
    generation INTEGER NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    replayed_at REAL NOT NULL
);
"""
