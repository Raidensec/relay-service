from __future__ import annotations

import time
from dataclasses import dataclass

from relay_service.config import Settings
from relay_service.delivery import send_delivery
from relay_service.errors import DeliveryFailure
from relay_service.store import SQLiteStore


@dataclass(frozen=True)
class WorkerOutcome:
    event_id: str
    state: str
    attempt_number: int
    http_status: int | None


def run_once(store: SQLiteStore, settings: Settings) -> WorkerOutcome | None:
    now = time.time()
    claim = store.claim(
        now, settings.request_timeout_seconds + 30, settings.max_attempts
    )
    if claim is None:
        return None
    destination = settings.destinations[claim.destination]
    try:
        status_code = send_delivery(claim, destination, settings)
    except DeliveryFailure as failure:
        state = store.finish(
            claim, False, time.time(), settings.max_attempts,
            retry_delay(claim.attempt_number, settings), failure.status_code,
            str(failure), failure.retryable,
        )
        return WorkerOutcome(claim.event_id, state, claim.attempt_number, failure.status_code)
    state = store.finish(
        claim, True, time.time(), settings.max_attempts, 0, status_code, None
    )
    return WorkerOutcome(claim.event_id, state, claim.attempt_number, status_code)


def retry_delay(attempt_number: int, settings: Settings) -> float:
    return min(settings.retry_max_seconds, settings.retry_base_seconds * 2 ** (attempt_number - 1))
