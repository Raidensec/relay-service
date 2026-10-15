from __future__ import annotations

import ipaddress
import json
import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Destination:
    name: str
    url: str
    secret_ref: str


@dataclass(frozen=True)
class Settings:
    mode: str
    database_path: Path
    ingress_secret: str
    destinations: dict[str, Destination]
    request_limit_bytes: int = 262_144
    max_attempts: int = 5
    retry_base_seconds: float = 1.0
    retry_max_seconds: float = 300.0
    request_timeout_seconds: float = 10.0


def load_settings() -> Settings:
    mode = os.environ.get("RELAY_MODE", "local")
    if mode not in {"local", "aws"}:
        raise ValueError("RELAY_MODE must be local or aws")
    secret = os.environ.get("RELAY_INGRESS_SECRET", "")
    if not secret:
        raise ValueError("RELAY_INGRESS_SECRET is required")
    raw_destinations = os.environ.get("RELAY_DESTINATIONS_JSON", "{}")
    try:
        entries = json.loads(raw_destinations)
    except json.JSONDecodeError as exc:
        raise ValueError("RELAY_DESTINATIONS_JSON must contain valid JSON") from exc
    if not isinstance(entries, dict) or not entries:
        raise ValueError("RELAY_DESTINATIONS_JSON must be a non-empty object")
    destinations = parse_destinations(entries, mode)
    return Settings(
        mode=mode,
        database_path=Path(os.environ.get("RELAY_DATABASE", "relay.sqlite3")),
        ingress_secret=secret,
        destinations=destinations,
        max_attempts=parse_positive_int("RELAY_MAX_ATTEMPTS", 5),
        retry_base_seconds=parse_positive_float("RELAY_RETRY_BASE_SECONDS", 1.0),
        retry_max_seconds=parse_positive_float("RELAY_RETRY_MAX_SECONDS", 300.0),
        request_timeout_seconds=parse_positive_float("RELAY_REQUEST_TIMEOUT_SECONDS", 10.0),
    )


def parse_destinations(entries: dict[str, object], mode: str) -> dict[str, Destination]:
    parsed: dict[str, Destination] = {}
    for name, entry in entries.items():
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", name):
            raise ValueError(f"invalid destination name: {name!r}")
        if not isinstance(entry, dict) or set(entry) != {"url", "secret_ref"}:
            raise ValueError(f"destination {name!r} requires url and secret_ref")
        url = entry["url"]
        secret_ref = entry["secret_ref"]
        if not isinstance(url, str) or not isinstance(secret_ref, str) or not secret_ref:
            raise ValueError(f"destination {name!r} has invalid configuration")
        validate_destination_url(url, mode)
        if mode == "local" and not secret_ref.startswith("env:"):
            raise ValueError(f"local destination {name!r} secret_ref must use env:")
        if mode == "aws" and not secret_ref.startswith("arn:aws:secretsmanager:"):
            raise ValueError(f"AWS destination {name!r} secret_ref must be a Secrets Manager ARN")
        parsed[name] = Destination(name, url, secret_ref)
    return parsed


def validate_destination_url(url: str, mode: str) -> None:
    parts = urlsplit(url)
    if parts.scheme not in ({"http", "https"} if mode == "local" else {"https"}):
        raise ValueError("destination URL must use HTTPS in AWS mode")
    if not parts.hostname or parts.username or parts.password or parts.fragment:
        raise ValueError("destination URL must have a host and no credentials or fragment")
    try:
        parts.port
    except ValueError as exc:
        raise ValueError("destination URL has an invalid port") from exc
    if mode == "local":
        return
    host = parts.hostname.rstrip(".").lower()
    if (
        host in {"localhost", "localhost.localdomain"}
        or host.endswith(".localhost")
        or host.endswith(".local")
    ):
        raise ValueError("AWS destinations cannot use loopback or local hostnames")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return
    if not address.is_global:
        raise ValueError("AWS destinations cannot use non-public IP addresses")


def parse_positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if parsed < 1:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def parse_positive_float(name: str, default: float) -> float:
    raw = os.environ.get(name, str(default))
    try:
        parsed = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive number") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{name} must be a positive number")
    return parsed
