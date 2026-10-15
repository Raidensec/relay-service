# Relay Service

A webhook can be accepted by an API and still disappear when delivery fails later. A receiver can also process a request before its response gets lost. Relay Service keeps a durable record of each event and gives delivery a retry and recovery path. It has a local SQLite mode and an AWS backend configuration using API Gateway, Lambda, DynamoDB, and SQS.

## How an event moves

1. **Accept:** A caller sends `POST /events` with a configured destination, a JSON payload, an idempotency key, and the shared bearer token.
2. **Persist:** The service commits the event to SQLite locally or DynamoDB in AWS before acknowledging it. Repeating the same key and request returns the existing event; changing the request under that key returns `409`.
3. **Deliver:** A worker sends the payload to the configured receiver with a timestamped HMAC signature and records each attempt. Network errors, `408`, `429`, and `5xx` responses retry with capped exponential backoff.
4. **Recover:** Once an event is dead, an operator can replay it as a new generation. Locally, use `relay-service replay`; in AWS, call `POST /events/{event_id}/replay` with a JSON body containing only a `reason` string. Cloud replay uses separate operator authentication and writes an audit record.

Delivery is **at least once**. A receiver may process a request even when the relay never receives its response, so receivers should verify the signature and deduplicate by `(X-Relay-Event-ID, X-Relay-Generation)`. Retries keep the same pair; a replay increments the generation.

`X-Caller-ID` scopes idempotency; it does not authenticate a caller or isolate tenants. Ingress uses one shared bearer token, so this is a single-tenant service. AWS replay uses a distinct operator token. The local `relay-service replay` command records the current OS username; the cloud route records the fixed actor `authenticated-operator` after operator-token authentication.

## Try the local demo

The demo starts a real HTTP API and receiver on loopback, writes to a temporary SQLite database, gets a `503` from the receiver on the first attempt, then retries and verifies the signed delivery. It prints the final event state and removes its temporary files when it exits. It does not use AWS.

Windows PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
relay-service demo
```

Linux or macOS:

```sh
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
relay-service demo
```

The package supports Python 3.12.

## Run the API and worker

Copy `.env.example` to `.env`. Replace both placeholder secrets with separate random values, and set `RELAY_DESTINATIONS_JSON` to a receiver you can reach. Load the same variables in the shells running the API and worker. In Bash:

```sh
set -a
. ./.env
set +a
```

In PowerShell, load the simple `KEY=value` lines from `.env` into the current process:

```powershell
Get-Content .env | ForEach-Object {
  if ($_ -match '^\s*([^#][^=]*)=(.*)$') {
    $value = $matches[2].Trim()
    if ($value.Length -ge 2 -and $value[0] -eq "'" -and $value[-1] -eq "'") {
      $value = $value.Substring(1, $value.Length - 2)
    }
    [Environment]::SetEnvironmentVariable($matches[1].Trim(), $value, 'Process')
  }
}
```

Start these in separate shells:

```sh
relay-service serve --host 127.0.0.1 --port 8000
relay-service worker
```

The API listens on `127.0.0.1:8000`. Local routes are `GET /health`, `POST /events`, and authenticated `GET /events/{event_id}`. The development server uses plain HTTP. To submit an event, send a JSON object with exactly `destination` and `payload`; `payload` must itself be an object. The destination must be configured on the server, so callers cannot supply a URL.

`relay-service worker --once` claims at most one ready event. `relay-service inspect` lists the latest 100 event summaries; add `--event-id <id>` to inspect one event. Replay a dead event with a reason:

```sh
event_id=11111111-1111-4111-8111-111111111111  # Replace with the ID of a dead event
relay-service replay "$event_id" --reason "operator explanation"
```

In Bash, with `.env` loaded in the current shell, submit a local event with:

```sh
curl -i -X POST http://127.0.0.1:8000/events \
  -H "Authorization: Bearer $RELAY_INGRESS_SECRET" \
  -H 'X-Caller-ID: billing-api' \
  -H 'Idempotency-Key: order-123' \
  -H 'Content-Type: application/json' \
  -d '{"destination":"demo","payload":{"order_id":"123"}}'
```

The API returns `202` with an `event_id` after SQLite commits the event. Use a unique idempotency key for each event. `RELAY_DESTINATIONS_JSON` is a JSON object keyed by destination name; each entry contains only `url` and `secret_ref`. In local mode, `secret_ref` has the form `env:VARIABLE_NAME`.

To run the API and worker in containers, configure the destination URL in `.env` to an address reachable from both containers, then run:

```sh
docker compose up --build
```

For a receiver running on the host in Docker Desktop, use `host.docker.internal`. The example destination at `127.0.0.1` is for native processes; inside a container, that address points back to the container.

## Delivery and destination security

The relay signs the exact outbound request body bytes with HMAC-SHA256 over the ASCII timestamp, a period, and the body. `X-Relay-Signature` contains `sha256=` followed by the lowercase hex digest. The relay sends `X-Relay-Timestamp`, `X-Relay-Signature`, `X-Relay-Event-ID`, and `X-Relay-Generation`. Receivers should compare signatures in constant time and reject timestamps more than five minutes from their clock. Retries within a generation keep the same event and generation identifiers.

Redirects and non-retryable `4xx` responses are terminal; an event becomes `dead` after a terminal response or the configured attempt limit. An expired final-attempt lease also marks the event dead and abandons that attempt before another delivery starts.

Requests are limited to 262,144 bytes. AWS destinations must use HTTPS; configuration rejects obvious local hostnames and literal non-public IP addresses. The application does not pin a hostname to a checked IP, so DNS rebinding or later DNS changes are not prevented. Use operator-controlled destinations and network egress restrictions when stronger isolation is needed. Redirects are not followed.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `RELAY_MODE` | `local` | Local CLI selects SQLite; AWS handlers select AWS mode internally. |
| `RELAY_DATABASE` | `relay.sqlite3` | SQLite file path in local mode. |
| `RELAY_INGRESS_SECRET` | none | Required local bearer token. |
| `RELAY_INGRESS_SECRET_ARN` | none | AWS API Lambda secret ARN for `POST /events`. |
| `RELAY_OPERATOR_SECRET_ARN` | none | Separate AWS API Lambda secret ARN for replay. |
| `RELAY_DESTINATIONS_JSON` | `{}` | Required non-empty map of named destinations. |
| `RELAY_MAX_ATTEMPTS` | `5` | Maximum delivery attempts in one generation. |
| `RELAY_RETRY_BASE_SECONDS` | `1` | Exponential backoff base. |
| `RELAY_RETRY_MAX_SECONDS` | `300` | Backoff cap. |
| `RELAY_REQUEST_TIMEOUT_SECONDS` | `10` | Per-delivery HTTP timeout. |

In AWS, each destination's `secret_ref` must be a Secrets Manager ARN. Destination URLs and secret references come from server configuration; callers choose a destination name, not a URL.

## Local behavior checks with Floci

The local HTTP check exercises SQLite, idempotency, retry, and signature verification against a real local receiver:

```sh
python -m pytest -q tests/test_local_http.py
```

The AWS integration checks use [Floci](https://github.com/floci-io/floci) for DynamoDB, SQS, and Secrets Manager. They invoke the Python Lambda handlers directly; they do not start a Lambda runtime. The outbound receiver is held at the HTTP boundary, so the checks do not contact a public endpoint. The fixture requires a loopback Floci URL, installs fake AWS credentials, and sets `AWS_ENDPOINT_URL`; it cannot fall through to AWS.

Start the pinned emulator in `compose.floci.yaml`, then run the checks. Windows PowerShell:

```powershell
try {
  docker compose -f compose.floci.yaml up -d
  $env:RELAY_FLOCI_ENDPOINT_URL = 'http://127.0.0.1:4566'
  $env:RELAY_FLOCI_REQUIRED = 'true'
  python -m pytest -q tests/test_floci_integration.py
} finally {
  docker compose -f compose.floci.yaml down
  Remove-Item Env:RELAY_FLOCI_ENDPOINT_URL, Env:RELAY_FLOCI_REQUIRED -ErrorAction SilentlyContinue
}
```

Linux or macOS:

```sh
docker compose -f compose.floci.yaml up -d
trap 'docker compose -f compose.floci.yaml down' EXIT
RELAY_FLOCI_ENDPOINT_URL=http://127.0.0.1:4566 \
RELAY_FLOCI_REQUIRED=true \
  python -m pytest -q tests/test_floci_integration.py
```

These checks cover authenticated idempotent ingress and conflicting-key rejection, recovery after an SQS enqueue failure, signed delivery and attempt records, audited replay, and malformed-message handling into the SQS dead-letter queue. Floci emulates the AWS services; it does not validate AWS behavior, Lambda execution, or a live deployment. Without `RELAY_FLOCI_ENDPOINT_URL`, pytest skips these checks. CI sets `RELAY_FLOCI_REQUIRED=true`, so a missing or unhealthy emulator fails the job.

## AWS configuration and validation

Terraform under [`infra/terraform`](infra/terraform) defines API Gateway HTTP API routes, three Lambda functions, DynamoDB, SQS and its dead-letter queue, IAM policies, log retention, and CloudWatch alarms. Ingress and operator secret ARNs must be distinct. Terraform stores references to Secrets Manager secrets, not their values. The API writes an event to DynamoDB before sending it to SQS; if the send fails, it returns `503` and keeps the event durable. A scheduled reconciler checks pending records and retries the enqueue.

Build the Lambda package and run the same offline configuration checks used in CI from the repository root:

```sh
python scripts/package_lambda.py
terraform -chdir=infra/terraform fmt -check -recursive
terraform -chdir=infra/terraform init -backend=false -input=false -lockfile=readonly -no-color
terraform -chdir=infra/terraform validate -no-color
terraform -chdir=infra/terraform plan -input=false -no-color -refresh=false -var-file=terraform.tfvars.example
```

Terraform initialization downloads the locked provider from the Terraform Registry. The example variables set `offline_plan = true`, use placeholder account and secret ARNs, and the plan disables refresh; these checks do not call AWS or validate credentials, permissions, quotas, networking, or runtime behavior. A real plan needs a valid account ID, distinct ingress and operator Secrets Manager ARNs, destination secret ARNs, AWS credentials, and `offline_plan = false`. `terraform apply` creates paid resources and is not part of these checks. No live AWS environment has been deployed or validated here.

The SQS visibility timeout defaults to 210 seconds for a 30-second worker Lambda with no batching window. Keep it at least six times the Lambda timeout plus the batching window.

CI runs Ruff, the local and Floci tests, `pip-audit`, Gitleaks secret scanning, Lambda packaging, and Terraform formatting, initialization, validation, and an offline plan. The emulator checks call handlers directly. CI does not deploy to AWS.

## Operations

See [`docs/operations.md`](docs/operations.md) for delivery recovery, dead-letter queue handling, receiver requirements, secret rotation, and alerts. A delivery recorded as `dead` in DynamoDB is different from a message that reaches the SQS dead-letter queue: the latter captures repeated failures at the Lambda/SQS boundary.
