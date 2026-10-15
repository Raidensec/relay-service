# Operations

## Delivery state and recovery

Delivery attempts use capped exponential backoff. Network errors, HTTP `408`, `429`, and `5xx` responses are retryable. Redirects and other `4xx` responses end the generation immediately. Once the configured attempt limit is reached, the event becomes `dead` and requires an operator replay. If the final attempt's lease expires before it can record an outcome, the next claim marks that attempt abandoned and the event dead without starting another delivery. The AWS worker acknowledges the current SQS message after this durable transition; the reconciler only enqueues pending events.

Inspect local events with `relay-service inspect`. The output contains the event state, attempt count, generation, latest delivery error, and replay audit history; it does not include request payloads. Replay only a dead event after checking the receiver and correcting the underlying condition:

```sh
relay-service replay 11111111-1111-4111-8111-111111111111 --reason "receiver recovered"
```

The local command records the current OS username as the audit actor. The cloud replay route is `POST /events/{event_id}/replay`, authenticated with the operator bearer token from `operator_secret_arn`, separate from the ingress token in `ingress_secret_arn`. It requires a JSON body with only a `reason` string. The server records the fixed actor `authenticated-operator`; caller headers do not set the audit actor. The shared operator token identifies the operator role, not an individual person.

AWS writes a pending event record before it sends to SQS. If the queue send fails, the API returns `503` with an instruction to retry the same idempotency key. A scheduled reconciler also checks pending records and retries enqueue. A successful queue send followed by an interrupted acknowledgement can produce a duplicate SQS message; the conditional lease and generation checks make stale messages harmless, while the receiver's event/generation deduplication handles repeated delivery.

The SQS dead-letter queue captures messages that repeatedly fail at the Lambda/SQS boundary. Inspect the DLQ alarm and message age alarm before redriving. A delivery failure that the worker records as `dead` is stored in DynamoDB and is not the same as an SQS DLQ message.

## Receiver requirements

Receivers should:

1. Read the exact request body bytes before parsing JSON.
2. Reject stale or malformed `X-Relay-Timestamp` values.
3. Compute HMAC-SHA256 using the destination's shared secret over `timestamp + b"." + raw_body` and compare signatures in constant time.
4. Deduplicate by `(X-Relay-Event-ID, X-Relay-Generation)` and return a 2xx response for an already-processed key.

The relay retries after ambiguous outcomes, including a receiver committing a request while the response is lost. Deduplication must therefore be part of the receiver's durable business transaction.

## Secret rotation

For local mode, rotate `RELAY_INGRESS_SECRET` and the destination signing secret in the shell environment and restart both processes together. Requests carrying the old bearer token will then fail. Update the receiver's destination secret in coordination with the relay because the implementation reads the secret for each delivery.

For AWS, rotate the Secrets Manager values referenced by `ingress_secret_arn`, `operator_secret_arn`, and each destination's `secret_arn`. The API Lambda reads the route-specific ingress or operator secret for each invocation; the worker reads the destination secret for each attempt. Coordinate receiver HMAC key rotation so requests signed during a transition can be verified. The Terraform state contains the ARNs, not the secret values.

## Alerts and checks

Terraform defines alarms for Lambda errors, oldest SQS message age, and visible DLQ messages. The source repository does not configure alarm notification targets. Configure CloudWatch alarm actions in the deployment environment if an on-call notification path is needed.

`GET /health` checks only local SQLite readiness. AWS API Gateway routes do not expose this local health endpoint. Do not treat a local health response as proof that destinations, SQS, DynamoDB, or AWS credentials are healthy.
