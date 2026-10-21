# Runbook: gateway ingestion failures

Covers `AstryonGatewayAckErrors` (5xx share of event writes above 1% for 10 minutes).

## Symptoms

- `astryon_gateway_ack_errors_total` grows relative to `astryon_gateway_ack_total`
  (ACK error = 5xx: the event was not durably accepted);
- clients report `503 PUBLISH_FAILED`;
- `astryon_gateway_client_errors_total` alone growing is a client problem (see below), not this alert.

## Safe diagnosis

Use counters, status codes, `X-Request-ID`/`trace_id` and tenant IDs only; never copy event bodies,
tokens or Idempotency-Keys into tickets.

1. Which code? `PUBLISH_FAILED` (503) means Kafka publish was not acknowledged: check broker health,
   `kafka_consumergroup_lag`, ISR/min.insync.replicas and the gateway's Kafka TLS/SASL state
   (look for `kafka tls reload failed, keeping previous material` warnings: an expired cert or CA
   after a failed rotation causes broker rejection).
2. 4xx spikes are not ACK errors: `UNAUTHENTICATED`/`FORBIDDEN` (AAS token, scope `events:write`),
   `TENANT_MISMATCH` (body tenant differs from the token claim, a client bug),
   `BODY_TOO_LARGE`, `BATCH_TOO_LARGE`, `IDEMPOTENCY_KEY_INVALID`, `ENVELOPE_INVALID` (event
   quarantined, not lost). A sudden 401 wave across tenants points at AAS/JWKS, not the gateway.
3. Batch requests return 207 per-item results; a 503 for a whole batch means no item was accepted.

## Recovery

1. Restore Kafka reachability or credentials; do not disable TLS/SASL. If a certificate rotation
   failed, fix the mounted material; the gateway keeps the previous one and reloads on change.
2. Clients retry with the same `Idempotency-Key`; the event ID derives from it, so retries do not
   create duplicate events. Do not ask clients to change keys.
3. `ASTRYON_DEV_FAKE_PUBLISHER=1` must never be set outside dev (events would not be durable).
4. Scale or restart gateway pods only after the downstream cause is fixed.

## Validation

- `astryon:gateway_ack_error_ratio:5m` returns below 0.01 and the alert resolves;
- a test write returns 202 and the event reaches `processed.events.v1` with lag trending to zero
  (see [stuck-consumer.md](stuck-consumer.md) if the sink falls behind).
