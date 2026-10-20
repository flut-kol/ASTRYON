# ADR-012: Large objects only in S3; Kafka carries references

Status: Accepted (plan v1.0, DEVELOPMENT_PLAN.md §1)

## Context
Large messages destabilize brokers and inflate cost.

## Decision
Payloads above the small-message limit are stored in S3; events carry `object_ref` + SHA-256 `checksum`.

## Consequences
Stable Kafka message size; extra S3 commit step before acceptance (event accepted only after S3 commit + registered job).
