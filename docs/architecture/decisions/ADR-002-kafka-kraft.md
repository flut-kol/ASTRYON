# ADR-002: Apache Kafka (KRaft) as transport

Status: Accepted (plan v1.0, DEVELOPMENT_PLAN.md §1)

## Context
Need durable transport with replay within retention and partitioned scaling.

## Decision
Use Kafka in KRaft mode. Critical topics: RF=3, acks=all, min.insync.replicas=2, idempotent producers. Topics-as-code in `infra/kafka/topics.yaml`; partition counts are hypotheses until POC.

## Consequences
Replay only within retention (S3 for longer). Hot-partition and tenant skew must be tested. Operating Kafka is an SRE cost.
