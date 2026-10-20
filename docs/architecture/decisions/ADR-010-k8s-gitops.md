# ADR-010: Kubernetes + Helm/Argo CD

Status: Accepted (plan v1.0, DEVELOPMENT_PLAN.md §1)

## Context
Need versioned deployment and scaling.

## Decision
Deploy on Kubernetes using Helm charts and Argo CD (overlays dev/stage/prod under `infra/`).

## Consequences
Declarative, reversible rollouts; stateful systems (Kafka/CH) need careful storage and operator decisions.
