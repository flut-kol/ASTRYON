# Kubernetes foundation (DPP-012)

`base/` contains namespace-scoped guardrails only: default requests/limits,
quota and default-deny ingress/egress. `overlays/dev` and `overlays/stage`
instantiate them in separate namespaces.

This repository intentionally does not include a registry credential, AAS CA,
private key, database password, cluster endpoint or kubeconfig. A workload may
be added only with all of the following in the target environment:

1. digest-pinned image from the approved registry;
2. service account and least-privilege RBAC;
3. explicit NetworkPolicies for DNS, AAS, Kafka, PostgreSQL, ClickHouse and S3;
4. secret-manager references for every credential and TLS/CA material;
5. readiness/liveness probes, resource requests/limits and a disruption budget;
6. an Argo CD Application whose repository URL and revision are controlled by
   the platform team.

Render locally with `kubectl kustomize infra/kubernetes/overlays/dev`. Applying
requires platform-owned cluster credentials and a reviewed egress policy.

`infra/helm/gateway` is the first workload chart. It rejects an empty Kafka
broker value and requires an image digest. Render it only with a platform-owned
values file that supplies a real approved image, broker endpoints and
secret-manager-created Secret names; the checked-in image is intentionally
unpullable.
