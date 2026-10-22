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

## Workload chart (`charts/astryon-services`)

One chart for gateway, query-api, file-api, file-worker, export-worker, quarantine, connector-manager,
retry-controller, replay-controller, replay-worker, admin-bff and admin-ui. Per service it renders a
ServiceAccount (token not mounted), Deployment (non-root, read-only root fs, all caps dropped, /tmp
emptyDir, resources, probes), Service, PodDisruptionBudget and a NetworkPolicy (DNS plus only the
egress targets the service needs; ingress only from the ingress controller, admin-bff or the
monitoring peers you list), plus a namespace `<release>-default-deny`.

- Config is env only. Plain values come from the environment values file; credentials are
  `secretKeyRef`s (default Secret `astryon-<service>`, key = env var name; see `secretEnv`) or secret
  volumes (BFF client key, Kafka CA). Secrets are created by the secret manager, never in git.
  `*_DEV_*`, `BFF_COOKIE_INSECURE` and `BFF_CLIENT_SECRET` are rejected at render time.
- Egress peers (`network.targets.*.to`) and required env have no defaults; rendering fails until the
  platform supplies them. Image digests are validated by `values.schema.json`.
- Probes: `/healthz` where the service has it (gateway, quarantine, replay-controller, retry-controller,
  connector-manager on its metrics port). query-api, file-api, file-worker, export-worker and admin-bff
  have no health endpoint, so they get a TCP probe; replay-worker has no listener and no probe.
- Validate: `helm lint charts/astryon-services -f charts/astryon-services/ci/values-lint.yaml` and
  `helm template r charts/astryon-services -f .../ci/values-lint.yaml | kubeconform -strict`.
  `ci/values-lint.yaml` holds placeholders (example.invalid, TEST-NET CIDRs) and is not deployable.
- `argocd/` has Application templates (placeholder repo URL, no automated sync).
