# ASTRYON GitOps (Argo CD)

Layout: `appproject.yaml` (project `astryon`), `platform-applicationset.yaml` (namespaces/quotas from
`infra/kubernetes/overlays/<env>`), `services-dev-applicationset.yaml` (auto-sync, prune, self-heal),
`services-promoted-applicationset.yaml` (stage/prod, **manual sync**). Services use the chart
`infra/helm/astryon-service` with `infra/helm/values/<svc>.yaml` + `values/env/<env>/<svc>.yaml` (multi-source, Argo CD >= 2.6).

## Bootstrap
1. Install Argo CD: `kubectl create ns argocd && kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/v2.12.0/manifests/install.yaml`
2. Replace `https://github.com/astryon/ASTRYON.git` in all files here with the real repo URL; register it (`argocd repo add ...` with read-only deploy key).
3. Set real AAS/Kafka/ClickHouse endpoints in `infra/helm/values/env/*/` (placeholders use `*.astryon.example`).
4. `kubectl apply -f infra/argocd/appproject.yaml` then the three ApplicationSets.
5. Create secrets out-of-band in each `astryon-<env>` namespace (never in git; use ESO/Vault, DPP-014):
   `gateway-secrets` (ASTRYON_KAFKA_SASL_USERNAME/PASSWORD), `kafka-ca` (ca.crt), `clickhouse-sink-secrets` (CHSINK_CH_PASSWORD).
6. Platform apps first (`platform-<env>` creates namespaces), then services. Stage/prod: `argocd app sync <svc>-<env>` after review.

CI (DPP-013) should push images to `ghcr.io/astryon/{gateway,clickhouse-sink}` and bump `image.tag` in `values/env/<env>/`.
Ingress controller namespace `ingress-nginx` is assumed by the gateway NetworkPolicy; adjust in `values/gateway.yaml`.
