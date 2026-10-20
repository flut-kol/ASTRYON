# ASTRYON Argo CD bootstrap

Apply this directory only after replacing every REPLACE_ORG/REPLACE_REPO value
with the approved Git repository URL and verifying that Argo CD runs in the
`argocd` namespace. The AppProject restricts deployments to the dev and stage
namespaces on the in-cluster API server.

Dev is automated with prune and self-heal. Stage deliberately requires a manual
sync until promotion controls, image digest policy and acceptance gates are
approved. Bootstrap credentials, repository tokens and cluster secrets must be
registered in Argo CD or an external secret system, never committed here.

```bash
kubectl apply -k infra/argocd
```
