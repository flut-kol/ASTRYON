# Supply chain: CI, SBOM, signing (DPP-013)

| Control | Where | Gate |
|---|---|---|
| gofmt / vet / golangci-lint | `ci.yml` `lint` (`make lint`, `.golangci.yml`) | blocks PR |
| Unit tests + race (gateway) | `ci.yml` `test` (`make test`) | blocks PR |
| SAST | CodeQL (Go) + gosec, `sast` | blocks PR (medium+) |
| Dependency scan (SCA) | govulncheck (reachable vulns) + Trivy fs HIGH/CRITICAL, `dependency-scan` | blocks PR |
| Secrets scan | gitleaks over full history, `secrets-scan` | blocks PR |
| SBOM | CycloneDX per module on every run (`sbom`, 90-day artifact); per release in `release.yml` | informational |
| Signing | Release tags `v*`: cosign keyless (Sigstore OIDC, no long-lived keys) on binaries + SBOMs | release |
| Provenance | `actions/attest-build-provenance` (SLSA build provenance) | release |
| Updates | Dependabot weekly: gomod (all modules) + GitHub Actions | PRs |

## Container registry plan (completes with DPP-012)
No Dockerfiles exist yet. When images are added: build in `release.yml`, push by digest,
`cosign sign <image>@<digest>` (keyless), `cosign attest --type cyclonedx` with the SBOM,
Trivy image scan before push, and admission policy in Kubernetes (Kyverno/sigstore policy-controller)
to accept only images signed by this repo's workflow identity.

## Follow-ups (need org decisions)
- Pin third-party actions to commit SHAs (currently major tags).
- Mark `lint`, `test`, `sast`, `dependency-scan`, `secrets-scan` as required status checks on `main`.
- Self-hosted vs GitHub-hosted runners / registry choice.
