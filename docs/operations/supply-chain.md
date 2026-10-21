# Supply-chain controls (DPP-013)

## Current CI gate

GitHub Actions runs `.github/workflows/ci.yml` on every pull request, pushes to
`main`, and a weekly scheduled scan. The checks are intentionally per Go module
because ASTRYON is a Go workspace with independent service modules:

- `make lint` and `make test` validate formatting, static checks and tests;
- `govulncheck` detects reachable known vulnerabilities;
- `gosec` performs Go SAST;
- Gitleaks scans the complete Git history for accidentally committed secrets;
- Syft produces an SPDX JSON SBOM from the repository source.

Failures block merging once the `CI` workflow is configured as a required
status check on `main`. Until then, the workflow is evidence, not an enforced
release gate.

## Release provenance and signing plan

No image may be promoted solely because it has a mutable tag. The release
workflow (introduced with the first deployable service image) must:

1. build one image per service from a protected tag or immutable commit;
2. publish by digest to the approved registry;
3. generate an image SBOM in SPDX JSON and attach it to the release record;
4. create SLSA-compatible build provenance that records the source commit,
   builder identity, build inputs and output digest;
5. sign the digest and attestation with Cosign keyless OIDC, then verify both
   identity and repository workflow before deployment;
6. deploy only digest-pinned images, after the matching SBOM/vulnerability gate
   has passed.

Registry credentials, signing identities, webhook tokens and private keys live
only in GitHub environments or the external secret manager. They are never put
in workflow YAML, `.env.example`, test fixtures or generated reports.

## Action and scanner maintenance

Third-party actions must be pinned to reviewed immutable commit SHAs before
`main` is protected. Dependabot (or an equivalent controlled update process)
opens the update; the CI/SBOM evidence is reviewed before the SHA changes. Tool
versions are deliberately explicit in the workflow so security results remain
reproducible. Scanner false positives require a documented, time-limited
suppression in the relevant tool configuration; silent `continue-on-error` is
not allowed.

The source SBOM job currently proves generation without publishing an artifact.
The release workflow will retain and attach the image SBOM, because that is the
artifact consumers actually deploy.

## Go toolchain and govulncheck (2026-10-21)

The repository moved from Go 1.22 to Go 1.25 (`go.work` and every module `go 1.25.0`, CI `1.25.x`, Dockerfiles
`golang:1.25`) because the fixes for GO-2026-6629, GO-2026-5970 (`golang.org/x/text` v0.41.0) and GO-2026-5004
(`github.com/jackc/pgx/v5` v5.9.2) require `go 1.25.0`. After the upgrade `govulncheck ./...` reports no
vulnerabilities in any of the 15 Go modules and `make test` and `make lint` pass. Container images were not
rebuilt in this change.
