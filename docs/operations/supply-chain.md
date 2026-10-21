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

The repository is on Go 1.27: `go.work` and every module declare `go 1.27.0`, CI uses `1.27.x`, Dockerfiles
`golang:1.27`. Reasons: Go 1.25.14's standard library carries 10 reachable vulnerabilities (net/http, crypto/tls,
crypto/x509, net/textproto) fixed only in Go 1.26.9+, and `pgx` v5.9.2 / `golang.org/x/text` v0.41.0 (fixes for
GO-2026-5004, GO-2026-6629, GO-2026-5970) need Go 1.25. With Go 1.27.3 `govulncheck ./...` reports nothing in
any of the 16 Go modules, `make test` and `make lint` pass, and the five service images build.

`gosec` releases up to v2.29.0 cannot read Go 1.27 export data ("export data version 5 is greater than maximum
supported version 4"), so CI pins the unreleased pseudo-version `v2.29.1-0.20261020094257-14ef84f6fecc`; replace it with the
first tagged release that supports Go 1.27.

`gosec ./...` is clean in every module (checked 2026-10-21 with the pinned build on Go 1.27.3). Fixed: clickhouse-sink
metrics server now has `ReadHeaderTimeout` (G114), unchecked `Close`/`Write`/`Encode` results are explicit `_ =` (G104),
load-test report file mode 0600, request IDs are logged with `%q`. Justified with `#nosec <rule> -- reason` on the line:
operator-configured paths and URLs (G304, G704), config values in fatal logs (G706), bounded counters and lengths (G115),
non-security jitter and seeded load data (G404), SHA-1 for Redis EVALSHA digests (G401, G505), and the Prometheus
textfile mode (G302). Review `#nosec` lines when the surrounding code changes.
