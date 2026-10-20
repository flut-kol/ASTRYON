# CI security and provenance plan

CI blocks merges on formatting, Go vet, tests, and known Go vulnerabilities.
Security automation runs CodeQL, repository secret scanning, dependency review
for pull requests, and publishes a CycloneDX SBOM artifact.

Image publication is intentionally not automated until a registry and release
identity are approved. That workflow must build from an immutable commit,
attach the SBOM, sign image and provenance with Sigstore OIDC or a managed
external signing key, and deploy only digest-pinned, signature-verified images.
No registry credentials, signing keys, or tokens belong in this repository.
