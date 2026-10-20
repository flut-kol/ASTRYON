# Ceph RGW / S3 baseline (DPP-018)

This reviewed desired state requires three replicas across host failure domains,
autoscaled RGW pools, versioned buckets, lifecycle policies and SSE-KMS. Connect
KMS and RGW identities through the selected secret manager; never put access
keys or KMS material in Git.

The policy template is safe only if the RGW identity provider supplies an
immutable tenant_id principal tag. Never trust client-provided prefixes or tags.

Validate on the real topology: lose one OSD/host, verify confirmed objects stay
readable/writable, test multipart/version recovery, then restore RGW metadata
and checksum-known objects into an isolated cluster. Record elapsed time and
gaps before claiming RPO/RTO.
