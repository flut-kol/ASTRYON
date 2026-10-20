# Read-only access to ASTRYON KV v2 secrets and certificate issuance.
path "secret/data/astryon/*" {
  capabilities = ["read"]
}
path "secret/metadata/astryon/*" {
  capabilities = ["list"]
}
path "pki_int/issue/astryon-internal" {
  capabilities = ["update"]
}
