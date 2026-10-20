# PKI role "astryon-internal" (apply with: vault write pki_int/roles/astryon-internal <key=value...>)
# HCL form for Terraform vault_pki_secret_backend_role equivalent.
allowed_domains    = ["astryon.svc", "svc.cluster.local", "localhost"]
allow_subdomains   = true
allow_bare_domains = false
allow_localhost    = true
server_flag        = true
client_flag        = true
key_type           = "ec"
key_bits           = 256
max_ttl            = "72h"
ttl                = "24h"
require_cn         = false
