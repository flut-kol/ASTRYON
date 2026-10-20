# ASTRYON Ansible baseline (DPP-011)

Idempotent Debian/Ubuntu baseline for packages, key-only admins, SSH hardening,
chrony, existing mounts, opt-in netplan configuration and sysctls. It does not
provision Kafka HA, PostgreSQL HA, Ceph, Kubernetes, secrets or applications.

Use a private inventory outside this repository. Do not commit real hostnames,
addresses, public keys, vault data, passwords or tokens. The play refuses to
disable SSH passwords without a key-based administrator. Mounts never format
disks. Firewall and `netplan apply` are opt-in; test them on a canary and retain
console access.

```bash
cd infra/ansible
ansible-galaxy collection install -r requirements.yml
ansible-playbook -i /secure/inventory/hosts.yml site.yml --check --diff
ansible-playbook -i /secure/inventory/hosts.yml site.yml
```
