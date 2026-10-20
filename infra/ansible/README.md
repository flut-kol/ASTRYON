# infra/ansible (DPP-011)

Roles: `common` (packages, users, SSH hardening, sysctl, limits, THP off, swappiness, chrony),
`disks` (XFS, noatime, data dirs), `network` (LACP bond via netplan/NetworkManager, MTU),
`hardening` (auditd, nftables role allow-lists, unattended security updates), `node_exporter`.
Target OS: Debian/Ubuntu (apt, `chrony`, `ssh` service names).

## Setup
    ansible-galaxy collection install -r requirements.yml
    cd infra/ansible

## Secrets
No secrets in the repo. Put them in `inventories/dev/group_vars/all/vault.yml` (not committed),
encrypt with `ansible-vault encrypt`, and reference vars such as `vault_admin_ssh_keys`.
Use `--ask-vault-pass` or a `vault_password_file` outside the repo. Set `node_exporter_checksum`
(`sha256:<hex>`) from the official release sha256sums.

## Usage
    ansible-playbook site.yml --syntax-check
    ansible-playbook site.yml --check --diff                 # dry run, no changes
    ansible-playbook site.yml --check --diff --limit kafka   # one group
    ansible-playbook site.yml --limit kafka --tags common,disks
    ansible-playbook site.yml                                 # apply

Check mode notes: `command`-based handlers and downloads are skipped/simulated; SSH and nftables
configs are validated (`sshd -t`, `nft -c`). Roll SSH/firewall changes out with `--limit` to a
single host first and keep an out-of-band session open.

## Safety switches
- `disks_format_enabled: false` by default; set true per host only for empty devices (`force: false`).
- `network_bond_enabled: false` by default; set NIC names, addresses and `network_mtu` per host.
- nftables is default-drop inbound; per-role ports come from `firewall_role_tcp_ports` in group_vars,
  SSH only from `mgmt_cidrs`. Adjust the example CIDRs (placeholders) before applying.

## Layout
`inventories/dev/hosts.yml` groups: kafka, clickhouse, postgres, ceph, k8s_worker; per-group tuning in `group_vars/`.
