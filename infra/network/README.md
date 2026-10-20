# Network and hardware acceptance plan (DPP-019)

This directory deliberately contains no commands that change a switch, PDU, BMC,
bond or disk. Complete the private inventory first, validate LACP/MTU/VLANs on
one canary, and record baseline link counters, latency and disk SMART state.

## Approval-gated tests

1. Confirm every Kafka/PG/Ceph placement spans the declared rack and PDU failure
   domains; do not rely on hostnames as evidence.
2. Fail one bond member at a time, then restore it. Confirm traffic continues,
   no unexpected interface flap occurs and counters remain clean.
3. Test an approved BMC power-cycle on a non-critical canary only; verify alert,
   fencing and service recovery.
4. Inject an approved disk failure in a disposable or replicated test set; log
   SMART alerts, rebuild time, data availability and any operator action.
5. Attach timestamps, change ticket, operator, rollback step and measured
   impact to the acceptance evidence.

Never pull power, disable switch ports, alter RAID, or change BMC credentials
from automation without an explicit maintenance window and hardware owner.
