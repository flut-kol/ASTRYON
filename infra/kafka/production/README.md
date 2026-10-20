# Kafka KRaft production baseline (DPP-016)

The template requires a three-member controller quorum, replication factor 3,
minimum ISR 2, disabled auto-topic creation, TLS and SCRAM ACL enforcement.
Render passwords and keystores only from the secret manager; never commit the
rendered server.properties or principal credentials.

Place brokers/controllers across distinct rack and PDU failure domains. Before
production approval, verify every critical topic has RF=3/min ISR=2, exercise a
single broker and controller loss, confirm producer ACKs obey the documented
failure model, and record recovery/rebalance duration. This baseline is not
compatible with the single-node plaintext Docker development cluster.
