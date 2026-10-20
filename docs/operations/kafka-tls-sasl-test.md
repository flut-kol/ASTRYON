# Kafka TLS + SASL live test harness

Proves the Kafka client security config of `services/gateway`, `services/clickhouse-sink` and
`services/replay-worker` against a secured broker. Tests are env-gated (skipped in `make test`).
Run 2026-10-20 against `apache/kafka:3.9.0` (single-node KRaft). Nothing here is committed: certs and
passwords are generated at runtime into a throwaway directory.

## Harness

1. Throwaway CA + broker cert (SAN `localhost`, `127.0.0.1`, container name) + PKCS12 keystore via `openssl`;
   a second unrelated CA for the untrusted-CA case.
2. Broker `server.properties`: `EXTERNAL://:29093` = `SASL_SSL` (mechanisms `SCRAM-SHA-512,SCRAM-SHA-256,PLAIN`),
   internal listener plaintext and unpublished, `StandardAuthorizer`, `allow.everyone.if.no.acl.found=false`.
3. SCRAM credentials created at format time: `kafka-storage.sh format ... --add-scram "SCRAM-SHA-512=[name=app,password=...]"`
   (and `SCRAM-SHA-256` for `app`, `SCRAM-SHA-512` for `denied`); PLAIN user `app` via JAAS `user_app`.
4. ACLs (`kafka-acls.sh` over the internal listener): `app` = ALL on topic prefix `ksec.` and group prefix `ksec`,
   IdempotentWrite on cluster; `denied` has none. Topics: `ksec.gw`, `ksec.sink`, `ksec.sink.quar`, `ksec.replay.jobs`, `ksec.replay.out`.
5. Container on a user-defined network, port published only on `127.0.0.1:29093`.

## Test commands (per service, from its module dir)

```
GATEWAY_KAFKA_IT_BROKER=127.0.0.1:29093 GATEWAY_KAFKA_IT_CA=ca.pem GATEWAY_KAFKA_IT_OTHER_CA=other-ca.pem \
GATEWAY_KAFKA_IT_USER=app GATEWAY_KAFKA_IT_PASS=... GATEWAY_KAFKA_IT_DENIED_USER=denied GATEWAY_KAFKA_IT_DENIED_PASS=... \
GATEWAY_KAFKA_IT_CONTAINER=<broker container, restarted by the reconnect test> \
  go test ./internal/gateway -run TestIT_ -v
```

Same variable set with prefix `REPLAYW_KAFKA_IT_` (`go test ./internal/worker -run TestIT_`) and
`CHSINK_KAFKA_IT_` (`go test ./internal/sink -run TestIT_`). Run the reconnect test alone (it restarts the broker).
Always-on unit tests (no broker): `kafka_tls_unit_test.go` in each service (config fail-closed, CA file handling,
hostname mismatch via an in-process TLS server, password redaction).

## Results (all pass)

| Case | gateway | clickhouse-sink | replay-worker |
|---|---|---|---|
| Round trip SCRAM-SHA-512 / SCRAM-SHA-256 / PLAIN over SASL_SSL | ok | ok | ok (jobs consume+commit, sink publish) |
| Wrong password: error, no record lands, password not in error, cause named | ok | ok | ok |
| TLS without SASL, or SASL without TLS, vs secured broker: fails | ok | ok | ok |
| Untrusted CA / system roots: `x509: certificate signed by unknown authority` | ok | ok | ok |
| Hostname mismatch (cert for other name) rejected (unit, in-process) | ok | ok | ok |
| User without ACL: `TOPIC_AUTHORIZATION_FAILED` within ~0.2 s, no hang | ok | ok | ok |
| Same client reconnects after `docker restart` of the broker | ok | ok | ok |

## Bugs found and fixed

- clickhouse-sink had no Kafka TLS/SASL support at all (plaintext only): added `CHSINK_KAFKA_TLS`, `_TLS_CA_FILE`, `_SASL_MECHANISM/_USERNAME/_PASSWORD`.
- All three: `ASTRYON_KAFKA_TLS` was `== "1"`, so `true`/`yes` silently meant plaintext; now 1/true/0/false only, anything else is an error.
  A CA file without TLS, and SASL PLAIN without TLS, are now config errors (fail closed).
- All three: TLS/SASL failures were retried silently and surfaced only as `context deadline exceeded`; the last
  TLS-verify / SASL error (password redacted, expires after 30 s) is now appended, and idle consumers (sink, replay-worker)
  return an error for permanent causes (rejected credentials, untrusted certificate) instead of looking like an empty topic.

## Not covered

mTLS client certificates (not supported by the config), SASL OAUTHBEARER/GSSAPI, SCRAM credential rotation while running,
multi-broker clusters, certificate expiry/rotation without restart (CA is read once at startup).
