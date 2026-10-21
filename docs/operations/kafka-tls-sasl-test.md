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

SASL OAUTHBEARER/GSSAPI, SCRAM credential rotation while running, multi-broker clusters. mTLS and certificate/CA rotation: see the next section.

## mTLS and certificate/CA rotation

All four Kafka clients (gateway, clickhouse-sink, replay-worker, replay-controller; each has its own copy of `kafka_mtls.go`)
support client certificates and hot reload:

- `<P>_KAFKA_TLS_CERT_FILE` + `<P>_KAFKA_TLS_KEY_FILE` (PEM): both or neither, and TLS must be on, else a startup config error.
  `<P>` is `ASTRYON` (gateway, replay-controller, replay-worker) or `CHSINK` (clickhouse-sink).
- `<P>_KAFKA_TLS_RELOAD_INTERVAL` (Go duration, default `1m`, `0` = off, negative/garbage = config error).
- Reload: on each new broker connection (at most once per interval) the cert/key and CA files are stat'ed; if mtime or size changed they are re-read.
  Every dial builds a fresh `tls.Config` (current CA pool as `RootCAs`, `GetClientCertificate` returning the current pair), so there is no
  `InsecureSkipVerify` and the server chain and hostname are verified against the current CA. A file that fails to load (partial write, bad PEM,
  cert/key mismatch) keeps the last good material and is retried every interval; the failure is logged once per minute with the path/OS error only,
  never key material. Established connections keep their handshake until the broker or the client closes them (franz-go idle timeout, broker restart).

Harness (throwaway, nothing committed; certs in `/private/tmp/claude-501/chagg-mtls-certs`, generated with `openssl`): container `chagg-mtls-kafka`
(`apache/kafka:3.9.0`, KRaft) on network `chagg-mtls-net`. Listeners: `SSL://:29094` with `ssl.client.auth=required`, PEM truststore `/trust/client-ca.pem`
(bind-mounted, rewritten by the rotation test), `ssl.principal.mapping.rules` CN -> user `app` (ACLs as above); `EXTERNAL://:29093` = SASL_SSL as before;
internal plaintext listener unpublished. Both published on `127.0.0.1` only.

```
GATEWAY_KAFKA_MTLS_IT_BROKER=127.0.0.1:29094 GATEWAY_KAFKA_MTLS_IT_CA=ca.pem GATEWAY_KAFKA_MTLS_IT_CERT=client.crt GATEWAY_KAFKA_MTLS_IT_KEY=client.key \
GATEWAY_KAFKA_MTLS_IT_TRUST_FILE=<host path of /trust/client-ca.pem> GATEWAY_KAFKA_MTLS_IT_CONTAINER=chagg-mtls-kafka \
  go test ./internal/gateway -run TestIT_MTLS -v
```
Prefixes: `CHSINK_` (`./internal/sink`), `REPLAYW_` (`./internal/worker`), `REPLAYC_` (`./internal/replay`). Topic `ksec.mtls` (override `_TOPIC`).
The last test restarts the broker (makes it re-read the truststore): run it alone if other tests share the broker; it restores the original trust file.

Live cases per service: round trip with a client cert (read back by an independent mTLS consumer); rejected without a cert; rejected with a cert from another CA
(same CN); running client with a stale CA bundle fails with `x509: certificate signed by unknown authority`, then recovers after the CA file is replaced; running client
recovers after BOTH its cert/key and the broker's trusted client CA were replaced (client not restarted; a half-written key/cert pair in between does not break it),
while a control client without reload (old cert) is rejected. SASL_SSL round trips on `:29093` still pass on the same broker.
Always-on unit tests (`kafka_mtls_test.go`): config matrix, renewed cert and renewed CA picked up, broken new files keep the old ones and log once a minute without
leaking content, interval 0 never re-reads, in-process TLS server requiring client certs (valid / none / other CA).

Limits: rejections by the broker after the handshake (TLS 1.3 client-cert alerts) surface as `context deadline exceeded` without a cause (the broker log says
`SSL handshake failed`); a client-side CA/cert problem does name its cause. Reload applies to new connections only. Single broker; the broker truststore was
re-read by restart, not by dynamic reconfiguration. Key files with a passphrase are not supported.
