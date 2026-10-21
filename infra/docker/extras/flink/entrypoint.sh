#!/usr/bin/env bash
# Starts a local Flink cluster, submits the processing job detached, and keeps the container in the foreground.
set -euo pipefail
cat >>/opt/flink/conf/config.yaml <<CONF
s3.endpoint: ${S3_ENDPOINT:-http://s3mock:9090}
s3.path.style.access: true
s3.access-key: ${S3_ACCESS_KEY:-demo}
s3.secret-key: ${S3_SECRET_KEY:-demo-dev-only}
taskmanager.numberOfTaskSlots: 2
rest.bind-address: 0.0.0.0
CONF
/opt/flink/bin/start-cluster.sh
for i in $(seq 1 30); do curl -fsS http://127.0.0.1:8081/overview >/dev/null 2>&1 && break; sleep 2; done
/opt/flink/bin/flink run -d /opt/flink/usrlib/processing.jar
exec tail -F /opt/flink/log/*.log
