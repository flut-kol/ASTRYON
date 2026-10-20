package replay

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Live Kafka audit test, skipped unless REPLAY_KAFKA_IT_BROKERS is set. REPLAY_KAFKA_IT_CONTAINER names the docker
// container running apache/kafka: the event is read back there with kafka-console-consumer.sh.
func TestKafkaAuditSinkLive(t *testing.T) {
	brokers, container := os.Getenv("REPLAY_KAFKA_IT_BROKERS"), os.Getenv("REPLAY_KAFKA_IT_CONTAINER")
	if brokers == "" || container == "" {
		t.Skip("REPLAY_KAFKA_IT_BROKERS / REPLAY_KAFKA_IT_CONTAINER not set")
	}
	cfg, err := LoadKafkaConfig(func(k string) string { return map[string]string{"ASTRYON_KAFKA_BROKERS": brokers}[k] })
	if err != nil {
		t.Fatal(err)
	}
	sink, err := NewKafkaAuditSink(cfg, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	from, to := int32(1), int32(2)
	ev := AuditEvent{Time: time.Now().UTC(), TenantID: "11111111-1111-4111-8111-111111111111", Subject: "alice", Action: ActionActivate,
		ReplayID: "00000000-0000-4000-8000-0000000000c2", SourceID: "22222222-2222-4222-8222-222222222222",
		FromVersion: &from, ToVersion: &to, Outcome: OutcomeSuccess, RequestID: "live-req-1"}
	ev.EventID = NewEventID(ev)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := sink.Record(ctx, ev); err != nil {
		t.Fatal(err)
	}
	out, err := exec.CommandContext(ctx, "docker", "exec", container, "/opt/kafka/bin/kafka-console-consumer.sh",
		"--bootstrap-server", "localhost:9092", "--topic", TopicAudit, "--from-beginning", "--max-messages", "1",
		"--timeout-ms", "30000", "--property", "print.key=true", "--property", "key.separator=|").CombinedOutput()
	if err != nil {
		t.Fatalf("console consumer: %v\n%s", err, out)
	}
	var key, val string
	for _, l := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(l, ev.TenantID+"|{") {
			key, val, _ = strings.Cut(l, "|")
		}
	}
	if key != ev.TenantID {
		t.Fatalf("event not read back with key tenant_id:\n%s", out)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(val), &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"event_id": ev.EventID, "tenant_id": ev.TenantID, "subject": "alice", "action": "replay.activate",
		"replay_id": ev.ReplayID, "source_id": ev.SourceID, "from_version": float64(1), "to_version": float64(2),
		"outcome": "success", "request_id": "live-req-1"}
	for k, w := range want {
		if got[k] != w {
			t.Fatalf("%s: got %v want %v (%s)", k, got[k], w, val)
		}
	}
	if _, ok := got["ts"].(string); !ok {
		t.Fatal("ts missing")
	}
}
