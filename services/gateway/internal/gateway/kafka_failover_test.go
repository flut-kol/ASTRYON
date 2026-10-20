package gateway

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestKafkaPublisherBrokerFailover (DPP-025) runs against a real throwaway
// Kafka and is skipped unless ASTRYON_KAFKA_IT_FAILOVER_BROKER is set.
//
//	ASTRYON_KAFKA_IT_FAILOVER_BROKER   host:port of the broker (e.g. 127.0.0.1:19592)
//	ASTRYON_KAFKA_IT_FAILOVER_COMPOSE  "<project>|<compose file>" used to stop/start service "kafka"
//
// Run (throwaway project, never the shared dev stack):
//
//	COMPOSE_PROJECT_NAME=gwfail-1 KAFKA_HOST_PORT=19592 scripts/dev/up.sh kafka
//	ASTRYON_KAFKA_IT_FAILOVER_BROKER=127.0.0.1:19592 \
//	ASTRYON_KAFKA_IT_FAILOVER_COMPOSE="gwfail-1|$PWD/infra/docker/docker-compose.dev.yml" \
//	  go test ./internal/gateway -run BrokerFailover -v
//	docker compose -p gwfail-1 -f infra/docker/docker-compose.dev.yml down -v
//
// Proves: while the broker is down Publish fails within DeliveryTimeout and never returns nil (no false ack);
// a record is in the log iff its Publish returned nil; Publish succeeds again
// once the broker is back.
func TestKafkaPublisherBrokerFailover(t *testing.T) {
	broker := os.Getenv("ASTRYON_KAFKA_IT_FAILOVER_BROKER")
	if broker == "" {
		t.Skip("ASTRYON_KAFKA_IT_FAILOVER_BROKER not set")
	}
	var proj, file string
	if _, err := fmt.Sscanf(replacePipe(os.Getenv("ASTRYON_KAFKA_IT_FAILOVER_COMPOSE")), "%s %s", &proj, &file); err != nil || proj == "" || file == "" {
		t.Fatal("ASTRYON_KAFKA_IT_FAILOVER_COMPOSE must be \"<project>|<compose file>\"")
	}
	compose := func(args ...string) {
		t.Helper()
		out, err := exec.Command("docker", append([]string{"compose", "-p", proj, "-f", file}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker compose %v: %v\n%s", args, err, out)
		}
	}
	topic := fmt.Sprintf("gwfail.%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	admin, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	one := int32(1)
	rf := int16(1)
	req := kmsg.NewCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic, ct.NumPartitions, ct.ReplicationFactor = topic, one, rf
	req.Topics = append(req.Topics, ct)
	resp, err := req.RequestWith(ctx, admin)
	if err != nil || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create topic: %v %+v", err, resp)
	}

	pub, err := NewKafkaPublisher(KafkaConfig{Brokers: []string{broker}, ClientID: "gwfail-it", DeliveryTimeout: 4 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Publish(ctx, topic, []byte("k"), []byte("A-before")); err != nil {
		t.Fatalf("publish with broker up: %v", err)
	}

	compose("stop", "-t", "5", "kafka")
	restarted := false
	defer func() {
		if !restarted {
			exec.Command("docker", "compose", "-p", proj, "-f", file, "start", "kafka").Run()
		}
	}()
	// B is published while the broker is down. franz-go 1.18.1 alone blocks far past
	// DeliveryTimeout (metadata retries); franzProducer.produce bounds the wait.
	type result struct {
		err error
		at  time.Time
	}
	bres := make(chan result, 1)
	go func() {
		err := pub.Publish(ctx, topic, []byte("k"), []byte("B-during-outage"))
		bres <- result{err, time.Now()}
	}()
	select {
	case r := <-bres:
		if r.err == nil {
			t.Fatal("Publish returned nil (ack) while broker was down")
		}
		t.Logf("outage publish failed (no ack): %v", r.err)
		bres <- r // re-queue for the post-recovery check
	case <-time.After(8 * time.Second):
		t.Fatal("Publish blocked past DeliveryTimeout (4s) during outage; it must fail fast")
	}

	compose("start", "kafka")
	restarted = true
	var lastErr error
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if lastErr = publishBounded(ctx, pub, topic, "C-after-recovery"); lastErr == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if lastErr != nil {
		t.Fatalf("publisher did not recover: %v", lastErr)
	}

	// B is either acked after recovery (then it must be in the log once) or
	// failed (then it must be absent). Never acked-but-missing.
	var bAcked bool
	select {
	case r := <-bres:
		bAcked = r.err == nil
		t.Logf("B resolved after recovery: err=%v", r.err)
	case <-time.After(90 * time.Second):
		t.Fatal("outage publish never resolved after broker recovery")
	}

	cons, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	got := map[string]int{}
	cctx, ccancel := context.WithTimeout(ctx, 20*time.Second)
	defer ccancel()
	for got["A-before"] == 0 || got["C-after-recovery"] == 0 {
		fs := cons.PollFetches(cctx)
		if cctx.Err() != nil {
			break
		}
		fs.EachRecord(func(r *kgo.Record) { got[string(r.Value)]++ })
	}
	// small extra drain so a stray B would be seen
	dctx, dcancel := context.WithTimeout(ctx, 2*time.Second)
	defer dcancel()
	cons.PollFetches(dctx).EachRecord(func(r *kgo.Record) { got[string(r.Value)]++ })

	if got["A-before"] != 1 || got["C-after-recovery"] != 1 {
		t.Fatalf("acked records missing/duplicated in log: %v", got)
	}
	if bAcked && got["B-during-outage"] != 1 {
		t.Fatalf("B acked but not exactly once in log: %v", got)
	}
	// A timed-out B may still land late (at-least-once); clients retry with the same Idempotency-Key.
	if !bAcked && got["B-during-outage"] > 1 {
		t.Fatalf("B duplicated in log: %v", got)
	}
	t.Logf("log contents: %v", got)
}

// replacePipe turns "proj|file" into "proj file" for Sscanf (paths w/o spaces).
func replacePipe(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c == '|' {
			b[i] = ' '
		}
	}
	return string(b)
}

func publishBounded(ctx context.Context, pub *KafkaPublisher, topic, v string) error {
	c, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return pub.Publish(c, topic, []byte("k"), []byte(v))
}
