package replay

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type fakeProducer struct {
	topic      string
	key, value []byte
	err        error
}

func (f *fakeProducer) produce(_ context.Context, topic string, key, value []byte) error {
	if f.err != nil {
		return f.err
	}
	f.topic, f.key, f.value = topic, key, value
	return nil
}
func (f *fakeProducer) close() {}

func TestKafkaJobPublisherKeyTopicAndBody(t *testing.T) {
	fp := &fakeProducer{}
	k := &KafkaJobPublisher{p: fp, topic: TopicReplayJobs}
	job := ReplayJob{JobID: "ab", TenantID: "11111111-1111-1111-1111-111111111111", SourceID: "22222222-2222-2222-2222-222222222222",
		PipelineVersion: 1, ProcessingVersion: 3, CreatedAt: time.Unix(0, 0).UTC()}
	if err := k.Publish(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if fp.topic != "replay.jobs.v1" || string(fp.key) != job.TenantID+"\x1f"+job.SourceID {
		t.Fatalf("topic/key: %q %q", fp.topic, fp.key)
	}
	var got ReplayJob
	if err := json.Unmarshal(fp.value, &got); err != nil || got.ProcessingVersion != 3 || got.TenantID != job.TenantID {
		t.Fatalf("body: %v %+v", err, got)
	}
}

func TestKafkaJobPublisherErrorsPropagate(t *testing.T) {
	boom := errors.New("no ack")
	k := &KafkaJobPublisher{p: &fakeProducer{err: boom}, topic: TopicReplayJobs}
	err := k.Publish(context.Background(), ReplayJob{TenantID: "t", SourceID: "s"})
	if !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
	if err := k.Publish(context.Background(), ReplayJob{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty job: %v", err)
	}
}

func TestLoadKafkaConfig(t *testing.T) {
	env := map[string]string{}
	get := func(k string) string { return env[k] }
	if _, err := LoadKafkaConfig(get); err == nil {
		t.Fatal("brokers required")
	}
	env["ASTRYON_KAFKA_BROKERS"] = "a:1, b:2"
	env["ASTRYON_KAFKA_SASL_MECHANISM"] = "scram-sha-512"
	if _, err := LoadKafkaConfig(get); err == nil {
		t.Fatal("sasl needs creds")
	}
	env["ASTRYON_KAFKA_SASL_USERNAME"], env["ASTRYON_KAFKA_SASL_PASSWORD"] = "u", "p"
	c, err := LoadKafkaConfig(get)
	if err != nil || len(c.Brokers) != 2 || c.saslMechanism() == nil {
		t.Fatalf("%v %+v", err, c)
	}
}
