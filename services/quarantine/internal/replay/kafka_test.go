package replay

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type fakeKafkaRecords struct {
	topics []string
	keys   [][]byte
	values [][]byte
	err    error
}

func (f *fakeKafkaRecords) Produce(_ context.Context, topic string, key, value []byte) error {
	f.topics = append(f.topics, topic)
	f.keys = append(f.keys, append([]byte(nil), key...))
	f.values = append(f.values, append([]byte(nil), value...))
	return f.err
}

func (*fakeKafkaRecords) Close() {}

func validReplayJob() ReplayJob {
	request := validReplayRequest()
	return ReplayJob{
		JobID: "job-1", TenantID: tenantA, SourceID: request.SourceID,
		OccurredFrom: request.OccurredFrom, OccurredTo: request.OccurredTo,
		PipelineVersion: request.PipelineVersion, CandidateCount: 42, CreatedAt: time.Now().UTC(),
	}
}

func TestKafkaPublisherUsesDedicatedTopicTenantKeyAndBoundedBody(t *testing.T) {
	records := &fakeKafkaRecords{}
	publisher := &KafkaPublisher{producer: records, replayTopic: TopicQuarantineReplayJobs}
	job := validReplayJob()
	if err := publisher.PublishReplayJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if len(records.topics) != 1 || records.topics[0] != TopicQuarantineReplayJobs ||
		string(records.keys[0]) != tenantA+"\x1f"+job.SourceID {
		t.Fatalf("topics=%v keys=%q", records.topics, records.keys)
	}
	var got ReplayJob
	if err := json.Unmarshal(records.values[0], &got); err != nil || got.TenantID != tenantA || got.CandidateCount != 42 {
		t.Fatalf("job=%+v err=%v", got, err)
	}

	job.CandidateCount = maxReplayCandidates + 1
	if err := publisher.PublishReplayJob(context.Background(), job); !errors.Is(err, ErrInvalidReplayRequest) || len(records.values) != 1 {
		t.Fatalf("unbounded job err=%v records=%d", err, len(records.values))
	}
}

func TestKafkaPublisherAuditUsesSharedAuditTopic(t *testing.T) {
	records := &fakeKafkaRecords{}
	publisher := &KafkaPublisher{producer: records, replayTopic: TopicQuarantineReplayJobs}
	event := replayAuditEvent(validReplayJob(), "operator", "request-1", "prepared", "pending", time.Now().UTC())
	if err := publisher.RecordReplayAudit(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if len(records.topics) != 1 || records.topics[0] != TopicAudit || string(records.keys[0]) != tenantA {
		t.Fatalf("topics=%v keys=%q", records.topics, records.keys)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(records.values[0], &wire); err != nil {
		t.Fatal(err)
	}
	wantKeys := []string{
		"event_id", "ts", "tenant_id", "subject", "action", "replay_id", "source_id",
		"from_version", "to_version", "outcome", "phase", "request_id",
	}
	if len(wire) != len(wantKeys) {
		t.Fatalf("audit keys=%v", wire)
	}
	for _, key := range wantKeys {
		if _, ok := wire[key]; !ok {
			t.Fatalf("audit missing shared field %q: %s", key, records.values[0])
		}
	}
	if _, exists := wire["job_id"]; exists || string(wire["from_version"]) != "null" || string(wire["to_version"]) != "null" ||
		string(wire["action"]) != `"replay.started"` || string(wire["replay_id"]) != `"job-1"` {
		t.Fatalf("incompatible replay audit: %s", records.values[0])
	}
	records.err = errors.New("broker down")
	if err := publisher.RecordReplayAudit(context.Background(), event); err == nil {
		t.Fatal("audit broker error was ignored")
	}
}

func TestLoadKafkaConfigFailsClosed(t *testing.T) {
	env := map[string]string{"ASTRYON_KAFKA_BROKERS": "kafka-1:9092, kafka-2:9092"}
	getenv := func(key string) string { return env[key] }
	c, err := LoadKafkaConfig(getenv)
	if err != nil || len(c.Brokers) != 2 || c.ReplayTopic != TopicQuarantineReplayJobs || c.ClientID != "astryon-quarantine" {
		t.Fatalf("config=%+v err=%v", c, err)
	}
	env["ASTRYON_KAFKA_SASL_MECHANISM"] = "plain"
	env["ASTRYON_KAFKA_SASL_USERNAME"] = "user"
	env["ASTRYON_KAFKA_SASL_PASSWORD"] = "password"
	if _, err := LoadKafkaConfig(getenv); err == nil {
		t.Fatal("PLAIN without TLS accepted")
	}
	env["ASTRYON_KAFKA_TLS"] = "true"
	if _, err := LoadKafkaConfig(getenv); err != nil {
		t.Fatal(err)
	}
	env["QUARANTINE_REPLAY_TOPIC"] = "bad topic"
	if _, err := LoadKafkaConfig(getenv); err == nil {
		t.Fatal("invalid topic accepted")
	}
}
