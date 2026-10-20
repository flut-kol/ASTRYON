package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Live Kafka test, skipped unless REPLAY_KAFKA_IT_BROKERS is set (e.g. 127.0.0.1:19892).
// It publishes a job with KafkaJobPublisher and reads it back with a fresh consumer group.
// If REPLAY_KAFKA_IT_TENANT/SOURCE/VERSION/FROM/TO are set, that job (not a random one) is published, so
// the same test feeds services/replay-worker in the live check.
func TestKafkaJobPublisherLive(t *testing.T) {
	brokers := os.Getenv("REPLAY_KAFKA_IT_BROKERS")
	if brokers == "" {
		t.Skip("REPLAY_KAFKA_IT_BROKERS not set")
	}
	env := map[string]string{"ASTRYON_KAFKA_BROKERS": brokers}
	cfg, err := LoadKafkaConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	pub, err := NewKafkaJobPublisher(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	now := time.Now().UTC().Truncate(time.Second)
	job := ReplayJob{JobID: now.Format("150405.000000000"), TenantID: "11111111-1111-4111-8111-111111111111",
		SourceID: "22222222-2222-4222-8222-222222222222", PipelineVersion: 1, ProcessingVersion: 2, CreatedAt: now,
		Ranges: DayRanges(now.Add(-48*time.Hour), now)}
	if v := os.Getenv("REPLAY_KAFKA_IT_TENANT"); v != "" {
		job.TenantID, job.SourceID = v, os.Getenv("REPLAY_KAFKA_IT_SOURCE")
		var pv uint32
		if _, err := fmt.Sscan(os.Getenv("REPLAY_KAFKA_IT_VERSION"), &pv); err != nil {
			t.Fatal(err)
		}
		from, err1 := time.Parse(time.RFC3339, os.Getenv("REPLAY_KAFKA_IT_FROM"))
		to, err2 := time.Parse(time.RFC3339, os.Getenv("REPLAY_KAFKA_IT_TO"))
		if err1 != nil || err2 != nil {
			t.Fatal("bad FROM/TO", err1, err2)
		}
		job.ProcessingVersion, job.Ranges = pv, DayRanges(from, to)
		if id := os.Getenv("REPLAY_KAFKA_IT_JOB_ID"); id != "" {
			job.JobID = id
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, job); err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.ConsumeTopics(TopicReplayJobs),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		var found bool
		fs.EachRecord(func(r *kgo.Record) {
			var got ReplayJob
			if json.Unmarshal(r.Value, &got) == nil && got.JobID == job.JobID && string(r.Key) == string(JobKey(job)) &&
				got.TenantID == job.TenantID && got.ProcessingVersion == job.ProcessingVersion && r.Topic == "replay.jobs.v1" {
				found = true
			}
		})
		if found {
			return
		}
	}
	t.Fatal("published job not read back")
}
