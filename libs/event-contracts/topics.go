package eventcontracts

import (
	"hash/fnv"
	"time"
)

// Topic names (§5.2).
const (
	TopicRawEvents       = "raw.events.v1"
	TopicValidatedEvents = "validated.events.v1"
	TopicProcessedEvents = "processed.events.v1"
	TopicFileJobs        = "ingestion.filejobs.v1"
	TopicJobStatus       = "processing.jobstatus.v1"
	TopicQuarantine      = "events.quarantine.v1"
	TopicDLQ             = "events.dlq.v1"
	TopicAudit           = "audit.events.v1"
)

// TopicConfig describes initial topic settings. Partition counts are a
// hypothesis, to be confirmed after POC (§5.2).
type TopicConfig struct {
	Name              string
	Partitions        int
	Retention         time.Duration
	ReplicationFactor int
	Critical          bool // enforces acks=all, min.insync.replicas=2, idempotent producer
}

// MinInsyncReplicas returns the required min.insync.replicas (2 for critical, else 1).
func (c TopicConfig) MinInsyncReplicas() int {
	if c.Critical {
		return 2
	}
	return 1
}

const day = 24 * time.Hour

// Topics is the initial topic table. All topics are treated as critical (RF=3).
// audit.events.v1 keeps 7 days in Kafka; longer retention is handled in S3.
var Topics = []TopicConfig{
	{TopicRawEvents, 96, 7 * day, 3, true},
	{TopicValidatedEvents, 96, 3 * day, 3, true},
	{TopicProcessedEvents, 96, 3 * day, 3, true},
	{TopicFileJobs, 24, 7 * day, 3, true},
	{TopicJobStatus, 24, 7 * day, 3, true},
	{TopicQuarantine, 24, 14 * day, 3, true},
	{TopicDLQ, 24, 14 * day, 3, true},
	{TopicAudit, 24, 7 * day, 3, true},
}

// TopicByName looks up a topic config.
func TopicByName(name string) (TopicConfig, bool) {
	for _, t := range Topics {
		if t.Name == name {
			return t, true
		}
	}
	return TopicConfig{}, false
}

// PartitionKey builds the Kafka message key from tenant_id + entity_key.
// A 0x1F separator prevents ambiguity between concatenations.
func PartitionKey(tenantID, entityKey string) []byte {
	k := make([]byte, 0, len(tenantID)+1+len(entityKey))
	k = append(k, tenantID...)
	k = append(k, 0x1f)
	return append(k, entityKey...)
}

// PartitionKey returns the key for this envelope. Without an entity_key, ordering
// is not required, so event_id is used to avoid a per-tenant hot partition.
func (e *Envelope) PartitionKey() []byte {
	if e.EntityKey == "" {
		return PartitionKey(e.TenantID, e.EventID)
	}
	return PartitionKey(e.TenantID, e.EntityKey)
}

// PartitionFor maps a key to a partition in [0,n) with FNV-1a, for skew tests
// and tooling. Real producers use the Kafka client's partitioner.
func PartitionFor(key []byte, n int) int {
	if n <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32() % uint32(n)) // #nosec G115 -- small bounded counter or length, reviewed 2026-10-21
}
