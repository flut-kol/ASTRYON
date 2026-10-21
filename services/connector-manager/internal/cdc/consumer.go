package cdc

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	ec "github.com/astryon/event-contracts"
)

// Record is one consumed Kafka message from the Debezium topic.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte // nil = Debezium tombstone after a delete (ignored)
}

// Feed is the Kafka seam. Poll returns at most max records; Commit durably records consumer progress for them.
type Feed interface {
	Poll(ctx context.Context, max int) ([]Record, error)
	Commit(ctx context.Context, recs []Record) error
}

// Emitter matches runner.Emitter: nil only when every event is durably accepted.
type Emitter interface {
	Emit(ctx context.Context, tenant, source string, events [][]byte) error
}

// Quarantiner stores a rejected input as an events.quarantine.v1 record. A failure here fails the batch (no commit).
type Quarantiner interface {
	Quarantine(ctx context.Context, q ec.QuarantineRecordV1) error
}

// PositionStore keeps the last source position emitted (monitoring / resume diagnostics). The authoritative resume
// point is the Feed commit; the store is written after emit and before commit.
type PositionStore interface {
	SavePosition(ctx context.Context, connectorID, position string) error
}

type Metrics struct {
	Batches, Emitted, Quarantined, Tombstones atomic.Int64
}

type Consumer struct {
	Feed       Feed
	Emitter    Emitter
	Quarantine Quarantiner
	Positions  PositionStore // optional
	Map        MapConfig
	MaxBatch   int // default 500
	Now        func() time.Time
	M          Metrics
	Producer   string // deviation id namespace, default "connector-manager-cdc"
}

// RunOnce handles one bounded batch: poll -> map (bad events quarantined) -> emit -> save position -> commit.
// Returns the number of polled records. Any error leaves the batch uncommitted; redelivery re-emits identical event ids.
func (c *Consumer) RunOnce(ctx context.Context) (int, error) {
	max := c.MaxBatch
	if max <= 0 {
		max = 500
	}
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	if err := c.Map.Validate(now()); err != nil {
		return 0, err
	}
	recs, err := c.Feed.Poll(ctx, max)
	if err != nil {
		return 0, fmt.Errorf("cdc poll: %w", err)
	}
	if len(recs) == 0 {
		return 0, nil
	}
	if len(recs) > max {
		recs = recs[:max]
	}
	var events [][]byte
	var lastPos string
	for _, r := range recs {
		if r.Value == nil {
			c.M.Tombstones.Add(1)
			continue
		}
		ch, err := ParseChange(r.Value)
		var ev []byte
		if err == nil {
			ev, err = Map(ch, r.Key, c.Map, now())
		}
		if err != nil {
			if !errors.Is(err, ErrMalformed) {
				return 0, err
			}
			if qerr := c.quarantine(ctx, r, err, now()); qerr != nil {
				return 0, qerr
			}
			c.M.Quarantined.Add(1)
			continue
		}
		events = append(events, ev)
		lastPos = ch.Source.Position()
	}
	if len(events) > 0 {
		if err := c.Emitter.Emit(ctx, c.Map.Tenant, c.Map.Source, events); err != nil {
			return 0, fmt.Errorf("cdc emit: %w", err)
		}
		c.M.Emitted.Add(int64(len(events)))
		if c.Positions != nil && lastPos != "" {
			if err := c.Positions.SavePosition(ctx, c.Map.ConnectorID, lastPos); err != nil {
				return 0, fmt.Errorf("cdc position: %w", err)
			}
		}
	}
	if err := c.Feed.Commit(ctx, recs); err != nil {
		return 0, fmt.Errorf("cdc commit: %w", err)
	}
	c.M.Batches.Add(1)
	return len(recs), nil
}

// Run loops until ctx ends; errors back off briefly and the batch is retried from the last commit.
func (c *Consumer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		n, err := c.RunOnce(ctx)
		if err != nil || n == 0 {
			d := 200 * time.Millisecond
			if err != nil {
				d = 2 * time.Second
			}
			select {
			case <-ctx.Done():
			case <-time.After(d):
			}
		}
	}
	return ctx.Err()
}

func (c *Consumer) quarantine(ctx context.Context, r Record, cause error, now time.Time) error {
	prod := c.Producer
	if prod == "" {
		prod = "connector-manager-cdc"
	}
	id, err := ec.QuarantineDeviationIDFromOffset(prod, r.Topic, r.Partition, r.Offset)
	if err != nil {
		return fmt.Errorf("cdc quarantine id: %w", err)
	}
	part, off := r.Partition, r.Offset
	orig := ec.NewQuarantineOriginalV1(r.Value)
	diag := cause.Error()
	if len(diag) > ec.MaxQuarantineDiagnosticBytes {
		diag = diag[:ec.MaxQuarantineDiagnosticBytes]
	}
	q := ec.QuarantineRecordV1{
		ContractVersion: ec.QuarantineContractVersionV1, DeviationID: id, TenantID: c.Map.Tenant, SourceID: c.Map.Source,
		QuarantinedAt: now.UTC(), Stage: "cdc_map", ReasonCode: "CDC_MALFORMED_EVENT", Diagnostic: diag,
		Provenance: ec.QuarantineProvenanceV1{SourceTopic: r.Topic, SourcePartition: &part, SourceOffset: &off},
		Original:   &orig,
	}
	if err := c.Quarantine.Quarantine(ctx, q); err != nil {
		return fmt.Errorf("cdc quarantine: %w", err)
	}
	return nil
}
