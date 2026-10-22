package replay

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	ec "github.com/astryon/event-contracts"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	TopicQuarantineJournal = "events.quarantine.v1"
	TopicDLQ               = "events.dlq.v1"
	DefaultJournalGroup    = "astryon-quarantine-journal-v1"
	zeroUUID               = "00000000-0000-0000-0000-000000000000"
)

var journalIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// JournalRow is the tenant-safe subset written to processing_errors_local.
// Original is intentionally not carried into the browser read model: it is an
// opaque potentially sensitive copy and requires a separate audited policy.
type JournalRow struct {
	TenantID          string `json:"tenant_id"`
	SourceID          string `json:"source_id"`
	BatchID           string `json:"batch_id"`
	JobID             string `json:"job_id"`
	EventID           string `json:"event_id"`
	ProcessingVersion uint32 `json:"processing_version"`
	PipelineVersion   uint32 `json:"pipeline_version"`
	OccurredAt        string `json:"occurred_at"`
	Stage             string `json:"stage"`
	ErrorCode         string `json:"error_code"`
	ErrorMessage      string `json:"error_message"`
	RawRef            string `json:"raw_ref"`
	SamplePayload     string `json:"sample_payload"`
}

type JournalSink interface {
	InsertQuarantine(context.Context, JournalRow, string) error
}

// ClickHouseJournal writes canonical quarantine records after strict contract validation.
type ClickHouseJournal struct {
	Store *ClickHouseStore
}

func (s ClickHouseJournal) InsertQuarantine(ctx context.Context, row JournalRow, token string) error {
	if s.Store == nil {
		return errors.New("ClickHouse journal store is required")
	}
	return s.Store.insertJournalRow(ctx, row, token)
}

type JournalConsumer struct {
	client *kgo.Client
	sink   JournalSink
	topic  string
	batch  int
	close  func()
}

func NewJournalConsumer(c KafkaConfig, group string, sink JournalSink) (*JournalConsumer, error) {
	if len(c.Brokers) == 0 {
		return nil, errors.New("journal Kafka brokers are required")
	}
	if sink == nil {
		return nil, errors.New("journal sink is required")
	}
	if group == "" {
		group = DefaultJournalGroup
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...), kgo.ClientID(c.ClientID + "-journal"),
		kgo.ConsumerGroup(group), kgo.ConsumeTopics(TopicQuarantineJournal),
		kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.BlockRebalanceOnPoll(),
	}
	if c.TLS {
		tlsConfig, err := loadKafkaTLS(c)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	if mechanism := kafkaSASL(c); mechanism != nil {
		opts = append(opts, kgo.SASL(mechanism))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("journal Kafka client: %w", err)
	}
	return &JournalConsumer{client: client, sink: sink, topic: TopicQuarantineJournal, batch: 100, close: client.Close}, nil
}

func (c *JournalConsumer) Close() {
	if c != nil && c.close != nil {
		c.close()
		c.close = nil
	}
}

// Run processes one bounded Kafka fetch at a time. Offsets are committed only
// after every row in the fetch has been durably acknowledged by ClickHouse.
func (c *JournalConsumer) Run(ctx context.Context) error {
	if c == nil || c.client == nil || c.sink == nil {
		return errors.New("journal consumer is not configured")
	}
	defer func() {
		// BlockRebalanceOnPoll keeps ownership stable while the fetched records
		// are acknowledged and committed. Always release it before closing,
		// otherwise kgo.Close waits for the blocked group transition.
		c.client.AllowRebalance()
		c.Close()
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		fetch := c.client.PollRecords(ctx, c.batch)
		if fetch.IsClientClosed() {
			return errors.New("journal Kafka client closed")
		}
		if errs := fetch.Errors(); len(errs) > 0 {
			return errs[0].Err
		}
		records := make([]*kgo.Record, 0, c.batch)
		fetch.EachRecord(func(record *kgo.Record) { records = append(records, record) })
		if len(records) == 0 {
			c.client.AllowRebalance()
			continue
		}
		for _, record := range records {
			row, err := DecodeJournalRecord(record.Value, time.Now().UTC())
			if err != nil {
				if rejectErr := c.publishJournalReject(ctx, record); rejectErr != nil {
					Metrics.incJournal("error")
					return fmt.Errorf("decode quarantine at %s/%d/%d: %w (DLQ handoff: %v)", record.Topic, record.Partition, record.Offset, err, rejectErr)
				}
				Metrics.incJournal("dlq")
				continue
			}
			token := journalToken(record.Topic, record.Partition, record.Offset)
			if err := c.sink.InsertQuarantine(ctx, row, token); err != nil {
				Metrics.incJournal("error")
				return fmt.Errorf("insert quarantine at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
			}
			Metrics.incJournal("inserted")
		}
		if err := commitJournalRecords(ctx, c.client, records); err != nil {
			Metrics.journalCommits.Add(1)
			return fmt.Errorf("commit quarantine journal offsets: %w", err)
		}
		c.client.AllowRebalance()
	}
}

func (c *JournalConsumer) publishJournalReject(ctx context.Context, record *kgo.Record) error {
	if record == nil {
		return errors.New("nil Kafka record")
	}
	body, err := journalRejectBody(record)
	if err != nil {
		return err
	}
	result := c.client.ProduceSync(ctx, &kgo.Record{
		Topic: TopicDLQ, Key: []byte("dlq\x1f" + journalRejectID(record)), Value: body,
	})
	return result.FirstErr()
}

func journalRejectBody(record *kgo.Record) ([]byte, error) {
	if record == nil {
		return nil, errors.New("nil Kafka record")
	}
	return json.Marshal(map[string]any{
		"contract_version": 1,
		"dead_letter_id":   journalRejectID(record),
		"quarantined_at":   time.Now().UTC().Format(time.RFC3339Nano),
		"stage":            "quarantine-journal",
		"reason_code":      "ERR_QUARANTINE_RECORD_INVALID",
		"diagnostic":       "invalid quarantine journal record",
		"provenance": map[string]any{
			"source_topic":     record.Topic,
			"source_partition": record.Partition,
			"source_offset":    record.Offset,
		},
	})
}

func DecodeJournalRecord(data []byte, now time.Time) (JournalRow, error) {
	record, err := ec.DecodeQuarantineRecordV1At(data, now)
	if err != nil {
		return JournalRow{}, err
	}
	row := JournalRow{
		TenantID: record.TenantID, SourceID: zeroUUID, BatchID: zeroUUID, JobID: zeroUUID, EventID: zeroUUID,
		ProcessingVersion: optionalUint32(record.ProcessingVersion), PipelineVersion: optionalUint32(record.PipelineVersion),
		OccurredAt: record.QuarantinedAt.UTC().Format("2006-01-02 15:04:05.000"), Stage: record.Stage,
		ErrorCode: record.ReasonCode, ErrorMessage: record.Diagnostic,
	}
	if record.Provenance.SourcePartition != nil && record.Provenance.SourceOffset != nil {
		row.RawRef = fmt.Sprintf("kafka:%s/%d/%d", record.Provenance.SourceTopic, *record.Provenance.SourcePartition, *record.Provenance.SourceOffset)
	}
	if record.SourceID != "" {
		row.SourceID = record.SourceID
	}
	if record.BatchID != "" {
		row.BatchID = record.BatchID
	}
	if record.JobID != "" {
		row.JobID = record.JobID
	}
	if record.EventID != "" {
		row.EventID = record.EventID
	}
	return row, nil
}

func optionalUint32(value *uint32) uint32 {
	if value == nil {
		return 0
	}
	return *value
}

func journalToken(topic string, partition int32, offset int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%d", topic, partition, offset)))
	return hex.EncodeToString(sum[:])
}

func journalRejectID(record *kgo.Record) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("astryon/quarantine-journal-dlq/v1\x00%s\x00%d\x00%d", record.Topic, record.Partition, record.Offset)))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x80
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func commitJournalRecords(ctx context.Context, client *kgo.Client, records []*kgo.Record) error {
	offsets := make(map[string]map[int32]kgo.EpochOffset)
	for _, record := range records {
		if offsets[record.Topic] == nil {
			offsets[record.Topic] = make(map[int32]kgo.EpochOffset)
		}
		current, exists := offsets[record.Topic][record.Partition]
		if !exists || record.Offset+1 > current.Offset {
			offsets[record.Topic][record.Partition] = kgo.EpochOffset{Epoch: -1, Offset: record.Offset + 1}
		}
	}
	var commitErr error
	client.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, response *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			commitErr = err
			return
		}
		if response == nil {
			commitErr = errors.New("empty Kafka offset commit response")
			return
		}
		for _, topic := range response.Topics {
			for _, partition := range topic.Partitions {
				if err := kerr.ErrorForCode(partition.ErrorCode); err != nil && commitErr == nil {
					commitErr = err
				}
			}
		}
	})
	return commitErr
}

func (s *ClickHouseStore) insertJournalRow(ctx context.Context, row JournalRow, token string) error {
	if s.URL == "" {
		return errors.New("ClickHouse URL is required")
	}
	table := s.Table
	if table == "" {
		table = "processing_errors_local"
	}
	if !journalIdentifier.MatchString(table) || !tenantUUID.MatchString(row.TenantID) {
		return errors.New("invalid quarantine journal table or tenant")
	}
	body, err := json.Marshal(row)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("INSERT INTO %s (tenant_id, source_id, batch_id, job_id, event_id, processing_version, pipeline_version, occurred_at, stage, error_code, error_message, raw_ref, sample_payload) FORMAT JSONEachRow", table)
	params := url.Values{
		"query": {query}, "insert_deduplicate": {"1"}, "insert_deduplication_token": {token},
		"date_time_input_format": {"best_effort"},
	}
	if s.Database != "" {
		params.Set("database", s.Database)
	}
	request, err := newClickHouseRequest(ctx, s, params, strings.NewReader(string(body)+"\n"))
	if err != nil {
		return err
	}
	return doClickHouseInsert(s, request)
}

func newClickHouseRequest(ctx context.Context, store *ClickHouseStore, params url.Values, body io.Reader) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(store.URL, "/")+"/?"+params.Encode(), body)
	if err != nil {
		return nil, err
	}
	if store.User != "" {
		request.Header.Set("X-ClickHouse-User", store.User)
		request.Header.Set("X-ClickHouse-Key", store.Password)
	}
	return request, nil
}

func doClickHouseInsert(store *ClickHouseStore, request *http.Request) error {
	client := store.HTTP
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode/100 == 2 {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
	return fmt.Errorf("clickhouse insert: HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
}
