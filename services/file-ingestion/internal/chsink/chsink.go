// Package chsink is the direct ClickHouse path for file jobs (DPP-035): valid NDJSON records are inserted into
// events_normalized and unparseable ones into processing_errors, without going through Kafka. Both inserts use
// deterministic dedup tokens and deterministic event ids, so a batch replayed after a crash is dropped by ClickHouse.
package chsink

import (
	"bytes"
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
	"strconv"
	"strings"
	"time"

	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/objects"
	"github.com/astryon/file-ingestion/internal/parse"
	"github.com/astryon/file-ingestion/internal/worker"
)

const nilUUID = "00000000-0000-0000-0000-000000000000"

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Mapping says how a raw JSON record becomes an events_normalized row.
type Mapping struct {
	EventType         string // required
	EventTimeField    string // top-level field with RFC 3339 text or Unix seconds/milliseconds; default: ingest time + quality flag
	EntityIDField     string // optional top-level field
	ProcessingVersion uint32 // default 1
}

type Config struct {
	URL, Database, User, Password string
	EventsTable, ErrorsTable      string // default events_normalized_distributed / processing_errors_local
	Mapping                       Mapping
	Files                         objects.Store // resolves the file object (bucket/key) of a job
	Client                        *http.Client
	Now                           func() time.Time
}

type Sink struct{ cfg Config }

func New(c Config) (*Sink, error) {
	if c.EventsTable == "" {
		c.EventsTable = "events_normalized_distributed"
	}
	if c.ErrorsTable == "" {
		c.ErrorsTable = "processing_errors_local"
	}
	if c.Mapping.ProcessingVersion == 0 {
		c.Mapping.ProcessingVersion = 1
	}
	if c.URL == "" || c.Mapping.EventType == "" || c.Files == nil || !identRe.MatchString(c.EventsTable) || !identRe.MatchString(c.ErrorsTable) ||
		(c.Database != "" && !identRe.MatchString(c.Database)) {
		return nil, errors.New("chsink: URL, Mapping.EventType, Files and valid table names are required")
	}
	if c.Client == nil {
		c.Client = &http.Client{Timeout: 60 * time.Second}
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return &Sink{c}, nil
}

// eventID derives a stable UUID (v5-shaped) from the record's dedup key (fingerprint + byte offset).
func eventID(dedupKey string) string {
	b, _ := hex.DecodeString(dedupKey)
	if len(b) < 16 {
		sum := sha256.Sum256([]byte(dedupKey))
		b = sum[:]
	}
	b = append([]byte{}, b[:16]...)
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

const chTime = "2006-01-02 15:04:05.000"

// Accept implements worker.Sink. Both inserts must succeed before it returns (the worker then advances the checkpoint).
func (s *Sink) Accept(ctx context.Context, j jobs.Job, valid, invalid []worker.Item) error {
	if len(valid) == 0 && len(invalid) == 0 {
		return nil
	}
	f, err := s.cfg.Files.Get(ctx, j.TenantID, j.FileObjectID)
	if err != nil {
		return err
	}
	if !f.Committed {
		return objects.ErrNotVerified
	}
	now := s.cfg.Now().UTC()
	if len(valid) > 0 {
		var body bytes.Buffer
		for _, it := range valid {
			line, err := s.eventRow(j, f, it, now)
			if err != nil {
				return worker.Permanent("mapping_failed")
			}
			body.Write(line)
			body.WriteByte('\n')
		}
		if err := s.insert(ctx, s.cfg.EventsTable, token("e", j, valid), &body); err != nil {
			return err
		}
	}
	if len(invalid) > 0 {
		var body bytes.Buffer
		for _, it := range invalid {
			line, _ := json.Marshal(map[string]any{
				"tenant_id": j.TenantID, "source_id": f.SourceID, "batch_id": f.ID, "job_id": j.ID, "event_id": eventID(it.DedupKey),
				"processing_version": s.cfg.Mapping.ProcessingVersion, "occurred_at": now.Format(chTime), "stage": "file_parse",
				"error_code": errorCode(it.Record.Err), "error_message": errorMessage(it.Record.Err), "raw_ref": rawRef(f, it.Record),
			})
			body.Write(line)
			body.WriteByte('\n')
		}
		if err := s.insert(ctx, s.cfg.ErrorsTable, token("x", j, invalid), &body); err != nil {
			return err
		}
	}
	return nil
}

func rawRef(f objects.File, r parse.Record) string {
	return "s3://" + f.Bucket + "/" + f.Key + "#" + strconv.FormatInt(r.Offset, 10)
}

// errorCode / errorMessage never echo record content (it may hold PII); only the failure class.
func errorCode(err error) string {
	if errors.Is(err, parse.ErrRecordTooLarge) {
		return "ERR_RECORD_TOO_LARGE"
	}
	if errors.Is(err, parse.ErrCSVMalformed) {
		return "ERR_CSV_MALFORMED"
	}
	return "ERR_JSON_MALFORMED"
}

func errorMessage(err error) string {
	if errors.Is(err, parse.ErrRecordTooLarge) {
		return "record exceeds the configured maximum size"
	}
	if errors.Is(err, parse.ErrCSVMalformed) {
		return "record is not valid CSV for the file header"
	}
	return "record is not valid JSON"
}

// token is deterministic for the same job, file and first/last record offsets and count: the worker re-cuts a
// replayed batch from the same checkpoint, so ClickHouse block deduplication drops it.
func token(kind string, j jobs.Job, items []worker.Item) string {
	first, last := items[0].Record, items[len(items)-1].Record
	sum := sha256.Sum256([]byte(fmt.Sprintf("file:%s:%s:%s:%d:%d:%d", kind, j.TenantID, j.FileObjectID, first.Offset, last.Offset, len(items))))
	return hex.EncodeToString(sum[:])
}

func (s *Sink) eventRow(j jobs.Job, f objects.File, it worker.Item, now time.Time) ([]byte, error) {
	m := s.cfg.Mapping
	var doc map[string]any
	_ = json.Unmarshal(it.Record.Raw, &doc) // a non-object JSON value (array, number) simply has no mapped fields
	flags := []string{}
	t, ok := parseTime(doc[m.EventTimeField])
	if !ok {
		t = now
		flags = append(flags, "event_time_defaulted")
	}
	entity := ""
	if m.EntityIDField != "" {
		switch v := doc[m.EntityIDField].(type) {
		case string:
			entity = v
		case float64:
			entity = strconv.FormatFloat(v, 'f', -1, 64)
		}
	}
	return json.Marshal(map[string]any{
		"tenant_id": j.TenantID, "source_id": f.SourceID, "event_id": eventID(it.DedupKey), "batch_id": f.ID,
		"processing_version": m.ProcessingVersion, "pipeline_id": nilUUID, "pipeline_version": 0, "event_type": m.EventType,
		"event_time": t.UTC().Format(chTime), "ingested_at": now.Format(chTime), "entity_id": entity, "attributes": map[string]string{},
		"payload": string(it.Record.Raw), "raw_ref": rawRef(f, it.Record), "quality_flags": flags,
	})
}

func parseTime(v any) (time.Time, bool) {
	switch x := v.(type) {
	case string:
		if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
			return t, true
		}
	case float64:
		if x > 0 && x < 1e11 { // seconds
			return time.Unix(int64(x), 0), true
		}
		if x >= 1e11 && x < 1e14 { // milliseconds
			return time.UnixMilli(int64(x)), true
		}
	}
	return time.Time{}, false
}

func (s *Sink) insert(ctx context.Context, table, tok string, body io.Reader) error {
	q := url.Values{}
	q.Set("query", fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", table))
	if s.cfg.Database != "" {
		q.Set("database", s.cfg.Database)
	}
	q.Set("insert_deduplicate", "1")
	q.Set("insert_deduplication_token", tok)
	q.Set("date_time_input_format", "best_effort")
	q.Set("input_format_skip_unknown_fields", "0")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.URL+"/?"+q.Encode(), body)
	if err != nil {
		return err
	}
	if s.cfg.User != "" {
		req.Header.Set("X-ClickHouse-User", s.cfg.User)
		if s.cfg.Password != "" {
			req.Header.Set("X-ClickHouse-Key", s.cfg.Password)
		}
	}
	resp, err := s.cfg.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("clickhouse insert into %s: HTTP %d: %s", table, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}
