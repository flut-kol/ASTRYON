package gateway

import (
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	ec "github.com/astryon/event-contracts"
)

// DPP-025: client retry after a broker outage must not mint a new event_id
// and must not leave a record from the failed attempt.
func TestRetryAfterBrokerOutageKeepsEventID(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	pub := &FakePublisher{Fail: func(string, []byte) error {
		if down.Load() {
			return errors.New("broker unavailable")
		}
		return nil
	}}
	ts, _ := newSrv(write, pub)
	defer ts.Close()
	h := map[string]string{"Idempotency-Key": "retry-1"}

	resp, _ := do(t, ts.URL+"/v1/events", evt(""), h)
	if resp.StatusCode != 503 || len(pub.ByTopic(ec.TopicRawEvents)) != 0 {
		t.Fatalf("outage: status %d, records %d", resp.StatusCode, len(pub.ByTopic(ec.TopicRawEvents)))
	}
	down.Store(false)
	resp, _ = do(t, ts.URL+"/v1/events", evt(""), h)
	if resp.StatusCode/100 != 2 {
		t.Fatalf("retry: status %d", resp.StatusCode)
	}
	// second retry of an already-acked submit keeps the same identity
	do(t, ts.URL+"/v1/events", evt(""), h)
	recs := pub.ByTopic(ec.TopicRawEvents)
	if len(recs) != 2 {
		t.Fatalf("want 2 records, got %d", len(recs))
	}
	var a, b ec.Envelope
	_ = json.Unmarshal(recs[0].Value, &a)
	_ = json.Unmarshal(recs[1].Value, &b)
	if a.EventID != b.EventID || a.DedupKey() != b.DedupKey() {
		t.Fatal("retry changed event identity")
	}
}

func TestMalformedPayloadsNeverReachRawTopic(t *testing.T) {
	ts, pub := newSrv(write, nil)
	defer ts.Close()
	for name, body := range map[string]string{
		"not json":     `{"source_id":`,
		"empty":        ``,
		"array":        `[1,2,3]`,
		"empty batch":  `{"events":[]}`,
		"batch string": `{"events":"x"}`,
	} {
		path := "/v1/events"
		if strings.HasPrefix(name, "batch") || name == "empty batch" {
			path = "/v1/events/batch"
		}
		resp, _ := do(t, ts.URL+path, body, nil)
		if resp.StatusCode < 400 || resp.StatusCode >= 500 {
			t.Errorf("%s: status %d, want 4xx", name, resp.StatusCode)
		}
	}
	if n := len(pub.ByTopic(ec.TopicRawEvents)); n != 0 {
		t.Fatalf("raw topic got %d records from malformed input", n)
	}
}

func TestBatchForeignTenantItemRejectedOthersAccepted(t *testing.T) {
	ts, pub := newSrv(write, nil)
	defer ts.Close()
	foreign := evt(`,"tenant_id":"` + tenantB + `"`)
	resp, body := do(t, ts.URL+"/v1/events/batch", `{"events":[`+evt("")+`,`+foreign+`]}`, nil)
	for _, r := range pub.Records() {
		if strings.Contains(string(r.Value), tenantB) {
			t.Fatalf("foreign tenant published: %d %s", resp.StatusCode, body)
		}
	}
}
