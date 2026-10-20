package gateway

// DPP-025: ingestion integration tests. They drive the full HTTP handler
// (authn -> validation -> quarantine -> publish) against an in-memory broker
// that can fail and recover, covering retry, duplicate submit, broker
// failover, malformed payloads and cross-tenant access.

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	ec "github.com/astryon/event-contracts"
)

// flakyBroker simulates a broker outage: while down, publishes are not acked.
type flakyBroker struct {
	FakePublisher
	down atomic.Bool
}

func newFlakyBroker() *flakyBroker {
	b := &flakyBroker{}
	b.Fail = func(string, []byte) error {
		if b.down.Load() {
			return errors.New("no leader for partition")
		}
		return nil
	}
	return b
}

func rawEnvs(t *testing.T, pub *FakePublisher) []ec.Envelope {
	t.Helper()
	var out []ec.Envelope
	for _, r := range pub.ByTopic(ec.TopicRawEvents) {
		var e ec.Envelope
		if err := json.Unmarshal(r.Value, &e); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

func TestIngestRetryAfterBrokerFailover(t *testing.T) {
	br := newFlakyBroker()
	ts, _ := newSrv(write, &br.FakePublisher)
	defer ts.Close()
	h := map[string]string{"Idempotency-Key": "failover-1"}

	br.down.Store(true)
	for i := 0; i < 3; i++ {
		resp, body := do(t, ts.URL+"/v1/events", evt(""), h)
		if resp.StatusCode != 503 || resp.Header.Get("Retry-After") == "" || strings.Contains(string(body), `"status":"accepted"`) {
			t.Fatalf("outage attempt %d: %d %s", i, resp.StatusCode, body)
		}
	}
	if n := len(br.Records()); n != 0 {
		t.Fatalf("unacked event leaked to broker: %d records", n)
	}

	br.down.Store(false) // failover completed
	resp, body := do(t, ts.URL+"/v1/events", evt(""), h)
	if resp.StatusCode != 202 {
		t.Fatalf("after recovery: %d %s", resp.StatusCode, body)
	}
	var res ItemResult
	_ = json.Unmarshal(body, &res)
	envs := rawEnvs(t, &br.FakePublisher)
	if len(envs) != 1 || envs[0].EventID != res.EventID {
		t.Fatalf("want exactly one event matching response id %q, got %+v", res.EventID, envs)
	}
}

func TestIngestBatchRetryAfterPartialOutage(t *testing.T) {
	br := newFlakyBroker()
	ts, _ := newSrv(write, &br.FakePublisher)
	defer ts.Close()
	h := map[string]string{"Idempotency-Key": "batch-failover"}
	body := `{"events":[` + evt("") + `,` + evt("") + `]}`

	// Broker dies after acking the first record of the batch.
	var n atomic.Int32
	br.Fail = func(string, []byte) error {
		if n.Add(1) > 1 {
			return errors.New("broker went away")
		}
		return nil
	}
	resp, out := do(t, ts.URL+"/v1/events/batch", body, h)
	if resp.StatusCode != 207 || !strings.Contains(string(out), `"failed":1`) {
		t.Fatalf("partial: %d %s", resp.StatusCode, out)
	}

	br.Fail = nil
	resp, out = do(t, ts.URL+"/v1/events/batch", body, h)
	if resp.StatusCode != 200 {
		t.Fatalf("retry: %d %s", resp.StatusCode, out)
	}
	// Item 0 is re-published (at-least-once) but must carry the same
	// event_id so downstream dedup collapses it; item 1 appears once.
	ids := map[string]int{}
	for _, e := range rawEnvs(t, &br.FakePublisher) {
		ids[e.EventID]++
	}
	if len(ids) != 2 {
		t.Fatalf("want 2 distinct event_ids across retries, got %v", ids)
	}
}

func TestIngestConcurrentDuplicateSubmit(t *testing.T) {
	ts, pub := newSrv(write, nil)
	defer ts.Close()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, _ := do(t, ts.URL+"/v1/events", evt(""), map[string]string{"Idempotency-Key": "dup"})
			if resp.StatusCode != 202 {
				t.Errorf("status %d", resp.StatusCode)
			}
		}()
	}
	wg.Wait()
	dedup := map[string]struct{}{}
	for _, e := range rawEnvs(t, pub) {
		dedup[e.DedupKey()] = struct{}{}
	}
	if len(dedup) != 1 {
		t.Fatalf("duplicate submits produced %d distinct dedup keys", len(dedup))
	}
}

func TestIngestMalformedPayloads(t *testing.T) {
	cases := map[string]string{
		"truncated json":     `{"source_id":"` + source + `","schema_id":`,
		"not json":           `<xml/>`,
		"json array":         `[1,2,3]`,
		"json null":          `null`,
		"scalar":             `42`,
		"unknown field":      evt(`,"is_admin":true`),
		"trailing data":      evt("") + ` {"x":1}`,
		"empty object":       `{}`,
		"wrong field type":   `{"source_id":123,"schema_id":"s","schema_version":1}`,
		"bad payload_format": strings.Replace(evt(""), `"json"`, `"yaml"`, 1),
		"invalid utf8":       "{\"source_id\":\"\xff\xfe\"}",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			ts, pub := newSrv(write, nil)
			defer ts.Close()
			resp, out := do(t, ts.URL+"/v1/events", body, nil)
			if resp.StatusCode != 422 {
				t.Fatalf("want 422, got %d %s", resp.StatusCode, out)
			}
			if len(pub.ByTopic(ec.TopicRawEvents)) != 0 {
				t.Fatal("malformed event reached raw topic")
			}
			if len(pub.ByTopic(ec.TopicQuarantine)) != 1 {
				t.Fatal("malformed event not quarantined")
			}
		})
	}
}

func TestIngestMalformedBatchEnvelope(t *testing.T) {
	ts, pub := newSrv(write, nil)
	defer ts.Close()
	for name, body := range map[string]string{
		"not json":     `nope`,
		"events wrong": `{"events":"x"}`,
		"empty":        `{"events":[]}`,
	} {
		resp, _ := do(t, ts.URL+"/v1/events/batch", body, nil)
		if resp.StatusCode != 400 {
			t.Fatalf("%s: want 400, got %d", name, resp.StatusCode)
		}
	}
	if len(pub.Records()) != 0 {
		t.Fatal("records published for rejected batches")
	}
}

func TestIngestQuarantineOutageIsNotAccepted(t *testing.T) {
	pub := &FakePublisher{Fail: func(topic string, _ []byte) error {
		if topic == ec.TopicQuarantine {
			return errors.New("quarantine topic unavailable")
		}
		return nil
	}}
	ts, _ := newSrv(write, pub)
	defer ts.Close()
	resp, _ := do(t, ts.URL+"/v1/events", `{"nope":1}`, nil)
	if resp.StatusCode != 503 {
		t.Fatalf("invalid event must not be silently dropped: got %d", resp.StatusCode)
	}
}

// twoTenantServer authenticates two static tokens as different tenants and
// funnels both into one broker.
type tokenMap map[string]Principal

func (m tokenMap) Authenticate(r *http.Request) (Principal, error) {
	tok, _ := bearer(r)
	if p, ok := m[tok]; ok {
		return p, nil
	}
	return Principal{}, ErrUnauthenticated
}

func TestIngestCrossTenantIsolation(t *testing.T) {
	pub := &FakePublisher{}
	auth := tokenMap{
		"tok-a": {TenantID: tenantA, Scopes: write},
		"tok-b": {TenantID: tenantB, Scopes: write},
	}
	cfg := Config{MaxBodyBytes: 4096, MaxBatchBytes: 16384, MaxBatchEvents: 3}
	ts := httptest.NewServer(NewServer(cfg, auth, pub).Handler())
	defer ts.Close()
	as := func(tok, url, body string) int {
		resp, _ := do(t, url, body, map[string]string{"Authorization": "Bearer " + tok, "X-Tenant-ID": tenantA})
		return resp.StatusCode
	}

	// Tenant B claiming tenant A in the body is refused, in both endpoints.
	if c := as("tok-b", ts.URL+"/v1/events", evt(`,"tenant_id":"`+tenantA+`"`)); c != 403 {
		t.Fatalf("single: %d", c)
	}
	if c := as("tok-b", ts.URL+"/v1/events/batch", `{"events":[`+evt(`,"tenant_id":"`+tenantA+`"`)+`]}`); c != 403 {
		t.Fatalf("batch: %d", c)
	}
	// Foreign tenant buried deep in a batch still rejects the whole request.
	if c := as("tok-a", ts.URL+"/v1/events/batch", `{"events":[`+evt("")+`,`+evt("")+`,`+evt(`,"tenant_id":"`+tenantB+`"`)+`]}`); c != 403 {
		t.Fatalf("batch deep: %d", c)
	}
	if len(pub.Records()) != 0 {
		t.Fatalf("cross-tenant requests published %d records", len(pub.Records()))
	}
	// Unknown token and X-Tenant-ID header alone grant nothing.
	if c := as("tok-evil", ts.URL+"/v1/events", evt("")); c != 401 {
		t.Fatalf("unknown token: %d", c)
	}

	// Legit traffic: every record (raw and quarantine) carries only the
	// caller's own tenant, regardless of the spoofed header.
	if c := as("tok-b", ts.URL+"/v1/events", evt("")); c != 202 {
		t.Fatalf("legit b: %d", c)
	}
	if c := as("tok-b", ts.URL+"/v1/events", `{"nope":1}`); c != 422 {
		t.Fatalf("quarantine b: %d", c)
	}
	for _, r := range pub.Records() {
		var m struct {
			TenantID string `json:"tenant_id"`
		}
		_ = json.Unmarshal(r.Value, &m)
		if m.TenantID != tenantB || !strings.HasPrefix(string(r.Key), tenantB) {
			t.Fatalf("record on %s leaked tenant %q key %q", r.Topic, m.TenantID, r.Key)
		}
	}
}
