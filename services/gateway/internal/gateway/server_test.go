package gateway

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	ec "github.com/astryon/event-contracts"
)

const (
	tenantA = "11111111-1111-4111-8111-111111111111"
	tenantB = "22222222-2222-4222-8222-222222222222"
	source  = "33333333-3333-4333-8333-333333333333"
	token   = "dev-token"
)

func newSrv(scopes []string, pub *FakePublisher) (*httptest.Server, *FakePublisher) {
	if pub == nil {
		pub = &FakePublisher{}
	}
	cfg := Config{MaxBodyBytes: 4096, MaxBatchBytes: 16384, MaxBatchEvents: 3}
	a := DevStaticAuthenticator{Token: token, Principal: Principal{TenantID: tenantA, Scopes: scopes}}
	return httptest.NewServer(NewServer(cfg, a, pub).Handler()), pub
}

func evt(extra string) string {
	t := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	return `{"source_id":"` + source + `","schema_id":"s","schema_version":1,"event_time":"` + t + `","payload_format":"json","payload":{"a":1}` + extra + `}`
}

func do(t *testing.T, url, body string, hdr map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest("POST", url, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b strings.Builder
	buf := make([]byte, 4096)
	for {
		n, e := resp.Body.Read(buf)
		b.Write(buf[:n])
		if e != nil {
			break
		}
	}
	return resp, []byte(b.String())
}

var write = []string{ScopeEventsWrite}

func TestHealthzAndRequestID(t *testing.T) {
	ts, _ := newSrv(write, nil)
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil || resp.StatusCode != 200 || resp.Header.Get("X-Request-ID") == "" {
		t.Fatalf("healthz: %v %v", err, resp)
	}
}

func TestHappyPath(t *testing.T) {
	ts, pub := newSrv(write, nil)
	defer ts.Close()
	resp, body := do(t, ts.URL+"/v1/events", evt(""), map[string]string{"Idempotency-Key": "k1", "X-Request-ID": "rid-1"})
	if resp.StatusCode != 202 {
		t.Fatalf("status %d %s", resp.StatusCode, body)
	}
	if resp.Header.Get("X-Request-ID") != "rid-1" {
		t.Fatal("request id not echoed")
	}
	recs := pub.ByTopic(ec.TopicRawEvents)
	if len(recs) != 1 {
		t.Fatalf("records %d", len(recs))
	}
	var env ec.Envelope
	_ = json.Unmarshal(recs[0].Value, &env)
	if env.TenantID != tenantA || env.TraceID != "rid-1" || env.Validate() != nil {
		t.Fatalf("bad envelope %+v", env)
	}
}

func TestUnauthorizedNoToken(t *testing.T) {
	ts, pub := newSrv(write, nil)
	defer ts.Close()
	req, _ := http.NewRequest("POST", ts.URL+"/v1/events", strings.NewReader(evt("")))
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != 401 || len(pub.Records()) != 0 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestTenantMismatchRejected(t *testing.T) {
	ts, pub := newSrv(write, nil)
	defer ts.Close()
	resp, body := do(t, ts.URL+"/v1/events", evt(`,"tenant_id":"`+tenantB+`"`), nil)
	var e apiError
	_ = json.Unmarshal(body, &e)
	if resp.StatusCode != 403 || e.Code != "TENANT_MISMATCH" || e.TraceID == "" || len(pub.Records()) != 0 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	// header is ignored; matching body tenant is fine
	resp, _ = do(t, ts.URL+"/v1/events", evt(`,"tenant_id":"`+tenantA+`"`), map[string]string{"X-Tenant-ID": tenantB})
	if resp.StatusCode != 202 {
		t.Fatalf("matching tenant: %d", resp.StatusCode)
	}
	var env ec.Envelope
	_ = json.Unmarshal(pub.ByTopic(ec.TopicRawEvents)[0].Value, &env)
	if env.TenantID != tenantA {
		t.Fatal("tenant not from principal")
	}
	// batch: any mismatching item rejects whole request
	resp, _ = do(t, ts.URL+"/v1/events/batch", `{"events":[`+evt("")+`,`+evt(`,"tenant_id":"`+tenantB+`"`)+`]}`, nil)
	if resp.StatusCode != 403 || len(pub.ByTopic(ec.TopicRawEvents)) != 1 {
		t.Fatalf("batch mismatch: %d", resp.StatusCode)
	}
}

func TestMissingScope(t *testing.T) {
	ts, pub := newSrv([]string{"astryon:events:read"}, nil)
	defer ts.Close()
	resp, _ := do(t, ts.URL+"/v1/events", evt(""), nil)
	if resp.StatusCode != 403 || len(pub.Records()) != 0 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestOversizedBody(t *testing.T) {
	ts, pub := newSrv(write, nil)
	defer ts.Close()
	resp, _ := do(t, ts.URL+"/v1/events", evt(`,"entity_key":"`+strings.Repeat("x", 5000)+`"`), nil)
	if resp.StatusCode != 413 || len(pub.Records()) != 0 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	resp, _ = do(t, ts.URL+"/v1/events/batch", `{"events":[`+evt("")+`,`+evt("")+`,`+evt("")+`,`+evt("")+`]}`, nil)
	if resp.StatusCode != 413 {
		t.Fatalf("batch too many: %d", resp.StatusCode)
	}
}

func TestInvalidEnvelopeQuarantined(t *testing.T) {
	ts, pub := newSrv(write, nil)
	defer ts.Close()
	resp, body := do(t, ts.URL+"/v1/events", `{"source_id":"bad","schema_id":"s","schema_version":1,"payload_format":"json","payload":{}}`, nil)
	if resp.StatusCode != 422 || len(pub.ByTopic(ec.TopicRawEvents)) != 0 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	q := pub.ByTopic(ec.TopicQuarantine)
	if len(q) != 1 || !strings.Contains(string(q[0].Value), "ERR_SOURCE_ID_INVALID") || !strings.Contains(string(q[0].Value), "ERR_EVENT_TIME_MISSING") {
		t.Fatalf("quarantine: %v", q)
	}
	// undecodable JSON
	resp, _ = do(t, ts.URL+"/v1/events", `{"nope":1}`, nil)
	if resp.StatusCode != 422 || !strings.Contains(string(pub.ByTopic(ec.TopicQuarantine)[1].Value), string(ErrEnvelopeMalformed)) {
		t.Fatalf("malformed: %d", resp.StatusCode)
	}
}

func TestBatchPartialFailure(t *testing.T) {
	ts, pub := newSrv(write, nil)
	defer ts.Close()
	bad := `{"source_id":"x","schema_id":"s","schema_version":1,"payload_format":"json","payload":{}}`
	resp, body := do(t, ts.URL+"/v1/events/batch", `{"events":[`+evt("")+`,`+bad+`,`+evt("")+`]}`, map[string]string{"Idempotency-Key": "b1"})
	if resp.StatusCode != 207 {
		t.Fatalf("status %d %s", resp.StatusCode, body)
	}
	var out struct {
		Accepted, Quarantined, Failed int
		Results                       []ItemResult
	}
	_ = json.Unmarshal(body, &out)
	if out.Accepted != 2 || out.Quarantined != 1 || out.Results[1].Status != "quarantined" || out.Results[0].EventID == out.Results[2].EventID {
		t.Fatalf("%s", body)
	}
	if len(pub.ByTopic(ec.TopicRawEvents)) != 2 || len(pub.ByTopic(ec.TopicQuarantine)) != 1 {
		t.Fatal("topic counts")
	}
}

func TestPublisherFailureNotAccepted(t *testing.T) {
	pub := &FakePublisher{Fail: func(string, []byte) error { return errors.New("broker down") }}
	ts, _ := newSrv(write, pub)
	defer ts.Close()
	resp, body := do(t, ts.URL+"/v1/events", evt(""), nil)
	if resp.StatusCode != 503 || strings.Contains(string(body), `"accepted"`) {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	resp, _ = do(t, ts.URL+"/v1/events/batch", `{"events":[`+evt("")+`]}`, nil)
	if resp.StatusCode != 503 {
		t.Fatalf("batch: %d", resp.StatusCode)
	}
	// partial publisher failure in batch -> 207 with failed item
	n := 0
	pub.Fail = func(string, []byte) error {
		n++
		if n == 2 {
			return errors.New("boom")
		}
		return nil
	}
	resp, body = do(t, ts.URL+"/v1/events/batch", `{"events":[`+evt("")+`,`+evt("")+`]}`, nil)
	if resp.StatusCode != 207 || !strings.Contains(string(body), `"failed":1`) {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
}

func TestDuplicateSubmitStableEventID(t *testing.T) {
	ts, pub := newSrv(write, nil)
	defer ts.Close()
	h := map[string]string{"Idempotency-Key": "same-key"}
	do(t, ts.URL+"/v1/events", evt(""), h)
	do(t, ts.URL+"/v1/events", evt(""), h)
	do(t, ts.URL+"/v1/events", evt(""), map[string]string{"Idempotency-Key": "other"})
	recs := pub.ByTopic(ec.TopicRawEvents)
	var e [3]ec.Envelope
	for i := range e {
		_ = json.Unmarshal(recs[i].Value, &e[i])
	}
	if e[0].EventID != e[1].EventID || e[0].DedupKey() != e[1].DedupKey() || string(recs[0].Key) != string(recs[1].Key) {
		t.Fatal("retry minted a new event_id")
	}
	if e[0].EventID == e[2].EventID {
		t.Fatal("different keys collided")
	}
}

func TestLoadConfigDevAuthNeedsSecrets(t *testing.T) {
	env := map[string]string{"ASTRYON_DEV_AUTH": "1"}
	if _, err := LoadConfig(func(k string) string { return env[k] }); err == nil {
		t.Fatal("expected error")
	}
}
