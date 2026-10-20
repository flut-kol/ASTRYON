package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ec "github.com/astryon/event-contracts"
)

// seqOnly hides BatchPublisher so the server uses the sequential path.
type seqOnly struct{ p Publisher }

func (s seqOnly) Publish(ctx context.Context, t string, k, v []byte) error {
	return s.p.Publish(ctx, t, k, v)
}

func batchSrv(pub Publisher) *httptest.Server {
	cfg := Config{MaxBodyBytes: 4096, MaxBatchBytes: 1 << 20, MaxBatchEvents: 100}
	a := DevStaticAuthenticator{Token: token, Principal: Principal{TenantID: tenantA, Scopes: write}}
	return httptest.NewServer(NewServer(cfg, a, pub).Handler())
}

type batchOut struct {
	Accepted, Quarantined, Failed int
	Results                       []ItemResult
}

func runBatch(t *testing.T, pub Publisher, n int) (int, batchOut) {
	t.Helper()
	ts := batchSrv(pub)
	defer ts.Close()
	bad := `{"source_id":"x","schema_id":"s","schema_version":1,"payload_format":"json","payload":{}}`
	var evs []string
	for i := 0; i < n; i++ {
		if i%7 == 3 {
			evs = append(evs, bad)
		} else {
			evs = append(evs, evt(`,"event_id":"`+newUUID()+`"`))
		}
	}
	resp, body := do(t, ts.URL+"/v1/events/batch", `{"events":[`+strings.Join(evs, ",")+`]}`, map[string]string{"Idempotency-Key": "k1"})
	var out batchOut
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("%v %s", err, body)
	}
	return resp.StatusCode, out
}

func TestBatchPathMatchesSequential(t *testing.T) {
	failEvery := func() *FakePublisher {
		var n int32
		return &FakePublisher{Fail: func(topic string, _ []byte) error {
			if topic == ec.TopicRawEvents && atomic.AddInt32(&n, 1)%5 == 0 {
				return errors.New("no ack")
			}
			return nil
		}}
	}
	s1, o1 := runBatch(t, seqOnly{failEvery()}, 30)
	s2, o2 := runBatch(t, failEvery(), 30)
	if s1 != s2 || o1.Accepted != o2.Accepted || o1.Quarantined != o2.Quarantined || o1.Failed != o2.Failed || o1.Failed == 0 {
		t.Fatalf("mismatch seq=%d %+v batch=%d %+v", s1, o1, s2, o2)
	}
	for i := range o2.Results {
		if o2.Results[i].Index != i || o1.Results[i].Status != o2.Results[i].Status || o1.Results[i].Code != o2.Results[i].Code {
			t.Fatalf("result %d: %+v vs %+v", i, o1.Results[i], o2.Results[i])
		}
	}
}

// ackGate fails/acks by message value index and only returns after release.
type ackGate struct {
	release chan struct{}
	started chan struct{}
	failVal string
}

func (g *ackGate) Publish(ctx context.Context, t string, k, v []byte) error {
	return g.PublishBatch(ctx, []Message{{t, k, v}})[0]
}
func (g *ackGate) PublishBatch(ctx context.Context, msgs []Message) []error {
	close(g.started)
	select {
	case <-g.release:
	case <-ctx.Done():
		errs := make([]error, len(msgs))
		for i := range errs {
			errs[i] = ctx.Err()
		}
		return errs
	}
	errs := make([]error, len(msgs))
	for i, m := range msgs {
		if g.failVal != "" && strings.Contains(string(m.Value), g.failVal) {
			errs[i] = errors.New("no ack")
		}
	}
	return errs
}

func TestAcceptedOnlyAfterAck(t *testing.T) {
	g := &ackGate{release: make(chan struct{}), started: make(chan struct{})}
	ts := batchSrv(g)
	defer ts.Close()
	done := make(chan batchOut, 1)
	go func() {
		_, body := do(t, ts.URL+"/v1/events/batch", `{"events":[`+evt("")+`,`+evt("")+`]}`, nil)
		var o batchOut
		_ = json.Unmarshal(body, &o)
		done <- o
	}()
	<-g.started
	select {
	case <-done:
		t.Fatal("responded before broker ack")
	case <-time.After(100 * time.Millisecond):
	}
	close(g.release)
	if o := <-done; o.Accepted != 2 {
		t.Fatalf("%+v", o)
	}
}

func TestBatchPartialFailureMappingByIndex(t *testing.T) {
	g := &ackGate{release: make(chan struct{}), started: make(chan struct{}), failVal: `"a":2`}
	close(g.release)
	ts := batchSrv(g)
	defer ts.Close()
	ev2 := strings.Replace(evt(""), `"a":1`, `"a":2`, 1)
	resp, body := do(t, ts.URL+"/v1/events/batch", `{"events":[`+evt("")+`,`+ev2+`,`+evt("")+`]}`, nil)
	var out batchOut
	_ = json.Unmarshal(body, &out)
	if resp.StatusCode != 207 || out.Results[0].Status != "accepted" || out.Results[1].Status != "failed" || out.Results[1].Code != "PUBLISH_FAILED" || out.Results[2].Status != "accepted" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
}

func TestBatchContextCancelNotAccepted(t *testing.T) {
	g := &ackGate{release: make(chan struct{}), started: make(chan struct{})}
	s := NewServer(Config{}, nil, g)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-g.started; cancel() }()
	env := evt("")
	items := []item{parseItem([]byte(env)), parseItem([]byte(env))}
	res := s.processBatch(ctx, Principal{TenantID: tenantA}, items, "")
	for i, r := range res {
		if r.Status != "failed" || r.Index != i {
			t.Fatalf("%d: %+v", i, r)
		}
	}
}

func TestKafkaPublisherBatch(t *testing.T) {
	sp := &stubProducer{failTopic: "bad"}
	k := &KafkaPublisher{p: sp}
	errs := k.PublishBatch(context.Background(), []Message{{Topic: "ok"}, {Topic: "bad"}, {Topic: "ok"}})
	if len(errs) != 3 || errs[0] != nil || errs[1] == nil || errs[2] != nil {
		t.Fatalf("%v", errs)
	}
	if k.PublishBatch(context.Background(), nil) != nil {
		t.Fatal("empty batch")
	}
}

func TestLoadKafkaConfigBatchKnobs(t *testing.T) {
	env := func(m map[string]string) func(string) string {
		m["ASTRYON_KAFKA_BROKERS"] = "a:1"
		return func(k string) string { return m[k] }
	}
	c, err := LoadKafkaConfig(env(map[string]string{}))
	if err != nil || c.Linger != 2*time.Millisecond || c.BatchMaxBytes != 1<<20 {
		t.Fatalf("defaults %+v %v", c, err)
	}
	c, err = LoadKafkaConfig(env(map[string]string{"ASTRYON_KAFKA_LINGER": "0s", "ASTRYON_KAFKA_BATCH_MAX_BYTES": "65536"}))
	if err != nil || c.Linger != 0 || c.BatchMaxBytes != 65536 {
		t.Fatalf("%+v %v", c, err)
	}
	for _, kv := range [][2]string{{"ASTRYON_KAFKA_LINGER", "-1s"}, {"ASTRYON_KAFKA_LINGER", "x"}, {"ASTRYON_KAFKA_BATCH_MAX_BYTES", "0"}, {"ASTRYON_KAFKA_BATCH_MAX_BYTES", "abc"}} {
		if _, err := LoadKafkaConfig(env(map[string]string{kv[0]: kv[1]})); err == nil {
			t.Fatalf("%v should fail", kv)
		}
	}
}
