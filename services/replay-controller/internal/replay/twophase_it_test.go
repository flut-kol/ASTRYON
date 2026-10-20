package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Live two-phase audit tests. Gated on REPLAY_PG_URL (non-superuser NOBYPASSRLS role, migrations applied),
// REPLAY_PG_ADMIN_URL (superuser, ONLY used to create/drop the commit-failure trigger), REPLAY_KAFKA_IT_BROKERS and
// REPLAY_KAFKA_IT_CONTAINER (docker container with apache/kafka; topic audit.events.v1 must exist).

const forceFailSubject = "force-commit-fail"

func liveSink(t *testing.T) *KafkaAuditSink {
	t.Helper()
	brokers := os.Getenv("REPLAY_KAFKA_IT_BROKERS")
	if brokers == "" || os.Getenv("REPLAY_KAFKA_IT_CONTAINER") == "" {
		t.Skip("REPLAY_KAFKA_IT_BROKERS / REPLAY_KAFKA_IT_CONTAINER not set")
	}
	cfg, err := LoadKafkaConfig(func(k string) string { return map[string]string{"ASTRYON_KAFKA_BROKERS": brokers}[k] })
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewKafkaAuditSink(cfg, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

// readAudit returns the audit events (raw JSON objects) whose request_id equals rid, read back from the topic.
func readAudit(t *testing.T, rid string) []map[string]any {
	t.Helper()
	out, err := exec.Command("docker", "exec", os.Getenv("REPLAY_KAFKA_IT_CONTAINER"), "/opt/kafka/bin/kafka-console-consumer.sh",
		"--bootstrap-server", "localhost:9092", "--topic", TopicAudit, "--from-beginning", "--timeout-ms", "8000",
		"--property", "print.key=true", "--property", "key.separator=|").CombinedOutput()
	if err != nil && len(out) == 0 {
		t.Fatalf("console consumer: %v", err)
	}
	var res []map[string]any
	for _, l := range strings.Split(string(out), "\n") {
		_, val, ok := strings.Cut(l, "|{")
		if !ok {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte("{"+val), &m) == nil && m["request_id"] == rid {
			res = append(res, m)
		}
	}
	return res
}

func phases(ev []map[string]any) []string {
	var p []string
	for _, e := range ev {
		p = append(p, fmt.Sprint(e["phase"]))
	}
	return p
}

func TestLiveCommitFailureEmitsPreparedAndAborted(t *testing.T) {
	adminURL := os.Getenv("REPLAY_PG_ADMIN_URL")
	if adminURL == "" {
		t.Skip("REPLAY_PG_ADMIN_URL not set")
	}
	sink := liveSink(t)
	pool, tA, _, src, _ := pgFixture(t)
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close) // registered before the trigger cleanup, so it runs after it
	// Deferred constraint trigger: the INSERT succeeds, the failure only surfaces at COMMIT. Armed by the subject only.
	for _, q := range []string{
		`CREATE OR REPLACE FUNCTION zz_fail_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
		   IF NEW.switched_by = '` + forceFailSubject + `' THEN RAISE EXCEPTION 'forced commit failure' USING ERRCODE = '40001'; END IF; RETURN NULL; END $$`,
		`DROP TRIGGER IF EXISTS zz_fail_commit ON source_active_versions`,
		`CREATE CONSTRAINT TRIGGER zz_fail_commit AFTER INSERT ON source_active_versions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION zz_fail_commit()`,
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, `DROP TRIGGER IF EXISTS zz_fail_commit ON source_active_versions`)
		_, _ = admin.Exec(ctx, `DROP FUNCTION IF EXISTS zz_fail_commit()`)
		_, _ = admin.Exec(ctx, `DELETE FROM source_active_versions WHERE tenant_id = $1::uuid`, tA)
	})
	var r1 string
	if err := admin.QueryRow(ctx, `INSERT INTO replay_requests (tenant_id, source_id, target_processing_version, status) VALUES ($1::uuid,$2::uuid,1,'COMPLETED') RETURNING id::text`, tA, src).Scan(&r1); err != nil {
		t.Fatal(err)
	}
	count := func() (n int) {
		if err := admin.QueryRow(ctx, `SELECT count(*) FROM source_active_versions WHERE source_id = $1::uuid`, src).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	call := func(subject, rid string) int {
		auth := DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tA, Subject: subject, Scopes: []string{ScopeReplayWrite}}}
		h := (&Handler{Auth: auth, Planner: &Planner{}, Activator: &PGActivator{DB: PoolDB{pool}}, Audit: sink, AuditRequired: true}).Routes()
		r := httptest.NewRequest("POST", "/v1/replays/"+r1+"/activate", nil)
		r.Header.Set("Authorization", "Bearer tok")
		r.Header.Set("X-Request-Id", rid)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	rid := fmt.Sprintf("it-fail-%d", time.Now().UnixNano())
	if c := call(forceFailSubject, rid); c != 502 {
		t.Fatalf("status %d, want 502 (commit must fail)", c)
	}
	if n := count(); n != 0 {
		t.Fatalf("switch persisted despite failed commit: %d rows", n)
	}
	ev := readAudit(t, rid)
	if p := phases(ev); len(p) != 2 || p[0] != PhasePrepared || p[1] != PhaseAborted {
		t.Fatalf("want [prepared aborted], got %v\n%v", p, ev)
	}
	if ev[0]["event_id"] == ev[1]["event_id"] || ev[1]["outcome"] != OutcomeAborted || ev[0]["subject"] != forceFailSubject {
		t.Fatalf("%v", ev)
	}
	// control: a normal subject commits -> prepared + committed
	rid2 := fmt.Sprintf("it-ok-%d", time.Now().UnixNano())
	if c := call("alice", rid2); c != 200 || count() != 1 {
		t.Fatalf("control failed: %d rows=%d", c, count())
	}
	if p := phases(readAudit(t, rid2)); len(p) != 2 || p[0] != PhasePrepared || p[1] != PhaseCommitted {
		t.Fatalf("control phases %v", p)
	}
}

// Required start: audit failure -> 503, nothing published, row FAILED, version burned; live Kafka success -> prepared event.
func TestLiveStartAuditBeforeJob(t *testing.T) {
	sink := liveSink(t)
	pool, tA, _, src, _ := pgFixture(t)
	ctx := context.Background()
	alloc := &PGAllocator{DB: PoolDB{pool}}
	pub := &MemPublisher{}
	pl := &Planner{Alloc: alloc, Counter: FixedCounter{}, Publisher: pub}
	auth := DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tA, Subject: "alice", Scopes: []string{ScopeReplayWrite}}}
	body := `{"source_id":"` + src + `","from":"2026-10-01T00:00:00Z","to":"2026-10-02T00:00:00Z","pipeline_version":1}`
	do := func(h http.Handler, rid string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/v1/replays", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer tok")
		r.Header.Set("X-Request-Id", rid)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	statuses := func() (out []string) {
		tx, _ := pool.Begin(ctx)
		defer tx.Rollback(ctx)
		_, _ = tx.Exec(ctx, sqlSetTenant, tA)
		rows, err := tx.Query(ctx, `SELECT target_processing_version::text || ':' || status::text FROM replay_requests WHERE source_id = $1::uuid ORDER BY target_processing_version`, src)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var s string
			_ = rows.Scan(&s)
			out = append(out, s)
		}
		return out
	}
	bad := &kafkaFake{err: errors.New("broker down")}
	hBad := (&Handler{Auth: auth, Planner: pl, Audit: newKafkaAuditSink(bad, 0), AuditRequired: true}).Routes()
	if w := do(hBad, "it-start-bad"); w.Code != 503 || pub.Count() != 0 {
		t.Fatalf("%d jobs=%d %s", w.Code, pub.Count(), w.Body.String())
	}
	if s := statuses(); len(s) != 1 || s[0] != "1:FAILED" {
		t.Fatalf("rows %v", s)
	}
	hOK := (&Handler{Auth: auth, Planner: pl, Audit: sink, AuditRequired: true}).Routes()
	rid := fmt.Sprintf("it-start-%d", time.Now().UnixNano())
	if w := do(hOK, rid); w.Code != 202 || pub.Count() != 1 {
		t.Fatalf("%d jobs=%d %s", w.Code, pub.Count(), w.Body.String())
	}
	if s := statuses(); len(s) != 2 || s[1] != "2:REQUESTED" {
		t.Fatalf("rows %v (version 1 must stay burned)", s)
	}
	ev := readAudit(t, rid)
	if len(ev) != 1 || ev[0]["action"] != "replay.started" || ev[0]["phase"] != PhasePrepared || ev[0]["to_version"] != float64(2) {
		t.Fatalf("%v", ev)
	}
}

// auth.denied on live Kafka: keyed by truncated remote_ip, no token.
func TestLiveAuthDenied(t *testing.T) {
	sink := liveSink(t)
	h := &Handler{Auth: DevStaticAuthenticator{Token: "live-secret-token"}, Planner: &Planner{}, Audit: sink}
	rid := fmt.Sprintf("it-denied-%d", time.Now().UnixNano())
	r := httptest.NewRequest("POST", "/v1/replays", nil)
	r.RemoteAddr = "203.0.113.77:4444"
	r.Header.Set("Authorization", "Bearer wrong-live-token")
	r.Header.Set("X-Request-Id", rid)
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	var ev []map[string]any
	for i := 0; i < 5 && len(ev) == 0; i++ {
		ev = readAudit(t, rid)
	}
	if len(ev) != 1 || ev[0]["action"] != "auth.denied" || ev[0]["reason"] != "unauthenticated" || ev[0]["remote_ip"] != "203.0.113.0/24" {
		t.Fatalf("%v", ev)
	}
	if b, _ := json.Marshal(ev); strings.Contains(string(b), "wrong-live-token") || strings.Contains(string(b), "Bearer") {
		t.Fatal("token leaked")
	}
}
