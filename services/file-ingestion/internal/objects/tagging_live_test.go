package objects

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Live test, skipped unless FILETAGS_LIVE=1 (needs docker). It starts a throwaway postgres:16, applies every migration in
// db/postgres/migrations in order (the way db/apply.sh does: one transaction per file), connects as a non-superuser so
// tenant RLS is enforced, and removes the container afterwards.
func TestLiveTagsLabelsQuarantineAndRLS(t *testing.T) {
	if os.Getenv("FILETAGS_LIVE") != "1" {
		t.Skip("FILETAGS_LIVE not set")
	}
	ctx := context.Background()
	out, err := exec.Command("docker", "run", "-d", "--rm", "-e", "POSTGRES_PASSWORD=pw", "-p", "127.0.0.1::5432", "postgres:16").Output()
	if err != nil {
		t.Fatalf("docker run: %v", err)
	}
	cid := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", cid).Run() })
	psql := func(db, sql string) {
		t.Helper()
		cmd := exec.Command("docker", "exec", "-i", cid, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-X", "-q")
		cmd.Stdin = strings.NewReader(sql)
		var eb bytes.Buffer
		cmd.Stderr = &eb
		if err := cmd.Run(); err != nil {
			t.Fatalf("psql: %v: %s", err, eb.String())
		}
	}
	for i := 0; i < 60; i++ {
		if exec.Command("docker", "exec", cid, "pg_isready", "-U", "postgres").Run() == nil {
			time.Sleep(time.Second) // the init server restarts once
			if exec.Command("docker", "exec", cid, "pg_isready", "-U", "postgres").Run() == nil {
				break
			}
		}
		time.Sleep(time.Second)
	}
	files, _ := filepath.Glob("../../../../db/postgres/migrations/*.up.sql")
	if len(files) < 12 {
		t.Fatalf("migrations not found: %v", files)
	}
	for _, f := range files { // glob is sorted
		b, _ := os.ReadFile(f)
		psql("postgres", "BEGIN;\n"+string(b)+"\nCOMMIT;\n")
	}
	psql("postgres", `CREATE ROLE app LOGIN PASSWORD 'pw'; GRANT ALL ON ALL TABLES IN SCHEMA public TO app; GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO app;
		GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO app;`)
	portOut, err := exec.Command("docker", "port", cid, "5432/tcp").Output()
	if err != nil {
		t.Fatal(err)
	}
	hostport := strings.TrimSpace(strings.Split(string(portOut), "\n")[0])
	hostport = hostport[strings.LastIndex(hostport, ":")+1:]
	pool, err := pgxpool.New(ctx, fmt.Sprintf("postgres://app:pw@127.0.0.1:%s/postgres", hostport))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var a, b, srcA string
	adm := func(sql string, dst *string, args ...any) {
		t.Helper()
		tx, _ := pool.Begin(ctx)
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `SELECT set_config('astryon.platform_admin','on',true)`); err != nil {
			t.Fatal(err)
		}
		if err := tx.QueryRow(ctx, sql, args...).Scan(dst); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	adm(`INSERT INTO tenants (slug,name) VALUES ('ta','a') RETURNING id::text`, &a)
	adm(`INSERT INTO tenants (slug,name) VALUES ('tb','b') RETURNING id::text`, &b)
	adm(`INSERT INTO sources (tenant_id,name,kind) VALUES ($1::uuid,'s','s3_files') RETURNING id::text`, &srcA, a)

	db := jobs.PoolDB{Pool: pool}
	fs := PGStore{DB: db}
	id, err := fs.Register(ctx, a, srcA, File{Bucket: "b", Key: "k1", SHA256: strings.Repeat("a", 64), Size: 10,
		SourceType: "report", Sensitivity: "internal", Tags: map[string]string{"team": "ops"}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := fs.Get(ctx, a, id)
	if err != nil || got.SourceType != "report" || got.Sensitivity != "internal" || got.Tags["team"] != "ops" {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err := fs.Get(ctx, b, id); !errors.Is(err, ErrNotFound) { // RLS: other tenant cannot see tags
		t.Fatalf("cross-tenant read: %v", err)
	}
	if err := fs.MarkCommitted(ctx, a, id, 10); err != nil {
		t.Fatal(err)
	}

	st := &jobs.Store{DB: db, WorkerID: "w1", Lease: time.Minute}
	if _, err := st.Enqueue(ctx, a, srcA, id); err != nil {
		t.Fatal(err)
	}
	j, err := st.Claim(ctx)
	if err != nil || j.SourceType != "report" || j.SourceID != srcA {
		t.Fatalf("claim: %+v %v", j, err)
	}
	d1 := classify.Labels{classify.PII: {Count: 2, Fields: []string{"email"}}}
	if err := st.SaveCheckpoint(ctx, j, []byte(`{"NextOffset":5}`), 2, 0, d1); err != nil {
		t.Fatal(err)
	}
	d2 := classify.Labels{classify.PII: {Count: 1, Fields: []string{"phone"}}}
	if err := st.SaveCheckpoint(ctx, j, []byte(`{"NextOffset":9}`), 1, 0, d2); err != nil {
		t.Fatal(err)
	}
	sum, err := st.Describe(ctx, a, id)
	if err != nil || sum.Labels[classify.PII].Count != 3 || len(sum.Labels[classify.PII].Fields) != 2 || sum.Status != "RUNNING" {
		t.Fatalf("%+v %v", sum, err)
	}
	if _, err := st.Describe(ctx, b, id); !errors.Is(err, jobs.ErrNoSummary) {
		t.Fatalf("cross-tenant describe: %v", err)
	}
	if err := st.Quarantine(ctx, j, classify.CodeCredentials, classify.Labels{classify.Credentials: {Count: 1, Fields: []string{"password"}}}); err != nil {
		t.Fatal(err)
	}
	sum, _ = st.Describe(ctx, a, id)
	got, _ = fs.Get(ctx, a, id)
	if sum.Status != "QUARANTINED" || sum.Error != classify.CodeCredentials || !sum.Labels.Has(classify.Credentials) || got.Sensitivity != "restricted" {
		t.Fatalf("%+v %+v", sum, got)
	}
	// a quarantined job is terminal and a stale fence cannot write
	if err := st.SaveCheckpoint(ctx, j, []byte(`{}`), 0, 0, d1); !errors.Is(err, jobs.ErrFenced) {
		t.Fatalf("write after quarantine: %v", err)
	}
	if _, err := st.Claim(ctx); !errors.Is(err, jobs.ErrNoJob) {
		t.Fatalf("quarantined job was claimable: %v", err)
	}
}
