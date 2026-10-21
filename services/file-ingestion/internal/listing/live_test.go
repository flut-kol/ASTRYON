package listing

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Live test, skipped unless FILELIST_LIVE=1 (needs docker). Throwaway postgres:16 with all migrations applied, queried as a
// non-superuser so tenant RLS is enforced; the container is removed afterwards.
func TestLiveListFiltersPagingAndRLS(t *testing.T) {
	if os.Getenv("FILELIST_LIVE") != "1" {
		t.Skip("FILELIST_LIVE not set")
	}
	ctx := context.Background()
	out, err := exec.Command("docker", "run", "-d", "--rm", "-e", "POSTGRES_PASSWORD=pw", "-p", "127.0.0.1::5432", "postgres:16").Output()
	if err != nil {
		t.Fatalf("docker run: %v", err)
	}
	cid := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", cid).Run() })
	psql := func(sql string) {
		t.Helper()
		cmd := exec.Command("docker", "exec", "-i", cid, "psql", "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-X", "-q")
		cmd.Stdin = strings.NewReader(sql)
		var eb bytes.Buffer
		cmd.Stderr = &eb
		if err := cmd.Run(); err != nil {
			t.Fatalf("psql: %v: %s", err, eb.String())
		}
	}
	for i := 0; i < 60; i++ {
		if exec.Command("docker", "exec", cid, "pg_isready", "-U", "postgres").Run() == nil {
			time.Sleep(time.Second)
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
	for _, f := range files {
		b, _ := os.ReadFile(f)
		psql("BEGIN;\n" + string(b) + "\nCOMMIT;\n")
	}
	psql(`CREATE ROLE app LOGIN PASSWORD 'pw'; GRANT ALL ON ALL TABLES IN SCHEMA public TO app; GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO app;
		GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO app;`)
	portOut, err := exec.Command("docker", "port", cid, "5432/tcp").Output()
	if err != nil {
		t.Fatal(err)
	}
	hp := strings.TrimSpace(strings.Split(string(portOut), "\n")[0])
	hp = hp[strings.LastIndex(hp, ":")+1:]
	pool, err := pgxpool.New(ctx, fmt.Sprintf("postgres://app:pw@127.0.0.1:%s/postgres", hp))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var a, b, srcA, srcB string
	adm := func(sql string, dst *string, args ...any) {
		t.Helper()
		tx, _ := pool.Begin(ctx)
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `SELECT set_config('astryon.platform_admin','on',true)`); err != nil {
			t.Fatal(err)
		}
		if dst == nil {
			if _, err := tx.Exec(ctx, sql, args...); err != nil {
				t.Fatal(err)
			}
		} else if err := tx.QueryRow(ctx, sql, args...).Scan(dst); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	adm(`INSERT INTO tenants (slug,name) VALUES ('ta','a') RETURNING id::text`, &a)
	adm(`INSERT INTO tenants (slug,name) VALUES ('tb','b') RETURNING id::text`, &b)
	adm(`INSERT INTO sources (tenant_id,name,kind) VALUES ($1::uuid,'s','s3_files') RETURNING id::text`, &srcA, a)
	adm(`INSERT INTO sources (tenant_id,name,kind) VALUES ($1::uuid,'s','s3_files') RETURNING id::text`, &srcB, b)

	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	ins := func(tenant, src, key, stype, sens, tags string, age int, status, labels string) string {
		var id string
		adm(`INSERT INTO file_objects (tenant_id,source_id,bucket,object_key,source_type,sensitivity,tags,created_at,committed_at)
			VALUES ($1::uuid,$2::uuid,'b',$3,$4,$5,$6::jsonb,$7,now()) RETURNING id::text`, &id, tenant, src, key, stype, sens, tags, base.Add(time.Duration(age)*time.Hour))
		if status != "" {
			adm(`INSERT INTO jobs (tenant_id,kind,status,file_object_id,content_labels,last_error) VALUES ($1::uuid,'process_file',$2::job_status,$3::uuid,$4::jsonb,'X')`,
				nil, tenant, status, id, labels)
		}
		return id
	}
	// ten files in tenant a: ages 0..9
	var ids []string
	for i := 0; i < 10; i++ {
		var stype, sens, tags, status, labels = "report", "internal", `{"team":"ops"}`, "SUCCEEDED", `{}`
		switch {
		case i == 3:
			stype, sens, status, labels = "geo", "restricted", "SUCCEEDED", `{"contains_geo":{"count":2,"fields":["lat"]}}`
		case i == 5:
			sens, tags, status, labels = "restricted", `{"team":"fin","env":"prod"}`, "QUARANTINED", `{"contains_credentials":{"count":1,"fields":["password"]}}`
		case i == 9:
			status = "" // no job yet
		}
		ids = append(ids, ins(a, srcA, fmt.Sprintf("k%d", i), stype, sens, tags, i, status, labels))
	}
	ins(b, srcB, "other", "report", "internal", `{"team":"ops"}`, 1, "QUARANTINED", `{"contains_credentials":{"count":9,"fields":["x"]}}`)

	st := PGStore{Pool: pool}
	ids5 := func(f Filter) []string {
		t.Helper()
		var got []string
		for {
			p, err := st.List(ctx, a, f)
			if err != nil {
				t.Fatal(err)
			}
			for _, it := range p.Items {
				got = append(got, it.FileObjectID)
			}
			if p.NextCursor == "" {
				return got
			}
			f.Cursor = p.NextCursor
		}
	}
	// keyset paging, newest first, 3 per page, no gaps or repeats, tenant b never visible
	f := Filter{Limit: 3}
	all := ids5(f)
	if len(all) != 10 {
		t.Fatalf("paging returned %d", len(all))
	}
	for i, id := range all {
		if id != ids[9-i] {
			t.Fatalf("order at %d", i)
		}
	}
	check := func(name string, f Filter, want ...int) {
		t.Helper()
		got := ids5(f)
		if len(got) != len(want) {
			t.Fatalf("%s: got %d want %v", name, len(got), want)
		}
		for i, w := range want {
			if got[i] != ids[w] {
				t.Fatalf("%s: item %d", name, i)
			}
		}
	}
	check("source_type", Filter{SourceType: "geo"}, 3)
	check("sensitivity", Filter{Sensitivity: "restricted"}, 5, 3)
	check("status quarantined", Filter{Status: "QUARANTINED"}, 5)
	check("label geo", Filter{Label: classify.Geo}, 3)
	check("label creds", Filter{Label: classify.Credentials}, 5)
	check("tag value", Filter{Tags: []Tag{{"team", "fin", true}}}, 5)
	check("tag key", Filter{Tags: []Tag{{Key: "env"}}}, 5)
	check("two tags", Filter{Tags: []Tag{{"team", "fin", true}, {"env", "prod", true}}}, 5)
	check("tag miss", Filter{Tags: []Tag{{"team", "nope", true}}})
	check("range", Filter{From: base.Add(2 * time.Hour), To: base.Add(5 * time.Hour)}, 4, 3, 2)
	check("combined", Filter{Sensitivity: "restricted", Status: "SUCCEEDED"}, 3)
	// no-job file appears with empty job info
	p, _ := st.List(ctx, a, Filter{Limit: 1})
	if p.Items[0].JobID != "" || p.Items[0].Tags["team"] != "ops" {
		t.Fatalf("%+v", p.Items[0])
	}
	// RLS: even asking for tenant a's rows as tenant b's session returns only b's
	pb, err := st.List(ctx, b, Filter{})
	if err != nil || len(pb.Items) != 1 || pb.Items[0].Labels[classify.Credentials].Count != 9 {
		t.Fatalf("tenant b: %+v %v", pb, err)
	}
}
