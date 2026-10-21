// Package e2e holds the env-gated end-to-end test of file ingestion: HTTP upload API -> MinIO -> PostgreSQL job ->
// worker -> ClickHouse, using throwaway docker containers on free ports (all removed afterwards).
package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/astryon/file-ingestion/internal/api"
	"github.com/astryon/file-ingestion/internal/chsink"
	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/objects"
	"github.com/astryon/file-ingestion/internal/s3"
	"github.com/astryon/file-ingestion/internal/upload"
	"github.com/astryon/file-ingestion/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	minioUser = "e2eaccess"
	minioPass = "e2esecret-0123456789"
	bucket    = "files"
)

type staticAuth struct{ tenant string }

func (a staticAuth) Authenticate(*http.Request) (api.Principal, error) {
	return api.Principal{TenantID: a.tenant, Scopes: []string{api.ScopeUploadsWrite}}, nil
}

type parquetRow struct {
	ID    string `parquet:"id"`
	TS    string `parquet:"ts"`
	Value int64  `parquet:"value"`
}

func parquetFile(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[parquetRow](&buf, parquet.MaxRowsPerRowGroup(2))
	rows := []parquetRow{
		{ID: "p1", TS: "2026-10-01T00:00:03Z", Value: 11},
		{ID: "p2", TS: "2026-10-01T00:00:04Z", Value: 12},
		{ID: "p3", TS: "2026-10-01T00:00:05Z", Value: 13},
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// start runs a detached throwaway container publishing one container port on a free loopback port.
func start(t *testing.T, port string, args ...string) (cid, hostport string) {
	t.Helper()
	a := append([]string{"run", "-d", "--rm", "-p", "127.0.0.1::" + port}, args...)
	// A first run may pull the image and write progress to stderr. docker() intentionally captures stderr for
	// diagnostics, so select the final line containing the detached container id.
	cid = lastLine(docker(t, a...))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", cid).Run() })
	p := docker(t, "port", cid, port+"/tcp")
	p = strings.Split(p, "\n")[0]
	return cid, p[strings.LastIndex(p, ":")+1:]
}

func wait(t *testing.T, what string, f func() bool) {
	t.Helper()
	for i := 0; i < 90; i++ {
		if f() {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("%s not ready", what)
}

func migrations(t *testing.T, ext string) []string {
	files, _ := filepath.Glob("../../../../db/" + ext + "/migrations/*.up.sql")
	if len(files) == 0 {
		t.Fatalf("no %s migrations", ext)
	}
	return files
}

func TestE2EUploadToClickHouse(t *testing.T) {
	if os.Getenv("FILEINGEST_E2E") != "1" {
		t.Skip("FILEINGEST_E2E not set (needs docker: postgres:16, clickhouse-server:24.8, minio)")
	}
	ctx := context.Background()

	// PostgreSQL with every migration, non-superuser app role (RLS enforced).
	pgID, pgPort := start(t, "5432", "-e", "POSTGRES_PASSWORD=pw", "postgres:16")
	psql := func(sql string) string {
		t.Helper()
		cmd := exec.Command("docker", "exec", "-i", pgID, "psql", "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-X", "-q", "-At")
		cmd.Stdin = strings.NewReader(sql)
		var ob, eb bytes.Buffer
		cmd.Stdout, cmd.Stderr = &ob, &eb
		if err := cmd.Run(); err != nil {
			t.Fatalf("psql: %v: %s", err, eb.String())
		}
		return ob.String()
	}
	// ClickHouse and MinIO start while PG initialises.
	chID, chPort := start(t, "8123", "-e", "CLICKHOUSE_USER=u", "-e", "CLICKHOUSE_PASSWORD=pw", "clickhouse/clickhouse-server:24.8")
	s3ID, s3Port := start(t, "9000", "-e", "MINIO_ROOT_USER="+minioUser, "-e", "MINIO_ROOT_PASSWORD="+minioPass, "-e", "MINIO_DEFAULT_BUCKETS="+bucket, "bitnamilegacy/minio:latest")

	wait(t, "postgres", func() bool {
		if exec.Command("docker", "exec", pgID, "pg_isready", "-U", "postgres").Run() != nil {
			return false
		}
		time.Sleep(time.Second) // the init server restarts once
		return exec.Command("docker", "exec", pgID, "pg_isready", "-U", "postgres").Run() == nil
	})
	for _, f := range migrations(t, "postgres") {
		b, _ := os.ReadFile(f)
		psql("BEGIN;\n" + string(b) + "\nCOMMIT;\n")
	}
	psql(`CREATE ROLE app LOGIN PASSWORD 'pw'; GRANT ALL ON ALL TABLES IN SCHEMA public TO app; GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO app;
		GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO app;`)
	tenant := strings.TrimSpace(psql(`SELECT set_config('astryon.platform_admin','on',false); INSERT INTO tenants (slug,name) VALUES ('e2e','e2e') RETURNING id::text`))
	tenant = lastLine(tenant)
	source := lastLine(psql(`SELECT set_config('astryon.platform_admin','on',false); INSERT INTO sources (tenant_id,name,kind) VALUES ('` + tenant + `','s','s3_files') RETURNING id::text`))

	chq := func(sql string, stdin string) string {
		t.Helper()
		cmd := exec.Command("docker", "exec", "-i", chID, "clickhouse-client", "-u", "u", "--password", "pw", "--multiquery", "--query", sql)
		cmd.Stdin = strings.NewReader(stdin)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("clickhouse-client: %v: %s", err, out)
		}
		return strings.TrimSpace(string(out))
	}
	wait(t, "clickhouse", func() bool {
		return exec.Command("docker", "exec", chID, "clickhouse-client", "-u", "u", "--password", "pw", "-q", "SELECT 1").Run() == nil
	})
	for _, f := range migrations(t, "clickhouse")[:2] { // events_normalized + processing_errors (single node, local tables)
		b, _ := os.ReadFile(f)
		sql := strings.NewReplacer("__ON_CLUSTER__", "", "__RMT__", "MergeTree()", "__CLUSTER__", "default").Replace(string(b))
		cmd := exec.Command("docker", "exec", "-i", chID, "clickhouse-client", "-u", "u", "--password", "pw", "--multiquery")
		cmd.Stdin = strings.NewReader(sql)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("ch migration %s: %v: %s", f, err, out)
		}
	}

	s3c := &s3.Client{Endpoint: "http://127.0.0.1:" + s3Port, Bucket: bucket, Region: "us-east-1", Creds: s3.Creds{AccessKey: minioUser, SecretKey: minioPass}}
	wait(t, "minio bucket", func() bool {
		return exec.Command("docker", "exec", s3ID, "sh", "-c", "mc alias set l http://127.0.0.1:9000 "+minioUser+" "+minioPass+" && (mc mb --ignore-existing l/"+bucket+")").Run() == nil
	})
	// The Bitnami image briefly starts MinIO for setup, then replaces that process with the final server. `mc mb`
	// can succeed against the setup process; wait past that hand-off so multipart PUTs do not race a socket reset.
	time.Sleep(6 * time.Second)

	pool, err := pgxpool.New(ctx, "postgres://app:pw@127.0.0.1:"+pgPort+"/postgres")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	db := jobs.PoolDB{Pool: pool}
	store := &jobs.Store{DB: db, WorkerID: "e2e", Lease: time.Minute}
	files := objects.PGStore{DB: db}

	srv := &api.Server{Auth: staticAuth{tenant}, Bucket: bucket, Files: files, Objs: s3c, Multipart: s3c, Jobs: store, Insights: store,
		Uploads: upload.Service{Store: api.MultipartStarter{C: s3c}, Now: time.Now, MaxBytes: 1 << 20, MaxParts: 4,
			ContentTypes: map[string]bool{"text/csv": true, "application/x-ndjson": true, worker.ParquetContentType: true}}}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	sink, err := chsink.New(chsink.Config{URL: "http://127.0.0.1:" + chPort, User: "u", Password: "pw", EventsTable: "events_normalized_local",
		Files: files, Mapping: chsink.Mapping{EventType: "file.row", EventTimeField: "ts", EntityIDField: "id"}})
	if err != nil {
		t.Fatal(err)
	}
	lineage := &jobs.MemLineage{}
	w := worker.New(worker.Config{Store: store, Source: objects.Source{Store: files, Objs: s3c, Bucket: bucket}, Sink: sink,
		BatchSize: 2, Heartbeat: 10 * time.Second, Poll: 100 * time.Millisecond, Lineage: lineage})

	upl := func(name, ctype, stype string, body []byte) string {
		t.Helper()
		sum := sha256.Sum256(body)
		req, _ := json.Marshal(map[string]any{"source_id": source, "filename": name, "content_type": ctype, "sha256": hex.EncodeToString(sum[:]),
			"size_bytes": len(body), "parts": 1, "source_type": stype})
		var cr struct {
			FileObjectID string   `json:"file_object_id"`
			UploadID     string   `json:"upload_id"`
			PartURLs     []string `json:"part_urls"`
		}
		doJSON(t, ts.URL+"/v1/files/uploads", req, 201, &cr)
		pr, _ := http.NewRequest(http.MethodPut, cr.PartURLs[0], bytes.NewReader(body))
		resp, err := http.DefaultClient.Do(pr)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("part PUT: %v %v", err, resp)
		}
		resp.Body.Close()
		comp, _ := json.Marshal(map[string]any{"upload_id": cr.UploadID, "parts": []map[string]any{{"number": 1, "etag": resp.Header.Get("ETag")}}})
		doJSON(t, ts.URL+"/v1/files/uploads/"+cr.FileObjectID+"/complete", comp, 200, nil)
		return cr.FileObjectID
	}

	csvOK := upl("ok.csv", "text/csv", "report", []byte("id,ts,value\na,2026-10-01T00:00:00Z,1\nb,2026-10-01T00:00:01Z,2\nc,2026-10-01T00:00:02Z,3\n"))
	cred := upl("cred.csv", "text/csv", "report", []byte("id,ts,password\nx,2026-10-01T00:00:00Z,hunter2-SuperSecretValue\ny,2026-10-01T00:00:01Z,another-secret-9\n"))
	geo := upl("geo.csv", "text/csv", "geo", []byte("id,ts,lat,lon\ng1,2026-10-01T00:00:00Z,50.45,30.52\ng2,2026-10-01T00:00:01Z,48.85,2.35\n"))
	bad := upl("bad.csv", "text/csv", "report", []byte("id,ts,value\nm1,2026-10-01T00:00:00Z,1\nm2,2026-10-01T00:00:01Z\nm3,2026-10-01T00:00:02Z,3\n"))
	parquetBody := parquetFile(t)
	parquetID := upl("rows.parquet", worker.ParquetContentType, "report", parquetBody)

	// The completed, checksum-verified source must remain readable from MinIO before the worker runs.
	pf, err := files.Get(ctx, tenant, parquetID)
	parquetSum := sha256.Sum256(parquetBody)
	if err != nil || !pf.Committed || !strings.EqualFold(pf.SHA256, hex.EncodeToString(parquetSum[:])) {
		t.Fatalf("parquet file object: %+v err=%v", pf, err)
	}
	if n, err := s3c.Head(ctx, pf.Key); err != nil || n != int64(len(parquetBody)) {
		t.Fatalf("parquet MinIO object: size=%d err=%v", n, err)
	}

	for i := 0; i < 10; i++ {
		ran, err := w.RunOnce(ctx)
		if err != nil {
			t.Logf("RunOnce: %v", err)
		}
		if !ran {
			break
		}
	}

	sum := func(id string) jobs.Summary {
		t.Helper()
		s, err := store.Describe(ctx, tenant, id)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	count := func(table, id string) string {
		return chq(fmt.Sprintf("SELECT count() FROM %s WHERE tenant_id='%s' AND batch_id='%s'", table, tenant, id), "")
	}

	if s := sum(csvOK); s.Status != "SUCCEEDED" || count("events_normalized_local", csvOK) != "3" || count("processing_errors_local", csvOK) != "0" {
		t.Errorf("csv: %+v events=%s", s, count("events_normalized_local", csvOK))
	}
	if got := chq(fmt.Sprintf("SELECT entity_id FROM events_normalized_local WHERE batch_id='%s' ORDER BY entity_id", csvOK), ""); got != "a\nb\nc" {
		t.Errorf("csv entity ids = %q", got)
	}

	s := sum(cred)
	if s.Status != "QUARANTINED" || !s.Labels.Has(classify.Credentials) {
		t.Errorf("credentials job: %+v", s)
	}
	if n := count("events_normalized_local", cred) + count("processing_errors_local", cred); n != "00" {
		t.Errorf("credentials file reached ClickHouse: %s", n)
	}
	dump := psql(`SELECT set_config('astryon.platform_admin','on',false); SELECT coalesce(string_agg(j::text, ' '), '') FROM jobs j; SELECT coalesce(string_agg(f::text, ' '), '') FROM file_objects f;`)
	for _, secret := range []string{"hunter2", "another-secret"} {
		if strings.Contains(dump, secret) {
			t.Errorf("credential value %q persisted in PostgreSQL", secret)
		}
	}
	if strings.Contains(fmt.Sprint(s.Labels), "hunter2") {
		t.Errorf("value in labels: %v", s.Labels)
	}

	if s := sum(geo); s.Status != "SUCCEEDED" || !s.Labels.Has(classify.Geo) || count("events_normalized_local", geo) != "2" {
		t.Errorf("geo: %+v events=%s", s, count("events_normalized_local", geo))
	}

	if s := sum(bad); s.Status != "SUCCEEDED" || count("events_normalized_local", bad) != "2" || count("processing_errors_local", bad) != "1" {
		t.Errorf("malformed: %+v events=%s errors=%s", s, count("events_normalized_local", bad), count("processing_errors_local", bad))
	}
	if code := chq(fmt.Sprintf("SELECT error_code, sample_payload FROM processing_errors_local WHERE batch_id='%s'", bad), ""); !strings.HasPrefix(code, "ERR_CSV_MALFORMED") {
		t.Errorf("error row = %q", code)
	}

	if s := sum(parquetID); s.Status != "SUCCEEDED" || count("events_normalized_local", parquetID) != "3" || count("processing_errors_local", parquetID) != "0" {
		t.Errorf("parquet: %+v events=%s errors=%s", s, count("events_normalized_local", parquetID), count("processing_errors_local", parquetID))
	}
	if got := chq(fmt.Sprintf("SELECT entity_id FROM events_normalized_local WHERE batch_id='%s' ORDER BY entity_id", parquetID), ""); got != "p1\np2\np3" {
		t.Errorf("parquet entity ids = %q", got)
	}
	pgState := strings.TrimSpace(psql(`SELECT set_config('astryon.platform_admin','on',false); SELECT status::text || '|' || events_processed::text || '|' || events_failed::text || '|' || COALESCE(payload->'checkpoint'->>'NextGroup','') FROM jobs WHERE file_object_id='` + parquetID + `'::uuid`))
	if !strings.HasSuffix(pgState, "SUCCEEDED|3|0|2") {
		t.Errorf("parquet PG state/checkpoint = %q", pgState)
	}

	// Durable lineage has no PostgreSQL repository/schema yet. Verify the worker still emits the supported
	// in-process batch lineage contract, and keep this assertion explicit until a PG recorder is implemented.
	var parquetBatches, parquetRows int
	for _, b := range lineage.All() {
		if b.SourceFileID != parquetID {
			continue
		}
		parquetBatches++
		parquetRows += b.Valid
		if b.Destination != jobs.ClickHouseBulk || b.SourceChecksum != strings.ToLower(pf.SHA256) || b.JobID == "" || b.Validate() != nil {
			t.Errorf("parquet lineage: %+v", b)
		}
	}
	if parquetBatches != 2 || parquetRows != 3 {
		t.Errorf("parquet lineage batches=%d rows=%d", parquetBatches, parquetRows)
	}
}

func lastLine(s string) string {
	l := strings.Split(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(l[len(l)-1])
}

func doJSON(t *testing.T, url string, body []byte, want int, dst any) {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		t.Fatalf("POST %s = %d: %s", url, resp.StatusCode, b)
	}
	if dst != nil {
		if err := json.Unmarshal(b, dst); err != nil {
			t.Fatal(err)
		}
	}
}
