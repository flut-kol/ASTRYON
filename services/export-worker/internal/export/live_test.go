package export

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
)

// Live test against the dev compose ClickHouse + S3 emulator. Skipped unless EXPORT_LIVE=1.
// Env: EXPORT_CH_URL, EXPORT_CH_USER, EXPORT_CH_PASSWORD, EXPORT_CH_DB, EXPORT_S3_ENDPOINT,
// EXPORT_S3_ACCESS_KEY, EXPORT_S3_SECRET_KEY (the s3mock accepts any credentials).
func TestLiveClickHouseAndS3(t *testing.T) {
	if os.Getenv("EXPORT_LIVE") != "1" {
		t.Skip("set EXPORT_LIVE=1 to run against dev ClickHouse + S3")
	}
	ch := ClickHouse{URL: os.Getenv("EXPORT_CH_URL"), Database: os.Getenv("EXPORT_CH_DB"),
		User: os.Getenv("EXPORT_CH_USER"), Password: os.Getenv("EXPORT_CH_PASSWORD"), Table: "events_normalized_local"}
	ctx := context.Background()
	const rows = 40000
	other := "33333333-3333-3333-3333-333333333333"
	seed := func(tenant string, n int) {
		q := fmt.Sprintf(`INSERT INTO events_normalized_local (tenant_id, source_id, event_id, batch_id, processing_version, pipeline_id, pipeline_version, event_type, event_time, entity_id, attributes, payload, raw_ref, quality_flags)
SELECT {t:UUID}, {t:UUID}, generateUUIDv4(), {t:UUID}, 1, {t:UUID}, 1, 'live.test', toDateTime64('2026-02-01 00:00:00', 3, 'UTC') + number, concat('ent,"', toString(number)), map('k', 'v'), repeat('x', 250), 'raw', ['q'] FROM numbers(%d)`, n)
		v := url.Values{"param_t": {tenant}}
		if ch.Database != "" {
			v.Set("database", ch.Database)
		}
		req, _ := http.NewRequestWithContext(ctx, "POST", ch.URL+"/?"+v.Encode(), strings.NewReader(q))
		req.Header.Set("X-ClickHouse-User", ch.User)
		req.Header.Set("X-ClickHouse-Key", ch.Password)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("seed: %s %s", resp.Status, b)
		}
	}
	seed(tenantA, rows)
	seed(other, 10)

	st := &S3Store{Endpoint: os.Getenv("EXPORT_S3_ENDPOINT"), Bucket: "export-live", Creds: Creds{os.Getenv("EXPORT_S3_ACCESS_KEY"), os.Getenv("EXPORT_S3_SECRET_KEY")}, PartSize: 5 << 20}
	if err := st.CreateBucket(ctx); err != nil {
		t.Fatal(err)
	}
	au := &MemoryAudit{}
	r := &Runner{CH: ch, Store: st, Presign: st, Audit: au, Quota: &MemoryQuota{MaxPerDay: 10, MaxConcurrent: 2}}
	pv := uint32(1)
	spec := goodSpec()
	spec.ProcessingVersion = &pv
	spec.From = spec.From.AddDate(0, 1, 0) // 2026-02-01
	spec.To = spec.From.AddDate(0, 0, 2)
	spec.MaxRows = rows

	for _, format := range []string{"ndjson", "csv"} {
		spec.Format = format
		id := "live-" + format + "-0001"
		res, err := r.Run(ctx, Principal{TenantID: tenantA}, id, spec)
		if err != nil {
			t.Fatalf("%s: %v", format, err)
		}
		if res.Rows != rows || res.Bytes < 5<<20 {
			t.Fatalf("%s: %+v (want multipart >5MiB)", format, res)
		}
		resp, err := http.Get(res.DownloadURL) // pre-signed, no auth header
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || int64(len(b)) != res.Bytes {
			t.Fatalf("%s: presigned GET %s len=%d want %d", format, resp.Status, len(b), res.Bytes)
		}
		if strings.Contains(string(b), other) {
			t.Fatal("other tenant's rows leaked")
		}
		wantLines := rows
		if format == "csv" {
			wantLines++ // header; quoted entity ids contain no newlines
		}
		if got := strings.Count(string(b), "\n"); got != wantLines {
			t.Fatalf("%s: %d lines want %d", format, got, wantLines)
		}
	}

	// Row cap overflow: aborted, object absent, audited.
	spec.Format, spec.MaxRows = "ndjson", 1000
	_, err := r.Run(ctx, Principal{TenantID: tenantA}, "live-cap-00001", spec)
	if !errors.Is(err, ErrRowCap) {
		t.Fatal(err)
	}
	if _, err := st.Get(ctx, ObjectKey(tenantA, "live-cap-00001", "ndjson")); err == nil {
		t.Fatal("partial object still present after cap abort")
	}
	// Server-side ClickHouse error mid-stream is not reproducible deterministically here; covered by fakes.
	// Wrong tenant sees nothing.
	spec.MaxRows = 100
	res, err := r.Run(ctx, Principal{TenantID: "44444444-4444-4444-4444-444444444444"}, "live-none-0001", spec)
	if err != nil || res.Rows != 0 {
		t.Fatalf("%v %+v", err, res)
	}
	t.Logf("audit: %+v", au.Records())
}
