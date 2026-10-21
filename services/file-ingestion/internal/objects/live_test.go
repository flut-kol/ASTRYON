package objects

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Live: FILEOBJ_PG_URL (non-superuser, migrations applied) + FILES3_ENDPOINT/BUCKET/ACCESS_KEY/SECRET_KEY.
func TestLiveVerifyAndSource(t *testing.T) {
	url, ep := os.Getenv("FILEOBJ_PG_URL"), os.Getenv("FILES3_ENDPOINT")
	if url == "" || ep == "" {
		t.Skip("FILEOBJ_PG_URL / FILES3_ENDPOINT not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	admin := func(fn func(tx pgx.Tx) error) {
		t.Helper()
		tx, _ := pool.Begin(ctx)
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `SELECT set_config('astryon.platform_admin','on',true)`); err != nil {
			t.Fatal(err)
		}
		if err := fn(tx); err != nil {
			t.Fatal(err)
		}
		tx.Commit(ctx)
	}
	var tenant, source string
	admin(func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO tenants (slug,name) VALUES ($1,'it') RETURNING id::text`, fmt.Sprintf("fileobj-it-%d", time.Now().UnixNano()%1_000_000_000)).Scan(&tenant); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `INSERT INTO sources (tenant_id,name,kind) VALUES ($1::uuid,'s','s3_files') RETURNING id::text`, tenant).Scan(&source)
	})
	t.Cleanup(func() {
		admin(func(tx pgx.Tx) error {
			tx.Exec(ctx, `DELETE FROM file_objects WHERE tenant_id=$1::uuid`, tenant)
			tx.Exec(ctx, `DELETE FROM sources WHERE tenant_id=$1::uuid`, tenant)
			_, err := tx.Exec(ctx, `DELETE FROM tenants WHERE id=$1::uuid`, tenant)
			return err
		})
	})
	bucket := os.Getenv("FILES3_BUCKET")
	c := &s3.Client{Endpoint: ep, Bucket: bucket, Creds: s3.Creds{AccessKey: os.Getenv("FILES3_ACCESS_KEY"), SecretKey: os.Getenv("FILES3_SECRET_KEY")}}
	st := PGStore{DB: jobs.PoolDB{Pool: pool}}

	upload := func(key string, body []byte) {
		id, urls, err := c.CreateMultipart(ctx, key, 1, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, urls[0], bytes.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("part: %v %v", err, resp)
		}
		resp.Body.Close()
		if err := c.CompleteMultipart(ctx, key, id, []s3.Part{{Number: 1, ETag: resp.Header.Get("ETag")}}); err != nil {
			t.Fatal(err)
		}
	}
	data := []byte("{\"a\":1}\n{\"a\":2}\n")
	good, bad := tenant+"/good.ndjson", tenant+"/bad.ndjson"
	upload(good, data)
	upload(bad, []byte("tampered after declaring the checksum"))
	gid, err := st.Register(ctx, tenant, source, File{Bucket: bucket, Key: good, SHA256: sum(data), Size: int64(len(data))})
	if err != nil {
		t.Fatal(err)
	}
	bid, err := st.Register(ctx, tenant, source, File{Bucket: bucket, Key: bad, SHA256: sum(data), Size: int64(len(data))})
	if err != nil {
		t.Fatal(err)
	}
	src := Source{Store: st, Objs: c, Bucket: bucket}
	if _, _, err := src.Open(ctx, jobs.Job{TenantID: tenant, FileObjectID: gid}, 0); !errors.Is(err, ErrNotVerified) {
		t.Fatalf("unverified object served: %v", err)
	}
	if err := Verify(ctx, st, c, tenant, gid); err != nil {
		t.Fatal(err)
	}
	if err := Verify(ctx, st, c, tenant, bid); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("tampered upload accepted: %v", err)
	}
	if _, err := c.Head(ctx, bad); !errors.Is(err, s3.ErrNotFound) {
		t.Fatalf("rejected object must be deleted: %v", err)
	}
	if _, err := st.Get(ctx, tenant, bid); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected row must disappear: %v", err)
	}
	rc, fp, err := src.Open(ctx, jobs.Job{TenantID: tenant, FileObjectID: gid}, 8)
	if err != nil || fp != sum(data) {
		t.Fatalf("open: %v %s", err, fp)
	}
	buf := make([]byte, 64)
	n, _ := rc.Read(buf)
	rc.Close()
	if string(buf[:n]) != "{\"a\":2}\n" {
		t.Fatalf("range read %q", buf[:n])
	}
}
