package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

// Live test against a real S3 API (MinIO), skipped unless FILES3_ENDPOINT is set:
// FILES3_ENDPOINT FILES3_BUCKET (must exist) FILES3_ACCESS_KEY FILES3_SECRET_KEY.
func TestLiveMultipartRoundTrip(t *testing.T) {
	ep := os.Getenv("FILES3_ENDPOINT")
	if ep == "" {
		t.Skip("FILES3_ENDPOINT not set")
	}
	c := &Client{Endpoint: ep, Bucket: os.Getenv("FILES3_BUCKET"), Creds: Creds{os.Getenv("FILES3_ACCESS_KEY"), os.Getenv("FILES3_SECRET_KEY")}}
	ctx := context.Background()
	key := "it/" + time.Now().Format("150405.000000") + "/file.ndjson"

	p1 := make([]byte, 5<<20) // S3 minimum for non-final parts
	_, _ = rand.Read(p1)
	p2 := []byte("tail\n")
	id, urls, err := c.CreateMultipart(ctx, key, 2, time.Minute)
	if err != nil || len(urls) != 2 {
		t.Fatalf("create: %v %v", err, urls)
	}
	var parts []Part
	for i, body := range [][]byte{p1, p2} {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, urls[i], bytes.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("part %d: %v %v", i+1, err, resp)
		}
		resp.Body.Close()
		parts = append(parts, Part{i + 1, resp.Header.Get("ETag")})
	}
	if err := c.CompleteMultipart(ctx, key, id, parts); err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte{}, p1...), p2...)
	if n, err := c.Head(ctx, key); err != nil || n != int64(len(want)) {
		t.Fatalf("head: %d %v", n, err)
	}
	off := int64(len(p1)) - 3
	rc, err := c.Get(ctx, key, off)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, want[off:]) {
		t.Fatalf("ranged read returned %d bytes, want %d", len(got), len(want[off:]))
	}
	if _, err := c.Head(ctx, key+"-missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing head: %v", err)
	}
	if _, err := c.Get(ctx, key+"-missing", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing get: %v", err)
	}
	// abort of an unfinished upload
	id2, _, err := c.CreateMultipart(ctx, key+"-b", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Abort(ctx, key+"-b", id2); err != nil {
		t.Fatal(err)
	}
}
