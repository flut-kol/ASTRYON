package export

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultPresignTTL = 15 * time.Minute
	MaxPresignTTL     = time.Hour
	MinPartSize       = 5 << 20
)

var ErrBadTTL = errors.New("presign ttl must be 1s..1h")

// S3Store is a path-style S3-compatible store over plain HTTP(S) with hand-written SigV4.
// Objects smaller than PartSize use a single PutObject; larger streams use multipart upload with at most
// one part buffered in memory, aborting the upload on any error.
type S3Store struct {
	Endpoint string // e.g. http://127.0.0.1:19200 or https://s3.example.com
	Bucket   string
	Region   string // default us-east-1
	Creds    Creds
	PartSize int // default 8 MiB, min 5 MiB for real S3
	HTTP     *http.Client
	Now      func() time.Time
}

func (s *S3Store) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}
func (s *S3Store) region() string {
	if s.Region == "" {
		return "us-east-1"
	}
	return s.Region
}
func (s *S3Store) partSize() int {
	if s.PartSize <= 0 {
		return 8 << 20
	}
	return s.PartSize
}
func (s *S3Store) client() *http.Client {
	if s.HTTP != nil {
		return s.HTTP
	}
	return &http.Client{Timeout: 5 * time.Minute}
}

func (s *S3Store) target(key string) (host, path string, base *url.URL, err error) {
	base, err = url.Parse(s.Endpoint)
	if err != nil || base.Host == "" {
		return "", "", nil, fmt.Errorf("invalid s3 endpoint %q", s.Endpoint)
	}
	path = "/" + s.Bucket
	if key != "" {
		path += "/" + key
	}
	return base.Host, path, base, nil
}

func (s *S3Store) do(ctx context.Context, method, key string, q url.Values, body []byte) (*http.Response, error) {
	host, path, base, err := s.target(key)
	if err != nil {
		return nil, err
	}
	t := s.now().UTC()
	ph := sha256Hex(body)
	hdr := map[string]string{"host": host, "x-amz-date": t.Format("20060102T150405Z"), "x-amz-content-sha256": ph}
	if q == nil {
		q = url.Values{}
	}
	u := *base
	u.Path = path
	u.RawPath = uriEncode(path, false)
	u.RawQuery = canonicalQuery(q)
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(len(body))
	for k, v := range hdr {
		if k != "host" {
			req.Header.Set(k, v)
		}
	}
	req.Header.Set("Authorization", authorizationHeader(s.Creds, method, path, q, hdr, ph, t, s.region(), "s3"))
	return s.client().Do(req)
}

func (s *S3Store) check(resp *http.Response, op string, okCodes ...int) error {
	for _, c := range okCodes {
		if resp.StatusCode == c {
			return nil
		}
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("s3 %s: %s: %s", op, resp.Status, strings.TrimSpace(string(b)))
}

// CreateBucket is a dev/test helper (idempotent).
func (s *S3Store) CreateBucket(ctx context.Context) error {
	resp, err := s.do(ctx, http.MethodPut, "", nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return nil
	}
	return s.check(resp, "create bucket", 200)
}

func (s *S3Store) simple(ctx context.Context, method, key string, q url.Values, body []byte, op string, ok ...int) (*http.Response, error) {
	resp, err := s.do(ctx, method, key, q, body)
	if err != nil {
		return nil, err
	}
	if err := s.check(resp, op, ok...); err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	return resp, nil
}

// Put implements ObjectStore.
func (s *S3Store) Put(ctx context.Context, key string, r io.Reader) (int64, error) {
	buf := make([]byte, s.partSize())
	n, err := io.ReadFull(r, buf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		resp, perr := s.simple(ctx, http.MethodPut, key, nil, buf[:n], "put object", 200)
		if perr != nil {
			return 0, perr
		}
		_ = resp.Body.Close()
		return int64(n), nil
	}
	if err != nil {
		return 0, err
	}
	// multipart
	resp, err := s.simple(ctx, http.MethodPost, key, url.Values{"uploads": {""}}, nil, "create multipart", 200)
	if err != nil {
		return 0, err
	}
	var init struct {
		UploadID string `xml:"UploadId"`
	}
	err = xml.NewDecoder(resp.Body).Decode(&init)
	_ = resp.Body.Close()
	if err != nil || init.UploadID == "" {
		return 0, fmt.Errorf("s3 create multipart: bad response: %v", err)
	}
	abort := func() {
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if r, aerr := s.do(actx, http.MethodDelete, key, url.Values{"uploadId": {init.UploadID}}, nil); aerr == nil {
			_ = r.Body.Close()
		}
	}
	type part struct {
		N    int
		ETag string
	}
	var parts []part
	var total int64
	for pn := 1; ; pn++ {
		resp, perr := s.simple(ctx, http.MethodPut, key, url.Values{"partNumber": {itoa(int64(pn))}, "uploadId": {init.UploadID}}, buf[:n], "upload part", 200)
		if perr != nil {
			abort()
			return 0, perr
		}
		_ = resp.Body.Close()
		parts = append(parts, part{pn, resp.Header.Get("ETag")})
		total += int64(n)
		n, err = io.ReadFull(r, buf)
		if err == io.EOF {
			break
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			abort()
			return 0, err
		}
		if err == io.ErrUnexpectedEOF { // last, short part
			pn++
			resp, perr := s.simple(ctx, http.MethodPut, key, url.Values{"partNumber": {itoa(int64(pn))}, "uploadId": {init.UploadID}}, buf[:n], "upload part", 200)
			if perr != nil {
				abort()
				return 0, perr
			}
			_ = resp.Body.Close()
			parts = append(parts, part{pn, resp.Header.Get("ETag")})
			total += int64(n)
			break
		}
	}
	var x strings.Builder
	x.WriteString("<CompleteMultipartUpload>")
	for _, p := range parts {
		fmt.Fprintf(&x, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", p.N, xmlEscape(p.ETag))
	}
	x.WriteString("</CompleteMultipartUpload>")
	resp, err = s.simple(ctx, http.MethodPost, key, url.Values{"uploadId": {init.UploadID}}, []byte(x.String()), "complete multipart", 200)
	if err != nil {
		abort()
		return 0, err
	}
	// S3 may return 200 with an <Error> body.
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()
	if bytes.Contains(b, []byte("<Error>")) {
		abort()
		return 0, fmt.Errorf("s3 complete multipart: %s", strings.TrimSpace(string(b)))
	}
	return total, nil
}

func xmlEscape(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

// Delete implements ObjectStore.
func (s *S3Store) Delete(ctx context.Context, key string) error {
	resp, err := s.simple(ctx, http.MethodDelete, key, nil, nil, "delete object", 200, 204, 404)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

// Get fetches an object with a signed request (used by tests and verification).
func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.simple(ctx, http.MethodGet, key, nil, nil, "get object", 200)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// PresignGet implements Presigner. ttl 0 means DefaultPresignTTL; ttl must be within 1s..1h.
func (s *S3Store) PresignGet(key string, ttl time.Duration) (string, time.Time, error) {
	if ttl == 0 {
		ttl = DefaultPresignTTL
	}
	if ttl < time.Second || ttl > MaxPresignTTL {
		return "", time.Time{}, ErrBadTTL
	}
	host, path, base, err := s.target(key)
	if err != nil {
		return "", time.Time{}, err
	}
	t := s.now().UTC().Truncate(time.Second)
	q := presignQuery(s.Creds, http.MethodGet, host, path, nil, ttl, t, s.region(), "s3")
	u := *base
	u.Path = path
	u.RawPath = uriEncode(path, false)
	u.RawQuery = canonicalQuery(q)
	return u.String(), t.Add(ttl), nil
}

// Upload is an incomplete multipart upload as listed by ListMultipartUploads.
type Upload struct {
	Key, UploadID string
	Initiated     time.Time
}

// ListMultipartUploads lists the incomplete multipart uploads under prefix (follows pagination, at most maxPages pages).
func (s *S3Store) ListMultipartUploads(ctx context.Context, prefix string) ([]Upload, error) {
	var out []Upload
	keyMarker, idMarker := "", ""
	for page := 0; page < 100; page++ {
		q := url.Values{"uploads": {""}, "prefix": {prefix}}
		if keyMarker != "" {
			q.Set("key-marker", keyMarker)
		}
		if idMarker != "" {
			q.Set("upload-id-marker", idMarker)
		}
		resp, err := s.simple(ctx, http.MethodGet, "", q, nil, "list multipart uploads", 200)
		if err != nil {
			return nil, err
		}
		var r struct {
			Truncated bool   `xml:"IsTruncated"`
			NextKey   string `xml:"NextKeyMarker"`
			NextID    string `xml:"NextUploadIdMarker"`
			Uploads   []struct {
				Key       string `xml:"Key"`
				UploadID  string `xml:"UploadId"`
				Initiated string `xml:"Initiated"`
			} `xml:"Upload"`
		}
		err = xml.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&r)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("s3 list multipart uploads: bad response: %v", err)
		}
		for _, u := range r.Uploads {
			t, _ := time.Parse(time.RFC3339, u.Initiated)
			out = append(out, Upload{Key: u.Key, UploadID: u.UploadID, Initiated: t})
		}
		if !r.Truncated {
			break
		}
		keyMarker, idMarker = r.NextKey, r.NextID
	}
	return out, nil
}

// AbortUpload aborts one multipart upload (idempotent: an unknown upload id, 404, is success).
func (s *S3Store) AbortUpload(ctx context.Context, key, uploadID string) error {
	resp, err := s.simple(ctx, http.MethodDelete, key, url.Values{"uploadId": {uploadID}}, nil, "abort multipart", 200, 204, 404)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}
