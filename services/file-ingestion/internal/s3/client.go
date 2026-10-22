// Package s3 is a small stdlib S3 client for file ingestion: multipart creation with pre-signed part URLs,
// completion, HEAD, ranged GET and streaming GET. Credentials come from the environment, never the repo.
package s3

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	Endpoint string // e.g. http://127.0.0.1:19000 or https://s3.example.com
	Bucket   string
	Region   string // default us-east-1
	Creds    Creds
	HTTP     *http.Client
	Now      func() time.Time
}

func (c *Client) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}
func (c *Client) region() string {
	if c.Region == "" {
		return "us-east-1"
	}
	return c.Region
}
func (c *Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 10 * time.Minute}
}

func (c *Client) target(key string) (host, path string, base *url.URL, err error) {
	base, err = url.Parse(c.Endpoint)
	if err != nil || base.Host == "" {
		return "", "", nil, fmt.Errorf("invalid s3 endpoint %q", c.Endpoint)
	}
	path = "/" + c.Bucket
	if key != "" {
		path += "/" + key
	}
	return base.Host, path, base, nil
}

// do sends a signed request. body is sent whole (small control payloads only); GET bodies are streamed back.
func (c *Client) do(ctx context.Context, method, key string, q url.Values, body []byte, extra map[string]string) (*http.Response, error) {
	host, path, base, err := c.target(key)
	if err != nil {
		return nil, err
	}
	t := c.now().UTC()
	ph := sha256Hex(body)
	hdr := map[string]string{"host": host, "x-amz-date": t.Format("20060102T150405Z"), "x-amz-content-sha256": ph}
	for k, v := range extra {
		hdr[strings.ToLower(k)] = v
	}
	if q == nil {
		q = url.Values{}
	}
	u := *base
	u.Path, u.RawPath, u.RawQuery = path, uriEncode(path, false), canonicalQuery(q)
	req, err := http.NewRequestWithContext(ctx, method, u.String(), strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(len(body))
	for k, v := range hdr {
		if k != "host" {
			req.Header.Set(k, v)
		}
	}
	req.Header.Set("Authorization", authorizationHeader(c.Creds, method, path, q, hdr, ph, t, c.region(), "s3"))
	return c.http().Do(req)
}

func check(resp *http.Response, op string, ok ...int) error {
	for _, code := range ok {
		if resp.StatusCode == code {
			return nil
		}
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("s3 %s: %s: %s", op, resp.Status, strings.TrimSpace(string(b)))
}

// CreateMultipart starts an upload and returns its id with one pre-signed PUT URL per part (valid for ttl).
// Parts are uploaded by the client directly; the service never proxies file bytes.
func (c *Client) CreateMultipart(ctx context.Context, key string, parts int, ttl time.Duration) (string, []string, error) {
	if parts < 1 || parts > 10000 || ttl < time.Second || ttl > 7*24*time.Hour {
		return "", nil, fmt.Errorf("s3: invalid parts/ttl")
	}
	resp, err := c.do(ctx, http.MethodPost, key, url.Values{"uploads": {""}}, nil, nil)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	if err := check(resp, "create multipart", 200); err != nil {
		return "", nil, err
	}
	var out struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&out); err != nil || out.UploadID == "" {
		return "", nil, fmt.Errorf("s3 create multipart: bad response")
	}
	host, path, base, err := c.target(key)
	if err != nil {
		return "", nil, err
	}
	t := c.now().UTC().Truncate(time.Second)
	urls := make([]string, parts)
	for i := range urls {
		extra := url.Values{"partNumber": {strconv.Itoa(i + 1)}, "uploadId": {out.UploadID}}
		q := presignQuery(c.Creds, http.MethodPut, host, path, extra, ttl, t, c.region(), "s3")
		u := *base
		u.Path, u.RawPath, u.RawQuery = path, uriEncode(path, false), canonicalQuery(q)
		urls[i] = u.String()
	}
	return out.UploadID, urls, nil
}

// Part is an uploaded part as reported by the client (ETag from the PUT response).
type Part struct {
	Number int
	ETag   string
}

// CompleteMultipart finishes an upload from the ordered part list.
func (c *Client) CompleteMultipart(ctx context.Context, key, uploadID string, parts []Part) error {
	var b strings.Builder
	b.WriteString("<CompleteMultipartUpload>")
	for _, p := range parts {
		fmt.Fprintf(&b, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", p.Number, xmlEscape(p.ETag))
	}
	b.WriteString("</CompleteMultipartUpload>")
	resp, err := c.do(ctx, http.MethodPost, key, url.Values{"uploadId": {uploadID}}, []byte(b.String()), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// S3 may answer 200 with an <Error> body; treat that as failure
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode != 200 || strings.Contains(string(body), "<Error>") {
		return fmt.Errorf("s3 complete multipart: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return nil
}

// Abort drops an unfinished upload.
func (c *Client) Abort(ctx context.Context, key, uploadID string) error {
	resp, err := c.do(ctx, http.MethodDelete, key, url.Values{"uploadId": {uploadID}}, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return check(resp, "abort multipart", 204, 404)
}

// Head returns the object size; ErrNotFound if it does not exist.
func (c *Client) Head(ctx context.Context, key string) (int64, error) {
	resp, err := c.do(ctx, http.MethodHead, key, nil, nil, nil)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return 0, ErrNotFound
	}
	if err := check(resp, "head", 200); err != nil {
		return 0, err
	}
	return resp.ContentLength, nil
}

// Get streams the object from byte offset (0 = whole object). The caller closes the body.
func (c *Client) Get(ctx context.Context, key string, offset int64) (io.ReadCloser, error) {
	var extra map[string]string
	if offset > 0 {
		extra = map[string]string{"Range": fmt.Sprintf("bytes=%d-", offset)}
	}
	resp, err := c.do(ctx, http.MethodGet, key, nil, nil, extra)
	if err != nil {
		return nil, err
	}
	want := 200
	if offset > 0 {
		want = 206 // a 200 here would mean the Range header was ignored: never parse from the wrong offset
	}
	if resp.StatusCode == 404 {
		_ = resp.Body.Close()
		return nil, ErrNotFound
	}
	if err := check(resp, "get", want); err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	return resp.Body, nil
}

// Put stores a small, whole object (worker output parts; body is held in memory by the caller and signed whole).
func (c *Client) Put(ctx context.Context, key string, body []byte, contentType string) error {
	resp, err := c.do(ctx, http.MethodPut, key, nil, body, map[string]string{"Content-Type": contentType})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return check(resp, "put", 200, 201, 204)
}

// Delete removes an object (used to discard uploads whose checksum did not verify).
func (c *Client) Delete(ctx context.Context, key string) error {
	resp, err := c.do(ctx, http.MethodDelete, key, nil, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return check(resp, "delete", 204, 404)
}

var ErrNotFound = fmt.Errorf("s3: object not found")

func xmlEscape(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}
