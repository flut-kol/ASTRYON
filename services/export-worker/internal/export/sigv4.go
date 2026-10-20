package export

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Hand-written AWS Signature Version 4 (stdlib only).

const (
	sigAlgo         = "AWS4-HMAC-SHA256"
	unsignedPayload = "UNSIGNED-PAYLOAD"
	emptySHA256     = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// Creds are static S3 credentials (from env; never in the repo).
type Creds struct{ AccessKey, SecretKey string }

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

// uriEncode is the SigV4 URI encoding (RFC 3986 unreserved kept, uppercase hex).
func uriEncode(s string, encodeSlash bool) string {
	const hexd = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexd[c>>4])
			b.WriteByte(hexd[c&15])
		}
	}
	return b.String()
}

func canonicalQuery(q url.Values) string {
	var parts []string
	for k, vs := range q {
		for _, v := range vs {
			parts = append(parts, uriEncode(k, true)+"="+uriEncode(v, true))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "&")
}

// canonicalRequest builds the canonical request. hdr keys are lower-case names of the signed headers.
func canonicalRequest(method, path string, q url.Values, hdr map[string]string, payloadHash string) (cr, signedHeaders string) {
	names := make([]string, 0, len(hdr))
	for k := range hdr {
		names = append(names, k)
	}
	sort.Strings(names)
	var ch strings.Builder
	for _, k := range names {
		ch.WriteString(k + ":" + strings.Join(strings.Fields(hdr[k]), " ") + "\n")
	}
	signedHeaders = strings.Join(names, ";")
	cr = strings.Join([]string{method, uriEncode(path, false), canonicalQuery(q), ch.String(), signedHeaders, payloadHash}, "\n")
	return cr, signedHeaders
}

func scope(t time.Time, region, service string) string {
	return t.UTC().Format("20060102") + "/" + region + "/" + service + "/aws4_request"
}

func signature(secret string, t time.Time, region, service, cr string) string {
	sts := strings.Join([]string{sigAlgo, t.UTC().Format("20060102T150405Z"), scope(t, region, service), sha256Hex([]byte(cr))}, "\n")
	k := hmacSHA256([]byte("AWS4"+secret), t.UTC().Format("20060102"))
	k = hmacSHA256(k, region)
	k = hmacSHA256(k, service)
	k = hmacSHA256(k, "aws4_request")
	return hex.EncodeToString(hmacSHA256(k, sts))
}

// authorizationHeader signs a request whose signed headers are hdr (must include host, x-amz-date,
// x-amz-content-sha256).
func authorizationHeader(c Creds, method, path string, q url.Values, hdr map[string]string, payloadHash string, t time.Time, region, service string) string {
	cr, signed := canonicalRequest(method, path, q, hdr, payloadHash)
	return sigAlgo + " Credential=" + c.AccessKey + "/" + scope(t, region, service) +
		", SignedHeaders=" + signed + ", Signature=" + signature(c.SecretKey, t, region, service, cr)
}

// presignQuery returns the full query (including X-Amz-Signature) for a pre-signed URL of host+path.
func presignQuery(c Creds, method, host, path string, extra url.Values, expires time.Duration, t time.Time, region, service string) url.Values {
	q := url.Values{}
	for k, v := range extra {
		q[k] = v
	}
	q.Set("X-Amz-Algorithm", sigAlgo)
	q.Set("X-Amz-Credential", c.AccessKey+"/"+scope(t, region, service))
	q.Set("X-Amz-Date", t.UTC().Format("20060102T150405Z"))
	q.Set("X-Amz-Expires", itoa(int64(expires/time.Second)))
	q.Set("X-Amz-SignedHeaders", "host")
	cr, _ := canonicalRequest(method, path, q, map[string]string{"host": host}, unsignedPayload)
	q.Set("X-Amz-Signature", signature(c.SecretKey, t, region, service, cr))
	return q
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
