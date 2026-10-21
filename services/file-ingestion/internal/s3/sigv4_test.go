package s3

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

// Vectors from the AWS docs "Signature Calculations for the Authorization Header" / "Query String Auth".
var awsCreds = Creds{"AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}

func TestSigV4AuthorizationVector(t *testing.T) {
	ts := time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)
	hdr := map[string]string{
		"host":                 "examplebucket.s3.amazonaws.com",
		"range":                "bytes=0-9",
		"x-amz-content-sha256": emptySHA256,
		"x-amz-date":           "20130524T000000Z",
	}
	got := authorizationHeader(awsCreds, "GET", "/test.txt", url.Values{}, hdr, emptySHA256, ts, "us-east-1", "s3")
	want := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	if strings.ReplaceAll(got, ", ", ",") != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestSigV4PresignVector(t *testing.T) {
	ts := time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)
	q := presignQuery(awsCreds, "GET", "examplebucket.s3.amazonaws.com", "/test.txt", nil, 86400*time.Second, ts, "us-east-1", "s3")
	if got, want := q.Get("X-Amz-Signature"), "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"; got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestSigV4PutObjectVector(t *testing.T) {
	// AWS docs PUT object example (payload "Welcome to Amazon S3.").
	ts := time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)
	ph := sha256Hex([]byte("Welcome to Amazon S3."))
	if ph != "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072" {
		t.Fatal("payload hash")
	}
	hdr := map[string]string{
		"date": "Fri, 24 May 2013 00:00:00 GMT", "host": "examplebucket.s3.amazonaws.com",
		"x-amz-content-sha256": ph, "x-amz-date": "20130524T000000Z",
		"x-amz-storage-class": "REDUCED_REDUNDANCY",
	}
	got := authorizationHeader(awsCreds, "PUT", "/test$file.text", url.Values{}, hdr, ph, ts, "us-east-1", "s3")
	if !strings.HasSuffix(got, "Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd") {
		t.Fatal(got)
	}
}
