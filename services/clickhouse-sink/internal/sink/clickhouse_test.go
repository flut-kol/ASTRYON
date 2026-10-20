package sink

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCHSinkRequest(t *testing.T) {
	var q, body, user string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q = r.URL.Query().Get("insert_deduplication_token") + "|" + r.URL.Query().Get("insert_deduplicate") + "|" + r.URL.Query().Get("query")
		b, _ := io.ReadAll(r.Body)
		body, user = string(b), r.Header.Get("X-ClickHouse-User")
	}))
	defer srv.Close()
	s, _ := NewCHSink(Config{CHURL: srv.URL, CHTable: "events_normalized_local", CHUser: "u"})
	if err := s.Insert(context.Background(), Chunk{Token: "tok", Rows: [][]byte{[]byte(`{"a":1}`), []byte(`{"a":2}`)}}); err != nil {
		t.Fatal(err)
	}
	if q != "tok|1|INSERT INTO events_normalized_local FORMAT JSONEachRow" || body != "{\"a\":1}\n{\"a\":2}\n" || user != "u" {
		t.Fatalf("%q %q %q", q, body, user)
	}
	if _, err := NewCHSink(Config{CHTable: "x; DROP"}); err == nil {
		t.Fatal("identifier injection accepted")
	}
}

func TestCHSinkHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "Code: 60", 404) }))
	defer srv.Close()
	s, _ := NewCHSink(Config{CHURL: srv.URL, CHTable: "t"})
	if err := s.Insert(context.Background(), Chunk{Rows: [][]byte{[]byte("{}")}}); err == nil {
		t.Fatal("expected error")
	}
}
