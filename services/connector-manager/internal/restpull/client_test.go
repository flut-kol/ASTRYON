package restpull

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPullSendsCursorAndReturnsNextCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("after"); got != "cursor one" {
			t.Fatalf("cursor = %q", got)
		}
		w.Header().Set("X-Next-Cursor", "cursor two")
		_, _ = w.Write([]byte(`{"records":[1]}`))
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL + "/items?fixed=true", CursorParameter: "after", RequestsPerSecond: 10, MaxResponseBytes: 1024}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Pull(context.Background(), "cursor one")
	if err != nil {
		t.Fatal(err)
	}
	if response.NextCursor != "cursor two" || string(response.Body) != `{"records":[1]}` {
		t.Fatalf("unexpected response: %#v", response)
	}
}

func TestPullHonorsResponseBoundAndRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/large":
			_, _ = w.Write([]byte("12345"))
		default:
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
		}
	}))
	defer server.Close()

	large, err := NewClient(Config{Endpoint: server.URL + "/large", CursorParameter: "cursor", RequestsPerSecond: 10, MaxResponseBytes: 4}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := large.Pull(context.Background(), "a"); !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("expected bounded response error, got %v", err)
	}

	limited, err := NewClient(Config{Endpoint: server.URL + "/limited", CursorParameter: "cursor", RequestsPerSecond: 10, MaxResponseBytes: 4}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = limited.Pull(context.Background(), "a")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusTooManyRequests || httpErr.RetryAfter != 7*time.Second {
		t.Fatalf("unexpected HTTP error: %#v (%v)", httpErr, err)
	}
}

func TestPullRateReservationUsesContext(t *testing.T) {
	client, err := NewClient(Config{Endpoint: "https://example.invalid/items", CursorParameter: "cursor", RequestsPerSecond: 1, MaxResponseBytes: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	client.now = func() time.Time { return now }
	client.wait = func(ctx context.Context, delay time.Duration) error {
		if delay <= 0 {
			return nil
		}
		return ctx.Err()
	}
	if err := client.reserve(context.Background()); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.reserve(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestConfigRejectsUnsafeOrIncompleteEndpoints(t *testing.T) {
	for _, config := range []Config{
		{Endpoint: "", CursorParameter: "cursor", RequestsPerSecond: 1, MaxResponseBytes: 1},
		{Endpoint: "ftp://example.invalid", CursorParameter: "cursor", RequestsPerSecond: 1, MaxResponseBytes: 1},
		{Endpoint: "https://example.invalid", RequestsPerSecond: 1, MaxResponseBytes: 1},
	} {
		if _, err := NewClient(config, nil); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("config %#v error = %v", config, err)
		}
	}
	if got := parseRetryAfter(http.TimeFormat, time.Now()); got != 0 {
		t.Fatalf("invalid Retry-After = %s", got)
	}
	if strings.TrimSpace(" ") != "" { // keeps strings imported in this focused test.
		t.Fatal("unreachable")
	}
}
