package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestNewWritesJSONWithServiceAttr(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogger(&buf, "gateway", "info")

	logger.Info("hello", "foo", "bar")

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("expected valid JSON log line, got %q: %v", buf.String(), err)
	}

	if got["service"] != "gateway" {
		t.Errorf("service = %v, want %q", got["service"], "gateway")
	}
	if got["msg"] != "hello" {
		t.Errorf("msg = %v, want %q", got["msg"], "hello")
	}
	if got["foo"] != "bar" {
		t.Errorf("foo = %v, want %q", got["foo"], "bar")
	}
}

func TestParseLevel(t *testing.T) {
	cases := []struct {
		in   string
		want slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"nonsense", slog.LevelInfo},
		{"  Error  ", slog.LevelError},
	}

	for _, tc := range cases {
		if got := parseLevel(tc.in); got != tc.want {
			t.Errorf("parseLevel(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestNewRespectsLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogger(&buf, "sink", "warn")

	logger.Info("should be filtered out")
	if buf.Len() != 0 {
		t.Fatalf("expected no output for Info below Warn threshold, got %q", buf.String())
	}

	logger.Warn("should appear")
	if buf.Len() == 0 {
		t.Fatalf("expected output for Warn at threshold, got none")
	}
}

func TestWithContextAndFromContextRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogger(&buf, "gateway", "debug")

	ctx := WithContext(context.Background(), logger)
	got := FromContext(ctx)

	got.Info("via context")

	var parsed map[string]any
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("expected valid JSON log line, got %q: %v", buf.String(), err)
	}
	if parsed["service"] != "gateway" {
		t.Errorf("service = %v, want %q", parsed["service"], "gateway")
	}
}

func TestFromContextWithoutLoggerReturnsUsableDefault(t *testing.T) {
	logger := FromContext(context.Background())
	if logger == nil {
		t.Fatal("FromContext returned nil logger for empty context")
	}
	// Should not panic, and should behave like a normal slog.Logger.
	logger.Info("no panic expected")
}

func TestNewReturnsUsableLogger(t *testing.T) {
	// Smoke test for the public constructor: ensure it builds a usable
	// logger without panicking, including with an unrecognized level
	// (which should fall back to info rather than erroring).
	logger := New("gateway", "bogus-level")
	if logger == nil {
		t.Fatal("New returned nil logger")
	}
}
