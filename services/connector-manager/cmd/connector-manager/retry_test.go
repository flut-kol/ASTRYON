package main

import "testing"

func TestRetryBackoffModes(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	if f, err := retryBackoff(env(nil)); f != nil || err != nil {
		t.Fatalf("default must be local: %v", err)
	}
	if f, err := retryBackoff(env(map[string]string{"CONNECTOR_RETRY_MODE": "controller"})); f == nil || err != nil {
		t.Fatalf("controller: %v", err)
	}
	for _, m := range []map[string]string{
		{"CONNECTOR_RETRY_MODE": "bogus"},
		{"CONNECTOR_RETRY_MODE": "controller", "CONNECTOR_RETRY_BASE_SECONDS": "x"},
		{"CONNECTOR_RETRY_MODE": "controller", "CONNECTOR_RETRY_BASE_SECONDS": "100", "CONNECTOR_RETRY_MAX_SECONDS": "10"},
	} {
		if _, err := retryBackoff(env(m)); err == nil {
			t.Errorf("%v must fail", m)
		}
	}
}
