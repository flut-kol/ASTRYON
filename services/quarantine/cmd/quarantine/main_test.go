package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestValidateAuthConfiguration(t *testing.T) {
	values := map[string]string{}
	getenv := func(key string) string { return values[key] }
	if err := validateAuthConfiguration(getenv); err != nil {
		t.Fatal(err)
	}
	values["QUARANTINE_DEV_TOKEN"] = "secret"
	values["QUARANTINE_DEV_TENANT"] = "00000000-0000-4000-8000-0000000000a1"
	if err := validateAuthConfiguration(getenv); err == nil {
		t.Fatal("dev auth accepted without explicit enable flag")
	}
	values["QUARANTINE_ENABLE_DEV_AUTH"] = "true"
	if err := validateAuthConfiguration(getenv); err != nil {
		t.Fatal(err)
	}
	values["AAS_BASE_URL"] = "https://aas.example"
	if err := validateAuthConfiguration(getenv); err == nil {
		t.Fatal("mixed AAS/dev auth accepted")
	}
	delete(values, "QUARANTINE_DEV_TOKEN")
	delete(values, "QUARANTINE_DEV_TENANT")
	delete(values, "QUARANTINE_ENABLE_DEV_AUTH")
	for _, key := range []string{"AAS_ISSUER", "AAS_AUDIENCE", "AAS_ENV", "AAS_ALLOWED_AZP"} {
		values[key] = "configured"
	}
	if err := validateAuthConfiguration(getenv); err != nil {
		t.Fatal(err)
	}
}

func TestReportJournalFailureRequestsProcessRestart(t *testing.T) {
	failures := make(chan error, 1)
	reportJournalFailure(failures, errors.New("clickhouse unavailable"))
	select {
	case err := <-failures:
		if err == nil || !strings.Contains(err.Error(), "clickhouse unavailable") {
			t.Fatalf("reported error = %v", err)
		}
	default:
		t.Fatal("journal failure was not reported to the process supervisor")
	}
}

func TestReportJournalFailureIgnoresExpectedCancellation(t *testing.T) {
	failures := make(chan error, 1)
	reportJournalFailure(failures, context.Canceled)
	select {
	case err := <-failures:
		t.Fatalf("cancellation reported as terminal failure: %v", err)
	default:
	}
}

func TestReportJournalFailureDoesNotBlockWhenSupervisorAlreadyHasFailure(t *testing.T) {
	failures := make(chan error, 1)
	failures <- errors.New("first failure")
	reportJournalFailure(failures, errors.New("second failure"))
	if got := (<-failures).Error(); got != "first failure" {
		t.Fatalf("queued failure = %q", got)
	}
}
