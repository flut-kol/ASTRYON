package retrybridge

import (
	"context"
	"errors"
	"testing"

	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/retry-controller/pkg/retryclient"
)

type fakeClient struct {
	cause error
	d     retryclient.Decision
	err   error
	calls *[]string
}

func (f *fakeClient) Fail(_ context.Context, id string, cause error) (retryclient.Decision, error) {
	*f.calls = append(*f.calls, "fail")
	f.cause = cause
	return f.d, f.err
}

type fakeLedger struct {
	calls  *[]string
	rewErr error
	end    string
}

func (l *fakeLedger) Rewind(context.Context, jobs.Job) error {
	*l.calls = append(*l.calls, "rewind")
	return l.rewErr
}
func (l *fakeLedger) End(_ context.Context, _ jobs.Job, status, _ string) error {
	*l.calls = append(*l.calls, "end")
	l.end = status
	return nil
}

type fakeBase struct {
	Inner
	called bool
}

func (b *fakeBase) Fail(context.Context, jobs.Job, string, bool) (jobs.State, error) {
	b.called = true
	return jobs.Retrying, nil
}

func setup(d retryclient.Decision, ferr error) (*Store, *fakeClient, *fakeLedger, *fakeBase, *[]string) {
	calls := &[]string{}
	fc := &fakeClient{d: d, err: ferr, calls: calls}
	l := &fakeLedger{calls: calls}
	b := &fakeBase{}
	return &Store{Inner: b, Ledger: l, NewClient: func(string) (Failer, error) { return fc, nil }}, fc, l, b, calls
}

var job = jobs.Job{ID: "j", TenantID: "t", Fence: 2, MaxAttempts: 5}

func TestTransientSchedulesRetry(t *testing.T) {
	s, fc, l, b, calls := setup(retryclient.Decision{Action: retryclient.ActionRetry}, nil)
	st, err := s.Fail(context.Background(), job, "worker_error", true)
	if err != nil || st != jobs.Retrying || b.called || l.end != "RETRYING" {
		t.Fatalf("st=%v err=%v base=%v end=%q", st, err, b.called, l.end)
	}
	if retryclient.Transient != classOf(fc.cause) {
		t.Fatalf("cause not transient")
	}
	if len(*calls) != 3 || (*calls)[0] != "rewind" || (*calls)[1] != "fail" || (*calls)[2] != "end" {
		t.Fatalf("order: %v", *calls)
	}
}

func TestPermanentDeadLetters(t *testing.T) {
	s, fc, l, _, _ := setup(retryclient.Decision{Action: retryclient.ActionDeadLetter}, nil)
	st, err := s.Fail(context.Background(), job, "credentials_blocked", false)
	if err != nil || st != jobs.Failed || l.end != "FAILED" || classOf(fc.cause) != retryclient.Permanent {
		t.Fatalf("st=%v err=%v end=%q", st, err, l.end)
	}
}

func TestLeaseLostIsFenced(t *testing.T) {
	s, _, l, _, _ := setup(retryclient.Decision{}, retryclient.ErrLeaseLost)
	if _, err := s.Fail(context.Background(), job, "x", true); !errors.Is(err, jobs.ErrFenced) || l.end != "" {
		t.Fatalf("err=%v end=%q", err, l.end)
	}
	s, fc, l, _, calls := setup(retryclient.Decision{}, nil)
	l.rewErr = jobs.ErrFenced
	if _, err := s.Fail(context.Background(), job, "x", true); !errors.Is(err, jobs.ErrFenced) || fc.cause != nil || len(*calls) != 1 {
		t.Fatalf("stale fence must not reach the client: err=%v calls=%v", err, *calls)
	}
}

func TestClientBuildErrorFallsBackToStore(t *testing.T) {
	b := &fakeBase{}
	s := &Store{Inner: b, Ledger: &fakeLedger{calls: &[]string{}}, NewClient: func(string) (Failer, error) { return nil, errors.New("bad cfg") }}
	if st, err := s.Fail(context.Background(), job, "x", true); err != nil || st != jobs.Retrying || !b.called {
		t.Fatalf("st=%v err=%v base=%v", st, err, b.called)
	}
}

func TestClientCachedPerTenant(t *testing.T) {
	n := 0
	s := &Store{NewClient: func(string) (Failer, error) { n++; return &fakeClient{}, nil }}
	for range 3 {
		_, _ = s.client("a")
	}
	_, _ = s.client("b")
	if n != 2 {
		t.Fatalf("built %d clients", n)
	}
}

func classOf(err error) retryclient.Class {
	var c interface{ RetryClass() retryclient.Class }
	if errors.As(err, &c) {
		return c.RetryClass()
	}
	return retryclient.Permanent
}

var _ interface {
	Claim(context.Context) (jobs.Job, error)
} = (*jobs.Store)(nil)
var _ Inner = (*jobs.Store)(nil)
