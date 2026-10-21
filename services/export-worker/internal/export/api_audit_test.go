package export

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/astryon/audit"
)

type fakeAuditEventWriter struct {
	mu        sync.Mutex
	submitted []audit.Event
	synced    []audit.Event
	submitErr error
	syncErr   error
	syncCtx   context.Context
}

func (f *fakeAuditEventWriter) Submit(e audit.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.submitted = append(f.submitted, e)
	return f.submitErr
}

func (f *fakeAuditEventWriter) WriteSync(ctx context.Context, e audit.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.synced = append(f.synced, e)
	f.syncCtx = ctx
	return f.syncErr
}

func TestAPIAuditMapsOnlyPrincipalIdentity(t *testing.T) {
	f := &fakeAuditEventWriter{}
	a := &APIAudit{Writer: f, RequiredCreate: true, SyncTimeout: time.Second}
	p := Principal{TenantID: tenantA, Subject: "aas-sub", ClientID: "portal", ACR: "mfa", AMR: []string{"pwd", "otp"}}
	if err := a.BeforeExportCreate(context.Background(), p, "exp-00000001", "request-1"); err != nil {
		t.Fatal(err)
	}
	if len(f.synced) != 1 || len(f.submitted) != 0 {
		t.Fatalf("sync=%d submit=%d", len(f.synced), len(f.submitted))
	}
	e := f.synced[0]
	if e.TenantID != tenantA || e.Subject != "aas-sub" || e.ClientID != "portal" || e.ACR != "mfa" {
		t.Fatalf("identity mapping: %+v", e)
	}
	if e.Action != "export.create.prepare" || e.Outcome != audit.OutcomeSuccess || e.Resource.Type != "export" || e.Resource.ID != "exp-00000001" {
		t.Fatalf("event: %+v", e)
	}
	if e.RequestID != "request-1" || e.Details["phase"] != "prepared" {
		t.Fatalf("request/details: %+v", e)
	}
	if _, ok := f.syncCtx.Deadline(); !ok {
		t.Fatal("required write has no deadline")
	}
	a.ExportCreated(p, "exp-00000001", "request-1")
	if len(f.submitted) != 1 || f.submitted[0].Action != "export.create" ||
		f.submitted[0].Outcome != audit.OutcomeSuccess || f.submitted[0].Details["phase"] != "committed" {
		t.Fatalf("committed event: %+v", f.submitted)
	}
}

func TestRequiredCreateAuditFailsBeforeJobMutation(t *testing.T) {
	e := newEnv(t, newMemStore(), nil, 1)
	f := &fakeAuditEventWriter{syncErr: errors.New("postgres unavailable")}
	e.srv.APIAudit = &APIAudit{Writer: f, RequiredCreate: true}
	w := e.do("POST", "/v1/exports", "a", specBody)
	if w.Code != 503 || errCodeOf(t, w) != "audit_unavailable" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	e.jobs.mu.Lock()
	n := len(e.jobs.jobs)
	e.jobs.mu.Unlock()
	if n != 0 {
		t.Fatalf("created %d jobs despite required audit failure", n)
	}
	if len(e.audit.Records()) != 0 {
		t.Fatal("worker audit ran despite rejected create")
	}
}

func TestBestEffortAuditDoesNotBlockCreateAndAuditsDownload(t *testing.T) {
	e := newEnv(t, newMemStore(), nil, 1)
	f := &fakeAuditEventWriter{submitErr: audit.ErrQueueFull}
	e.srv.APIAudit = &APIAudit{Writer: f}
	w := e.do("POST", "/v1/exports", "a", specBody)
	if w.Code != 202 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	id := decodeMap(t, w)["id"].(string)
	waitStatus(t, e, "a", id, StatusSucceeded)

	f.mu.Lock()
	defer f.mu.Unlock()
	var creates, links int
	for _, ev := range f.submitted {
		switch ev.Action {
		case "export.create":
			creates++
		case "export.download_link":
			links++
			if ev.TenantID != tenantA || ev.Subject != "ua" || ev.ClientID != "portal" || ev.ACR != "mfa" {
				t.Fatalf("download identity did not come from authenticated principal: %+v", ev)
			}
			if ev.Resource.ID != id || len(ev.Details) != 0 {
				t.Fatalf("download event leaked details or wrong id: %+v", ev)
			}
		}
	}
	if creates != 1 || links < 1 {
		t.Fatalf("create=%d download_link=%d events=%+v", creates, links, f.submitted)
	}
}

type rejectQuota struct{ err error }

func (q rejectQuota) Acquire(string) (func(), error) { return nil, q.err }

type createErrorStore struct {
	MemoryJobStore
	err error
}

func (s *createErrorStore) Create(Job) error { return s.err }

func assertRequiredCreateFailed(t *testing.T, f *fakeAuditEventWriter) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.synced) != 1 || f.synced[0].Action != "export.create.prepare" || f.synced[0].Details["phase"] != "prepared" {
		t.Fatalf("prepared events: %+v", f.synced)
	}
	if len(f.submitted) != 1 || f.submitted[0].Action != "export.create" ||
		f.submitted[0].Outcome != audit.OutcomeFailure || f.submitted[0].Details["phase"] != "failed" {
		t.Fatalf("failure events: %+v", f.submitted)
	}
}

func TestRequiredCreateResolvesPreparedIntentOnQuotaFailure(t *testing.T) {
	e := newEnv(t, newMemStore(), rejectQuota{err: ErrQuotaExceeded}, 1)
	f := &fakeAuditEventWriter{}
	e.srv.APIAudit = &APIAudit{Writer: f, RequiredCreate: true}
	w := e.do("POST", "/v1/exports", "a", specBody)
	if w.Code != 429 || errCodeOf(t, w) != "quota_exceeded" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	assertRequiredCreateFailed(t, f)
}

func TestRequiredCreateResolvesPreparedIntentOnJobFailure(t *testing.T) {
	e := newEnv(t, newMemStore(), nil, 1)
	e.srv.Jobs = &createErrorStore{err: errors.New("create failed")}
	f := &fakeAuditEventWriter{}
	e.srv.APIAudit = &APIAudit{Writer: f, RequiredCreate: true}
	w := e.do("POST", "/v1/exports", "a", specBody)
	if w.Code != 500 || errCodeOf(t, w) != "internal" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	assertRequiredCreateFailed(t, f)
}
