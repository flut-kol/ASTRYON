package replay

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsVersionConflict(t *testing.T) {
	uq := &pgconn.PgError{Code: "23505", ConstraintName: "replay_requests_version_uq"}
	if !isVersionConflict(uq) || !isVersionConflict(errors.Join(errors.New("x"), uq)) {
		t.Fatal("unique violation of the version index must be a conflict")
	}
	for _, e := range []error{nil, errors.New("x"),
		&pgconn.PgError{Code: "23505", ConstraintName: "replay_requests_pkey"},
		&pgconn.PgError{Code: "23503", ConstraintName: "replay_requests_version_uq"}} {
		if isVersionConflict(e) {
			t.Fatalf("%v must not be a conflict", e)
		}
	}
}

// uqDB fails the INSERT with a unique violation (a writer that bypassed the advisory lock took the version).
type uqDB struct{ fakeDB }

func (d *uqDB) Begin(ctx context.Context) (Tx, error) {
	tx, _ := d.fakeDB.Begin(ctx)
	return uqTx{tx.(*fakeTx)}, nil
}

type uqTx struct{ *fakeTx }

func (t uqTx) QueryRow(ctx context.Context, sql string, a ...any) Row {
	if sql == sqlIns {
		return fakeScan{func(...any) error {
			return &pgconn.PgError{Code: "23505", ConstraintName: "replay_requests_version_uq"}
		}}
	}
	return t.fakeTx.QueryRow(ctx, sql, a...)
}

func TestReserveMapsUniqueViolationToConflict(t *testing.T) {
	a := &PGAllocator{DB: &uqDB{}}
	_, err := a.Reserve(context.Background(), tenantA, srcX, time.Time{}, time.Time{})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v", err)
	}
}

func TestCreateConflictIs409AndPublishesNothing(t *testing.T) {
	pub := &MemPublisher{}
	pl := &Planner{Alloc: &PGAllocator{DB: &uqDB{}}, Counter: FixedCounter{}, Publisher: pub}
	auth := DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tenantA, Scopes: []string{ScopeReplayWrite}}}
	h := (&Handler{Auth: auth, Planner: pl}).Routes()
	r := httptest.NewRequest("POST", "/v1/replays", strings.NewReader(body("", false)))
	r.Header.Set("Authorization", "Bearer tok")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"replay_conflict"`) {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	if pub.Count() != 0 {
		t.Fatal("job published despite conflict")
	}
}
