package query

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Active processing version (replay cutover). Rules:
//   - an explicit processing_version in the request always wins (inspect a replay before cutover);
//   - omitted + source_id -> that source's active version from source_active_versions (migration 0010);
//   - omitted without source_id -> 400: a tenant-wide read would mix per-source versions, so the caller must
//     name the source (or an explicit version);
//   - a source without a row -> 400 no_active_version. There is deliberately NO default of 1: a missing cutover
//     must be explicit, otherwise readers could silently serve a stale or wrong version;
//   - no resolver configured (QUERY_PG_URL unset) -> processing_version stays required.

// ErrNoActiveVersion means the tenant has no active version for the source (no row, or the source is not the tenant's).
var ErrNoActiveVersion = errors.New("no active processing version")

// VersionResolver returns the active processing version of one source of one tenant.
// tenantID must come from the verified Principal. Unknown or foreign sources yield ErrNoActiveVersion.
type VersionResolver interface {
	Resolve(ctx context.Context, tenantID, sourceID string) (uint32, error)
}

// resolveVersion fills f.ProcessingVersion when the caller omitted it.
func (s *Server) resolveVersion(ctx context.Context, p Principal, f *Filter) error {
	if f.ProcessingVersion != nil {
		return nil
	}
	if err := f.validateRange(); err != nil {
		return err
	}
	if s.Versions == nil {
		return bad("invalid_request", "processing_version is required")
	}
	if f.SourceID == "" {
		return bad("invalid_request", "source_id is required when processing_version is omitted")
	}
	if !uuidRe.MatchString(f.SourceID) {
		return bad("invalid_request", "source_id must be a UUID")
	}
	v, err := s.Versions.Resolve(ctx, p.TenantID, f.SourceID)
	if errors.Is(err, ErrNoActiveVersion) {
		return bad("no_active_version", "source has no active processing_version; pass processing_version explicitly")
	}
	if err != nil {
		return err // 502 backend_error (details are logged, never returned)
	}
	f.ProcessingVersion = &v
	return nil
}

// PGVersionResolver reads source_active_versions. Every lookup is its own short transaction that sets the RLS
// tenant (`astryon.tenant_id`, migration 0001) from the argument, which callers take only from the Principal.
// The database role must NOT bypass RLS.
type PGVersionResolver struct{ Pool *pgxpool.Pool }

const (
	sqlSetTenant = `SELECT set_config('astryon.tenant_id', $1, true)`
	sqlActive    = `SELECT active_processing_version FROM source_active_versions WHERE tenant_id = $1::uuid AND source_id = $2::uuid`
)

func (r PGVersionResolver) Resolve(ctx context.Context, tenantID, sourceID string) (uint32, error) {
	if !uuidRe.MatchString(tenantID) || !uuidRe.MatchString(sourceID) {
		return 0, ErrNoActiveVersion
	}
	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, sqlSetTenant, tenantID); err != nil {
		return 0, err
	}
	var v int32
	switch err := tx.QueryRow(ctx, sqlActive, tenantID, sourceID).Scan(&v); {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, ErrNoActiveVersion
	case err != nil:
		return 0, err
	case v <= 0:
		return 0, fmt.Errorf("query: invalid stored active version %d", v)
	}
	return uint32(v), nil
}

// CachingResolver caches results (including "no active version") per tenant+source for a short TTL, bounded in size.
// A cutover is therefore visible to readers after at most TTL.
type CachingResolver struct {
	Inner      VersionResolver
	TTL        time.Duration // <= 0 -> DefaultVersionCacheTTL
	MaxEntries int           // <= 0 -> 10000
	Now        func() time.Time

	mu sync.Mutex
	m  map[cacheKey]cacheVal
}

// DefaultVersionCacheTTL is used when QUERY_VERSION_CACHE_SECONDS is unset.
const DefaultVersionCacheTTL = 5 * time.Second

type cacheKey struct{ tenant, source string }
type cacheVal struct {
	v   uint32
	ok  bool
	exp time.Time
}

func (c *CachingResolver) Resolve(ctx context.Context, tenantID, sourceID string) (uint32, error) {
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	ttl := c.TTL
	if ttl <= 0 {
		ttl = DefaultVersionCacheTTL
	}
	k := cacheKey{tenantID, sourceID} // the tenant is part of the key: a hit can never cross tenants
	c.mu.Lock()
	if e, ok := c.m[k]; ok && now().Before(e.exp) {
		c.mu.Unlock()
		if !e.ok {
			return 0, ErrNoActiveVersion
		}
		return e.v, nil
	}
	c.mu.Unlock()
	v, err := c.Inner.Resolve(ctx, tenantID, sourceID)
	if err != nil && !errors.Is(err, ErrNoActiveVersion) {
		return 0, err // backend errors are never cached
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[cacheKey]cacheVal{}
	}
	limit := c.MaxEntries
	if limit <= 0 {
		limit = 10000
	}
	if len(c.m) >= limit {
		for kk, e := range c.m { // drop expired first, then everything if still full
			if !now().Before(e.exp) {
				delete(c.m, kk)
			}
		}
		if len(c.m) >= limit {
			c.m = map[cacheKey]cacheVal{}
		}
	}
	c.m[k] = cacheVal{v: v, ok: err == nil, exp: now().Add(ttl)}
	return v, err
}
