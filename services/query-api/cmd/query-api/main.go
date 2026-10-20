// Command query-api serves POST /v1/query/events and /v1/query/aggregate (DPP-029).
// Env: LISTEN_ADDR (:8082) CH_URL CH_DATABASE CH_USER CH_PASSWORD; DEV ONLY: QUERY_DEV_TOKEN + QUERY_DEV_TENANT.
// AAS: AAS_BASE_URL AAS_ISSUER AAS_AUDIENCE AAS_ENV AAS_ALLOWED_AZP (tokens need scope astryon:query:read and a tenant claim).
// Limits (DPP-050): QUERY_RATE_PER_SEC QUERY_BURST QUERY_MAX_CONCURRENT QUERY_IDLE_TTL_SECONDS QUERY_MAX_TENANTS
// QUERY_MAX_EXECUTION_SECONDS QUERY_MAX_RESULT_ROWS QUERY_MAX_MEMORY_BYTES.
// Shared limits across replicas: QUERY_REDIS_URL (redis://[user:pass@]host:port[/db]) QUERY_REDIS_FAILURE (local|closed) QUERY_REDIS_NODES QUERY_REDIS_TOPOLOGY_REFRESH_SECONDS QUERY_REDIS_SENTINELS QUERY_REDIS_MASTER_NAME QUERY_REDIS_LEASE_SECONDS.
// Active version (replay cutover): QUERY_PG_URL (role without BYPASSRLS) QUERY_VERSION_CACHE_SECONDS (default 5). Unset = processing_version required.
// Neither AAS nor the dev token configured -> every request is rejected (fail closed).
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	goauth "github.com/astryon/go-common/auth"
	"github.com/astryon/query-api/internal/query"
	"github.com/jackc/pgx/v5/pgxpool"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

type denyAll struct{}

func (denyAll) Authenticate(*http.Request) (query.Principal, error) {
	return query.Principal{}, query.ErrUnauthenticated
}

func main() {
	var auth query.Authenticator = denyAll{}
	if os.Getenv("AAS_BASE_URL") != "" {
		v, err := goauth.NewVerifier(context.Background(), aasConfig())
		if err != nil {
			log.Fatalf("aas: %v", err)
		}
		auth = aasAuthenticator{v}
	}
	if t := os.Getenv("QUERY_DEV_TOKEN"); t != "" {
		auth = query.DevStaticAuthenticator{Token: t, Principal: query.Principal{
			TenantID: os.Getenv("QUERY_DEV_TENANT"), Scopes: []string{query.ScopeQueryRead}}}
		log.Print("WARNING: dev static authenticator enabled")
	}
	lc, caps, err := query.LoadLimits(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	limiter := query.NewLimiter(lc)
	shared, mode, err := query.LoadShared(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	if shared != nil {
		limiter.UseShared(shared, mode)
		log.Printf("query-api: shared (Redis) tenant limits enabled, on failure: %s", mode)
	} else {
		log.Print("query-api: tenant limits are per instance (QUERY_REDIS_URL unset)")
	}
	s := &query.Server{Auth: auth, Limiter: limiter, Caps: caps, CHURL: env("CH_URL", "http://localhost:8123"), CHDatabase: env("CH_DATABASE", "astryon"),
		CHUser: os.Getenv("CH_USER"), CHPass: os.Getenv("CH_PASSWORD")}
	if u := os.Getenv("QUERY_PG_URL"); u != "" {
		ttl := query.DefaultVersionCacheTTL
		if v := os.Getenv("QUERY_VERSION_CACHE_SECONDS"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				log.Fatalf("QUERY_VERSION_CACHE_SECONDS must be a positive integer, got %q", v)
			}
			ttl = time.Duration(n) * time.Second
		}
		pool, err := pgxpool.New(context.Background(), u)
		if err != nil {
			log.Fatalf("QUERY_PG_URL: %v", err)
		}
		defer pool.Close()
		s.Versions = &query.CachingResolver{Inner: query.PGVersionResolver{Pool: pool}, TTL: ttl}
		log.Printf("query-api: processing_version optional (active version per source, cache %s)", ttl)
	} else {
		log.Print("query-api: QUERY_PG_URL unset: processing_version is required on every request")
	}
	srv := &http.Server{Addr: env("LISTEN_ADDR", ":8082"), Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("query-api listening on %s", srv.Addr)
	log.Fatal(srv.ListenAndServe())
}
