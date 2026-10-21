// admin-bff: Admin UI backend-for-frontend (OIDC Authorization Code + PKCE against AAS, server-side tokens,
// /api/<service>/ reverse proxy). Env:
//
//	BFF_ADDR (:8090) BFF_PUBLIC_ORIGIN (https://admin.example) BFF_COOKIE_INSECURE=1 (http dev only)
//	AAS_BASE_URL  BFF_CLIENT_ID (astryon-portal)  BFF_CLIENT_KEY_FILE + BFF_CLIENT_KEY_ID (private_key_jwt)
//	BFF_CLIENT_SECRET (dev only, instead of the key)  BFF_SCOPES ("openid ...", space separated)
//	BFF_UPSTREAMS "gateway=http://..,files=..,query=..,connectors=..,exports=..,quarantine=.."
//	BFF_SESSION_MAX_AGE (8h)  BFF_ALLOWED_ORIGINS (comma separated extra Origins for unsafe requests, e.g. http://127.0.0.1:15173)  BFF_AFTER_LOGIN_URL (default "/"; absolute URL when the UI is served from another origin, e.g. the Vite dev server)
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/astryon/admin-bff/internal/bff"
)

func main() {
	origin := strings.TrimSuffix(os.Getenv("BFF_PUBLIC_ORIGIN"), "/")
	up := map[string]string{}
	for _, kv := range strings.Split(os.Getenv("BFF_UPSTREAMS"), ",") {
		if k, v, ok := strings.Cut(strings.TrimSpace(kv), "="); ok && k != "" && v != "" {
			up[k] = v
		}
	}
	age := 8 * time.Hour
	if v := os.Getenv("BFF_SESSION_MAX_AGE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			log.Fatalf("BFF_SESSION_MAX_AGE: %v", err)
		}
		age = d
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := bff.New(ctx, bff.Options{
		OIDC: bff.Config{BaseURL: os.Getenv("AAS_BASE_URL"), ClientID: os.Getenv("BFF_CLIENT_ID"), ClientSecret: os.Getenv("BFF_CLIENT_SECRET"),
			KeyFile: os.Getenv("BFF_CLIENT_KEY_FILE"), KeyID: os.Getenv("BFF_CLIENT_KEY_ID"), RedirectURL: origin + "/auth/callback", Scopes: strings.Fields(os.Getenv("BFF_SCOPES"))},
		PublicOrigin: origin, Upstreams: up, CookieSecure: os.Getenv("BFF_COOKIE_INSECURE") != "1", SessionMaxAge: age, AfterLoginPath: os.Getenv("BFF_AFTER_LOGIN_URL"), AllowedOrigins: splitList(os.Getenv("BFF_ALLOWED_ORIGINS")),
	})
	if err != nil {
		log.Fatal(err)
	}
	addr := os.Getenv("BFF_ADDR")
	if addr == "" {
		addr = ":8090"
	}
	srv := &http.Server{Addr: addr, Handler: s, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

func splitList(v string) []string {
	var out []string
	for _, x := range strings.Split(v, ",") {
		if x = strings.TrimSuffix(strings.TrimSpace(x), "/"); x != "" {
			out = append(out, x)
		}
	}
	return out
}
