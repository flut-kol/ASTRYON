package bff

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"
)

// CSRFHeader must be present (any value from this list) on every unsafe request; browsers cannot add it
// cross-site without a CORS preflight, which this service never grants.
const CSRFHeader, CSRFValue = "X-Requested-With", "astryon-admin"

// Options configure the HTTP layer.
type Options struct {
	OIDC           Config
	PublicOrigin   string            // scheme://host[:port] the browser sees; checked against Origin on unsafe requests
	Upstreams      map[string]string // path prefix ("query") -> base URL; served at /api/<prefix>/...
	CookieSecure   bool              // false only for http://localhost dev (cookie loses the __Host- prefix)
	SessionMaxAge  time.Duration     // absolute session lifetime (default 8h)
	LoginTimeout   time.Duration     // default 10m
	MaxSessions    int               // default 10000
	AfterLoginPath string            // default "/"
	AllowedOrigins []string          // extra Origins accepted on unsafe requests (UI served from another origin, e.g. the Vite dev server)
	Logger         *log.Logger
	Store          Store // session/login/replay store; default: in-process memory (single replica). See NewRedisStore.
}

// Server is the BFF http.Handler.
type Server struct {
	o       Options
	oc      *oidcClient
	mux     *http.ServeMux
	store   Store
	locks   [64]sync.Mutex // per-process refresh serialisation, striped by session id
	proxies map[string]*httputil.ReverseProxy
}

func New(ctx context.Context, o Options) (*Server, error) {
	oc, err := newOIDC(ctx, o.OIDC)
	if err != nil {
		return nil, err
	}
	if o.SessionMaxAge <= 0 {
		o.SessionMaxAge = 8 * time.Hour
	}
	if o.LoginTimeout <= 0 {
		o.LoginTimeout = 10 * time.Minute
	}
	if o.MaxSessions <= 0 {
		o.MaxSessions = 10000
	}
	if o.AfterLoginPath == "" {
		o.AfterLoginPath = "/"
	}
	if o.Logger == nil {
		o.Logger = log.Default()
	}
	s := &Server{o: o, oc: oc, mux: http.NewServeMux(), store: o.Store, proxies: map[string]*httputil.ReverseProxy{}}
	if s.store == nil {
		s.store = NewMemoryStore(o.MaxSessions, func() time.Time { return s.oc.now() })
	}
	for prefix, target := range o.Upstreams {
		u, err := url.Parse(target)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return nil, errors.New("bff: bad upstream for " + prefix)
		}
		s.proxies[prefix] = &httputil.ReverseProxy{
			Rewrite: func(r *httputil.ProxyRequest) {
				r.SetURL(u)
				r.Out.URL.Path = strings.TrimSuffix(u.Path, "/") + r.In.URL.Path[len("/api/"+prefix):]
				r.Out.URL.RawPath = ""
				r.Out.Header.Del("Cookie")
				r.Out.Header.Set("Authorization", r.In.Header.Get(internalAuth)) // never the browser's own header
				r.Out.Header.Del(internalAuth)
			},
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				jsonErr(w, http.StatusBadGateway, "upstream_unavailable", "upstream is not reachable")
			},
		}
	}
	s.mux.HandleFunc("GET /auth/login", s.login)
	s.mux.HandleFunc("GET /auth/callback", s.callback)
	s.mux.HandleFunc("GET /auth/me", s.me)
	s.mux.HandleFunc("POST /auth/logout", s.logout)
	s.mux.HandleFunc("POST /auth/backchannel-logout", s.backchannelLogout) // server-to-server from AAS: no cookie, no CSRF header; authenticated by the signed logout_token
	if strings.Contains(s.o.AfterLoginPath, "://") {                       // post-logout lands on the BFF origin: hand the browser back to the UI
		s.mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, s.o.AfterLoginPath, http.StatusFound)
		})
	}
	s.mux.HandleFunc("/api/{service}/{rest...}", s.proxy)
	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	s.mux.ServeHTTP(w, r)
}

func (s *Server) cookieName(base string) string {
	if s.o.CookieSecure {
		return "__Host-" + base
	}
	return base
}

func (s *Server) setCookie(w http.ResponseWriter, name, val string, maxAge int) {
	http.SetCookie(w, &http.Cookie{Name: s.cookieName(name), Value: val, Path: "/", HttpOnly: true, Secure: s.o.CookieSecure, SameSite: http.SameSiteLaxMode, MaxAge: maxAge})
}

func jsonErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	state, nonce, verifier := randString(24), randString(24), randString(48)
	now := s.oc.now()
	switch err := s.store.SavePending(r.Context(), state, Pending{verifier, nonce, now.Add(s.o.LoginTimeout)}, s.o.LoginTimeout); {
	case errors.Is(err, ErrFull):
		jsonErr(w, http.StatusServiceUnavailable, "busy", "too many logins in progress")
		return
	case err != nil:
		s.o.Logger.Printf("login: store: %v", err)
		jsonErr(w, http.StatusServiceUnavailable, "unavailable", "session store unavailable")
		return
	}
	s.setCookie(w, "astryon_login", state, int(s.o.LoginTimeout.Seconds()))
	http.Redirect(w, r, s.oc.authURL(state, nonce, verifier), http.StatusFound)
}

func (s *Server) callback(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	state := q.Get("state")
	c, err := r.Cookie(s.cookieName("astryon_login"))
	s.setCookie(w, "astryon_login", "", -1)
	p, ok, perr := s.store.TakePending(r.Context(), state) // single use
	if perr != nil {
		s.o.Logger.Printf("callback: store: %v", perr)
		jsonErr(w, http.StatusServiceUnavailable, "unavailable", "session store unavailable")
		return
	}
	if err != nil || state == "" || subtle.ConstantTimeCompare([]byte(c.Value), []byte(state)) != 1 || !ok || s.oc.now().After(p.Exp) {
		jsonErr(w, http.StatusBadRequest, "invalid_state", "login state is missing, expired or does not belong to this browser")
		return
	}
	if e := q.Get("error"); e != "" {
		jsonErr(w, http.StatusUnauthorized, "login_denied", "AAS refused the login: "+safeCode(e))
		return
	}
	code := q.Get("code")
	if code == "" {
		jsonErr(w, http.StatusBadRequest, "invalid_request", "missing code")
		return
	}
	tok, err := s.oc.exchange(r.Context(), code, p.Verifier)
	if err != nil {
		s.o.Logger.Printf("code exchange failed: %v", err)
		jsonErr(w, http.StatusBadGateway, "token_exchange_failed", "could not complete the login")
		return
	}
	sub, sid := "", ""
	if tok.ID != "" {
		if sub, sid, err = s.oc.idClaims(tok.ID, p.Nonce); err != nil {
			s.o.Logger.Printf("id_token rejected: %v", err)
			jsonErr(w, http.StatusUnauthorized, "invalid_id_token", "identity token was rejected")
			return
		}
	}
	id := randString(32)
	if err := s.store.CreateSession(r.Context(), id, Record{Tok: tok, Sub: sub, SID: sid, Created: s.oc.now()}, s.o.SessionMaxAge); err != nil {
		s.o.Logger.Printf("callback: store: %v", err)
		jsonErr(w, http.StatusServiceUnavailable, "unavailable", "session store unavailable")
		return
	}
	s.setCookie(w, "astryon_session", id, int(s.o.SessionMaxAge.Seconds()))
	http.Redirect(w, r, s.o.AfterLoginPath, http.StatusFound)
}

func safeCode(e string) string {
	if len(e) > 40 {
		e = e[:40]
	}
	return strings.Map(func(r rune) rune {
		if r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' {
			return r
		}
		return -1
	}, e)
}

func (s *Server) lookup(r *http.Request) (string, *Record) {
	c, err := r.Cookie(s.cookieName("astryon_session"))
	if err != nil {
		return "", nil
	}
	rec, ok, err := s.store.GetSession(r.Context(), c.Value)
	if err != nil {
		s.o.Logger.Printf("session lookup: %v", err) // fail closed: treated as signed out
		return "", nil
	}
	if !ok {
		return "", nil
	}
	if s.oc.now().Sub(rec.Created) > s.o.SessionMaxAge {
		_ = s.store.DeleteSession(r.Context(), c.Value)
		return "", nil
	}
	return c.Value, &rec
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	_, se := s.lookup(r)
	w.Header().Set("Content-Type", "application/json")
	if se == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"authenticated": false})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"authenticated": true, "subject": se.Sub, "session_expires_at": se.Created.Add(s.o.SessionMaxAge).UTC()})
}

// csrfOK: unsafe methods need the custom header and, when the browser sends Origin, the public origin.
func (s *Server) csrfOK(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	}
	if r.Header.Get(CSRFHeader) != CSRFValue {
		return false
	}
	o := r.Header.Get("Origin")
	if o == "" || s.o.PublicOrigin == "" || o == s.o.PublicOrigin {
		return true
	}
	for _, a := range s.o.AllowedOrigins {
		if o == a {
			return true
		}
	}
	return false
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if !s.csrfOK(r) {
		jsonErr(w, http.StatusForbidden, "csrf", "missing or invalid CSRF header")
		return
	}
	id, se := s.lookup(r)
	if id != "" {
		if err := s.store.DeleteSession(r.Context(), id); err != nil {
			s.o.Logger.Printf("logout: store: %v", err)
		}
	}
	s.setCookie(w, "astryon_session", "", -1)
	out := map[string]any{"logged_out": true}
	if se != nil && s.oc.ep.EndSession != "" {
		q := url.Values{"client_id": {s.oc.cfg.ClientID}}
		if se.Tok.ID != "" {
			q.Set("id_token_hint", se.Tok.ID)
		}
		if s.o.PublicOrigin != "" {
			after := s.o.AfterLoginPath
			if strings.Contains(after, "://") { // absolute UI URL: the registered post-logout URI is the BFF's own origin
				after = "/"
			}
			q.Set("post_logout_redirect_uri", s.o.PublicOrigin+after)
		}
		out["logout_url"] = s.oc.ep.EndSession + "?" + q.Encode()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// accessToken returns a token valid for at least 30 s, refreshing under a per-process lock. The record is re-read
// under the lock, so a refresh done by another request (or, with a shared store, another replica) is reused.
// Two replicas refreshing the same session at the same instant can still race; if AAS rotates refresh tokens the
// loser re-reads the winner's result instead of ending the session.
func (s *Server) accessToken(ctx context.Context, id string) (string, bool) {
	h := fnv32(id) % uint32(len(s.locks))
	s.locks[h].Lock()
	defer s.locks[h].Unlock()
	ttlOf := func(r Record) time.Duration { return s.o.SessionMaxAge - s.oc.now().Sub(r.Created) }
	load := func() (Record, bool) {
		r, ok, err := s.store.GetSession(ctx, id)
		if err != nil {
			s.o.Logger.Printf("session load: %v", err)
		}
		return r, ok && err == nil
	}
	rec, ok := load()
	if !ok {
		return "", false
	}
	if rec.Tok.Expires.After(s.oc.now().Add(30 * time.Second)) {
		return rec.Tok.Access, true
	}
	if rec.Tok.Refresh == "" {
		return "", false
	}
	nt, err := s.oc.refresh(ctx, rec.Tok.Refresh)
	if err != nil {
		if r2, ok := load(); ok && r2.Tok.Expires.After(s.oc.now().Add(30*time.Second)) {
			return r2.Tok.Access, true // another replica refreshed first
		}
		s.o.Logger.Printf("refresh failed, session dropped: %v", err)
		_ = s.store.DeleteSession(ctx, id)
		return "", false
	}
	if nt.Refresh == "" {
		nt.Refresh = rec.Tok.Refresh
	}
	if nt.ID == "" {
		nt.ID = rec.Tok.ID
	}
	rec.Tok = nt
	if err := s.store.UpdateSession(ctx, id, rec, ttlOf(rec)); err != nil {
		s.o.Logger.Printf("session update: %v", err) // the token is still good for this request
	}
	return nt.Access, true
}

func fnv32(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h = (h ^ uint32(s[i])) * 16777619
	}
	return h
}

func (s *Server) proxy(w http.ResponseWriter, r *http.Request) {
	p := s.proxies[r.PathValue("service")]
	if p == nil {
		jsonErr(w, http.StatusNotFound, "not_found", "unknown API")
		return
	}
	if !s.csrfOK(r) {
		jsonErr(w, http.StatusForbidden, "csrf", "missing or invalid CSRF header")
		return
	}
	id, se := s.lookup(r)
	if se == nil {
		jsonErr(w, http.StatusUnauthorized, "unauthenticated", "sign in at /auth/login")
		return
	}
	tok, ok := s.accessToken(r.Context(), id)
	if !ok {
		s.setCookie(w, "astryon_session", "", -1)
		jsonErr(w, http.StatusUnauthorized, "session_expired", "sign in again")
		return
	}
	r = r.Clone(r.Context())
	r.Header.Set(internalAuth, "Bearer "+tok) // always overwritten here, so a browser cannot choose it
	p.ServeHTTP(w, r)
}

const internalAuth = "X-Bff-Authorization"
