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
	Logger         *log.Logger
}

type pending struct {
	verifier, nonce string
	exp             time.Time
}

type session struct {
	mu      sync.Mutex // serialises refresh
	tok     Tokens
	sub     string
	created time.Time
}

// Server is the BFF http.Handler.
type Server struct {
	o        Options
	oc       *oidcClient
	mux      *http.ServeMux
	mu       sync.Mutex
	pend     map[string]pending
	sessions map[string]*session
	proxies  map[string]*httputil.ReverseProxy
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
	s := &Server{o: o, oc: oc, mux: http.NewServeMux(), pend: map[string]pending{}, sessions: map[string]*session{}, proxies: map[string]*httputil.ReverseProxy{}}
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
	s.mu.Lock()
	for k, p := range s.pend { // bounded: drop expired, refuse when still full
		if now.After(p.exp) {
			delete(s.pend, k)
		}
	}
	if len(s.pend) >= s.o.MaxSessions {
		s.mu.Unlock()
		jsonErr(w, http.StatusServiceUnavailable, "busy", "too many logins in progress")
		return
	}
	s.pend[state] = pending{verifier, nonce, now.Add(s.o.LoginTimeout)}
	s.mu.Unlock()
	s.setCookie(w, "astryon_login", state, int(s.o.LoginTimeout.Seconds()))
	http.Redirect(w, r, s.oc.authURL(state, nonce, verifier), http.StatusFound)
}

func (s *Server) callback(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	state := q.Get("state")
	c, err := r.Cookie(s.cookieName("astryon_login"))
	s.setCookie(w, "astryon_login", "", -1)
	s.mu.Lock()
	p, ok := s.pend[state]
	delete(s.pend, state) // single use
	s.mu.Unlock()
	if err != nil || state == "" || subtle.ConstantTimeCompare([]byte(c.Value), []byte(state)) != 1 || !ok || s.oc.now().After(p.exp) {
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
	tok, err := s.oc.exchange(r.Context(), code, p.verifier)
	if err != nil {
		s.o.Logger.Printf("code exchange failed: %v", err)
		jsonErr(w, http.StatusBadGateway, "token_exchange_failed", "could not complete the login")
		return
	}
	sub := ""
	if tok.ID != "" {
		if sub, err = s.oc.idClaims(tok.ID, p.nonce); err != nil {
			s.o.Logger.Printf("id_token rejected: %v", err)
			jsonErr(w, http.StatusUnauthorized, "invalid_id_token", "identity token was rejected")
			return
		}
	}
	id := randString(32)
	s.mu.Lock()
	if len(s.sessions) >= s.o.MaxSessions {
		s.evictOldestLocked()
	}
	s.sessions[id] = &session{tok: tok, sub: sub, created: s.oc.now()}
	s.mu.Unlock()
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

func (s *Server) evictOldestLocked() {
	var oldest string
	var t time.Time
	for k, v := range s.sessions {
		if oldest == "" || v.created.Before(t) {
			oldest, t = k, v.created
		}
	}
	delete(s.sessions, oldest)
}

func (s *Server) lookup(r *http.Request) (string, *session) {
	c, err := r.Cookie(s.cookieName("astryon_session"))
	if err != nil {
		return "", nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	se := s.sessions[c.Value]
	if se != nil && s.oc.now().Sub(se.created) > s.o.SessionMaxAge {
		delete(s.sessions, c.Value)
		return "", nil
	}
	return c.Value, se
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	_, se := s.lookup(r)
	w.Header().Set("Content-Type", "application/json")
	if se == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"authenticated": false})
		return
	}
	se.mu.Lock()
	defer se.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{"authenticated": true, "subject": se.sub, "session_expires_at": se.created.Add(s.o.SessionMaxAge).UTC()})
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
	return o == "" || s.o.PublicOrigin == "" || o == s.o.PublicOrigin
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if !s.csrfOK(r) {
		jsonErr(w, http.StatusForbidden, "csrf", "missing or invalid CSRF header")
		return
	}
	id, se := s.lookup(r)
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
	s.setCookie(w, "astryon_session", "", -1)
	out := map[string]any{"logged_out": true}
	if se != nil && s.oc.ep.EndSession != "" {
		q := url.Values{"client_id": {s.oc.cfg.ClientID}}
		if se.tok.ID != "" {
			q.Set("id_token_hint", se.tok.ID)
		}
		if s.o.PublicOrigin != "" {
			q.Set("post_logout_redirect_uri", s.o.PublicOrigin+s.o.AfterLoginPath)
		}
		out["logout_url"] = s.oc.ep.EndSession + "?" + q.Encode()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// accessToken returns a token valid for at least 30 s, refreshing under the session lock.
func (s *Server) accessToken(ctx context.Context, id string, se *session) (string, bool) {
	se.mu.Lock()
	defer se.mu.Unlock()
	if se.tok.Expires.After(s.oc.now().Add(30 * time.Second)) {
		return se.tok.Access, true
	}
	if se.tok.Refresh == "" {
		return "", false
	}
	nt, err := s.oc.refresh(ctx, se.tok.Refresh)
	if err != nil {
		s.o.Logger.Printf("refresh failed, session dropped: %v", err)
		s.mu.Lock()
		delete(s.sessions, id)
		s.mu.Unlock()
		return "", false
	}
	if nt.Refresh == "" {
		nt.Refresh = se.tok.Refresh
	}
	if nt.ID == "" {
		nt.ID = se.tok.ID
	}
	se.tok = nt
	return nt.Access, true
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
	tok, ok := s.accessToken(r.Context(), id, se)
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
