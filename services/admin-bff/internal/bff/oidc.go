// Package bff is the Admin UI backend-for-frontend: OIDC Authorization Code + PKCE S256 against AAS, server-side
// token storage behind an HttpOnly session cookie, and a reverse proxy that adds the access token to API calls.
package bff

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Config configures the OIDC client. Client authentication is private_key_jwt (ES256, KeyFile) or, for dev,
// ClientSecret (client_secret_basic). Neither value is ever logged.
type Config struct {
	BaseURL      string // AAS realm URL; endpoints come from its discovery document
	ClientID     string
	ClientSecret string
	KeyFile      string // PEM EC P-256 private key
	KeyID        string
	RedirectURL  string // exact registered callback, e.g. https://admin.example/auth/callback
	Scopes       []string
	HTTPClient   *http.Client
	Now          func() time.Time
}

type endpoints struct {
	Issuer     string `json:"issuer"`
	Auth       string `json:"authorization_endpoint"`
	Token      string `json:"token_endpoint"`
	EndSession string `json:"end_session_endpoint"`
}

type oidcClient struct {
	cfg Config
	ep  endpoints
	key *ecdsa.PrivateKey
	kid string
	hc  *http.Client
	now func() time.Time
}

// Tokens is the parsed token endpoint response.
type Tokens struct {
	Access, Refresh, ID string
	Expires             time.Time
}

func newOIDC(ctx context.Context, cfg Config) (*oidcClient, error) {
	if cfg.BaseURL == "" || cfg.ClientID == "" || cfg.RedirectURL == "" {
		return nil, errors.New("bff: BaseURL, ClientID and RedirectURL are required")
	}
	if (cfg.ClientSecret == "") == (cfg.KeyFile == "") {
		return nil, errors.New("bff: set exactly one of ClientSecret (dev) or KeyFile (private_key_jwt)")
	}
	if len(cfg.Scopes) == 0 {
		cfg.Scopes = []string{"openid"}
	}
	c := &oidcClient{cfg: cfg, hc: cfg.HTTPClient, now: cfg.Now}
	if c.hc == nil {
		c.hc = &http.Client{Timeout: 10 * time.Second}
	}
	if c.now == nil {
		c.now = time.Now
	}
	if cfg.KeyFile != "" {
		k, err := loadKey(cfg.KeyFile)
		if err != nil {
			return nil, err
		}
		c.key, c.kid = k, cfg.KeyID
		if c.kid == "" {
			c.kid = thumbprint(&k.PublicKey)
		}
	}
	var ep endpoints
	if err := c.getJSON(ctx, strings.TrimSuffix(cfg.BaseURL, "/")+"/.well-known/openid-configuration", &ep); err != nil {
		return nil, fmt.Errorf("bff: discovery: %w", err)
	}
	base, _ := url.Parse(cfg.BaseURL)
	for name, v := range map[string]string{"authorization_endpoint": ep.Auth, "token_endpoint": ep.Token} {
		u, err := url.Parse(v)
		if err != nil || u.Host != base.Host || u.Scheme != base.Scheme {
			return nil, fmt.Errorf("bff: discovery %s %q is not same-origin as %s", name, v, cfg.BaseURL) // assertions must not go elsewhere
		}
	}
	c.ep = ep
	return c, nil
}

func (c *oidcClient) getJSON(ctx context.Context, u string, dst any) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	res, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("status %d", res.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(dst)
}

func randString(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b64u(b)
}

func b64u(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func challenge(verifier string) string { h := sha256.Sum256([]byte(verifier)); return b64u(h[:]) }

func (c *oidcClient) authURL(state, nonce, verifier string) string {
	q := url.Values{"response_type": {"code"}, "client_id": {c.cfg.ClientID}, "redirect_uri": {c.cfg.RedirectURL},
		"scope": {strings.Join(c.cfg.Scopes, " ")}, "state": {state}, "nonce": {nonce},
		"code_challenge": {challenge(verifier)}, "code_challenge_method": {"S256"}}
	return c.ep.Auth + "?" + q.Encode()
}

func (c *oidcClient) exchange(ctx context.Context, code, verifier string) (Tokens, error) {
	return c.tokenRequest(ctx, url.Values{"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {c.cfg.RedirectURL}, "code_verifier": {verifier}})
}

func (c *oidcClient) refresh(ctx context.Context, refresh string) (Tokens, error) {
	return c.tokenRequest(ctx, url.Values{"grant_type": {"refresh_token"}, "refresh_token": {refresh}})
}

func (c *oidcClient) tokenRequest(ctx context.Context, form url.Values) (Tokens, error) {
	form.Set("client_id", c.cfg.ClientID)
	hdr := http.Header{"Content-Type": {"application/x-www-form-urlencoded"}, "Accept": {"application/json"}}
	if c.key != nil {
		form.Set("client_assertion_type", "urn:ietf:params:oauth:client-assertion-type:jwt-bearer")
		a, err := c.assertion()
		if err != nil {
			return Tokens{}, err
		}
		form.Set("client_assertion", a)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.ep.Token, strings.NewReader(form.Encode()))
	req.Header = hdr
	if c.cfg.ClientSecret != "" {
		req.SetBasicAuth(url.QueryEscape(c.cfg.ClientID), url.QueryEscape(c.cfg.ClientSecret))
	}
	res, err := c.hc.Do(req)
	if err != nil {
		return Tokens{}, err
	}
	defer res.Body.Close()
	var body struct {
		Access  string `json:"access_token"`
		Refresh string `json:"refresh_token"`
		ID      string `json:"id_token"`
		Expires int64  `json:"expires_in"`
		Type    string `json:"token_type"`
		Err     string `json:"error"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&body); err != nil {
		return Tokens{}, fmt.Errorf("token endpoint: status %d, bad body", res.StatusCode)
	}
	if res.StatusCode != 200 || body.Access == "" {
		return Tokens{}, fmt.Errorf("token endpoint: status %d error %q", res.StatusCode, body.Err) // never echo the body
	}
	if !strings.EqualFold(body.Type, "bearer") {
		return Tokens{}, fmt.Errorf("token endpoint: unsupported token_type %q (DPoP-bound tokens are not supported)", body.Type)
	}
	if body.Expires <= 0 {
		body.Expires = 60
	}
	return Tokens{Access: body.Access, Refresh: body.Refresh, ID: body.ID, Expires: c.now().Add(time.Duration(body.Expires) * time.Second)}, nil
}

func (c *oidcClient) assertion() (string, error) {
	hdr, _ := json.Marshal(map[string]string{"alg": "ES256", "typ": "JWT", "kid": c.kid})
	now := c.now()
	cl, _ := json.Marshal(map[string]any{"iss": c.cfg.ClientID, "sub": c.cfg.ClientID, "aud": c.ep.Token, "jti": randString(16),
		"iat": now.Unix(), "exp": now.Add(60 * time.Second).Unix()})
	signed := b64u(hdr) + "." + b64u(cl)
	h := sha256.Sum256([]byte(signed))
	r, s, err := ecdsa.Sign(rand.Reader, c.key, h[:])
	if err != nil {
		return "", err
	}
	sig := append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...)
	return signed + "." + b64u(sig), nil
}

// idClaims checks the ID token received directly from the token endpoint over TLS (OIDC Core 3.1.3.7 allows
// TLS server validation in place of a signature): issuer, audience, expiry and nonce. It returns sub.
func (c *oidcClient) idClaims(idToken, nonce string) (string, error) {
	parts := strings.Split(idToken, ".")
	if len(parts) != 3 {
		return "", errors.New("id_token malformed")
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", errors.New("id_token malformed")
	}
	var cl struct {
		Iss   string `json:"iss"`
		Sub   string `json:"sub"`
		Aud   any    `json:"aud"`
		Exp   int64  `json:"exp"`
		Nonce string `json:"nonce"`
	}
	if json.Unmarshal(raw, &cl) != nil {
		return "", errors.New("id_token malformed")
	}
	issuer := c.ep.Issuer
	if issuer == "" {
		issuer = strings.TrimSuffix(c.cfg.BaseURL, "/")
	}
	aud := false
	switch a := cl.Aud.(type) {
	case string:
		aud = a == c.cfg.ClientID
	case []any:
		for _, x := range a {
			aud = aud || x == c.cfg.ClientID
		}
	}
	switch {
	case cl.Iss != issuer:
		return "", errors.New("id_token issuer mismatch")
	case !aud:
		return "", errors.New("id_token audience mismatch")
	case cl.Exp <= c.now().Unix():
		return "", errors.New("id_token expired")
	case cl.Nonce != nonce:
		return "", errors.New("id_token nonce mismatch")
	case cl.Sub == "":
		return "", errors.New("id_token has no sub")
	}
	return cl.Sub, nil
}

func loadKey(path string) (*ecdsa.PrivateKey, error) {
	raw, err := os.ReadFile(path) // #nosec G304 -- operator configuration
	if err != nil {
		return nil, fmt.Errorf("bff: read client key: %w", err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("bff: client key is not PEM")
	}
	var key *ecdsa.PrivateKey
	if k, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		key, _ = k.(*ecdsa.PrivateKey)
	} else if k, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		key = k
	}
	if key == nil || key.Curve != elliptic.P256() {
		return nil, errors.New("bff: client key must be an EC P-256 private key")
	}
	return key, nil
}

func thumbprint(pub *ecdsa.PublicKey) string {
	canon := fmt.Sprintf(`{"crv":"P-256","kty":"EC","x":"%s","y":"%s"}`, b64u(pub.X.FillBytes(make([]byte, 32))), b64u(pub.Y.FillBytes(make([]byte, 32))))
	h := sha256.Sum256([]byte(canon))
	return b64u(h[:])
}
