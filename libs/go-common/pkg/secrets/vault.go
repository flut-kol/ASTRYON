package secrets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// VaultConfig configures VaultProvider. Zero values fall back to env:
// VAULT_ADDR, VAULT_TOKEN, VAULT_K8S_ROLE.
type VaultConfig struct {
	Addr         string        // VAULT_ADDR
	Token        string        // VAULT_TOKEN (static token)
	K8sRole      string        // VAULT_K8S_ROLE; enables Kubernetes auth when Token empty
	K8sJWTPath   string        // default /var/run/secrets/kubernetes.io/serviceaccount/token
	K8sAuthMount string        // default "kubernetes"
	Namespace    string        // optional X-Vault-Namespace
	CacheTTL     time.Duration // default 30s; <0 disables
	HTTPClient   *http.Client
}

// VaultProvider reads KV v2 secrets. Ref format: "mount/path#key", e.g.
// "secret/astryon/postgres#password".
type VaultProvider struct {
	cfg VaultConfig
	hc  *http.Client

	mu       sync.Mutex
	token    string
	tokenExp time.Time
	cache    map[string]cacheEntry
}

type cacheEntry struct {
	data map[string]any
	exp  time.Time
}

// NewVaultProvider validates config and returns a provider.
func NewVaultProvider(cfg VaultConfig) (*VaultProvider, error) {
	if cfg.Addr == "" {
		cfg.Addr = os.Getenv("VAULT_ADDR")
	}
	if cfg.Token == "" {
		cfg.Token = os.Getenv("VAULT_TOKEN")
	}
	if cfg.K8sRole == "" {
		cfg.K8sRole = os.Getenv("VAULT_K8S_ROLE")
	}
	if cfg.K8sJWTPath == "" {
		cfg.K8sJWTPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	}
	if cfg.K8sAuthMount == "" {
		cfg.K8sAuthMount = "kubernetes"
	}
	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = 30 * time.Second
	}
	if cfg.Addr == "" {
		return nil, fmt.Errorf("secrets: vault: VAULT_ADDR not set")
	}
	if cfg.Token == "" && cfg.K8sRole == "" {
		return nil, fmt.Errorf("secrets: vault: need VAULT_TOKEN or VAULT_K8S_ROLE")
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	cfg.Addr = strings.TrimRight(cfg.Addr, "/")
	return &VaultProvider{cfg: cfg, hc: hc, token: cfg.Token, cache: map[string]cacheEntry{}}, nil
}

func (v *VaultProvider) Get(ctx context.Context, ref string) (Secret, error) {
	path, key, ok := strings.Cut(ref, "#")
	mount, sub, ok2 := strings.Cut(strings.Trim(path, "/"), "/")
	if !ok || key == "" || !ok2 || mount == "" || sub == "" {
		return Secret{}, fmt.Errorf("secrets: vault: invalid ref %q (want mount/path#key)", ref)
	}
	data, err := v.read(ctx, mount, sub)
	if err != nil {
		return Secret{}, fmt.Errorf("secrets: vault %s: %w", path, err)
	}
	val, ok := data[key].(string)
	if !ok || val == "" {
		return Secret{}, fmt.Errorf("secrets: vault %s#%s: %w", path, key, ErrNotFound)
	}
	return New(val), nil
}

func (v *VaultProvider) read(ctx context.Context, mount, sub string) (map[string]any, error) {
	ck := mount + "/" + sub
	now := time.Now()
	v.mu.Lock()
	if e, ok := v.cache[ck]; ok && now.Before(e.exp) {
		v.mu.Unlock()
		return e.data, nil
	}
	v.mu.Unlock()

	for attempt := 0; attempt < 2; attempt++ {
		tok, err := v.getToken(ctx)
		if err != nil {
			return nil, err
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, v.cfg.Addr+"/v1/"+mount+"/data/"+sub, nil)
		req.Header.Set("X-Vault-Token", tok)
		if v.cfg.Namespace != "" {
			req.Header.Set("X-Vault-Namespace", v.cfg.Namespace)
		}
		resp, err := v.hc.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request failed")
		}
		var body struct {
			Data struct {
				Data map[string]any `json:"data"`
			} `json:"data"`
		}
		derr := json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		switch {
		case resp.StatusCode == http.StatusNotFound:
			return nil, ErrNotFound
		case resp.StatusCode == http.StatusForbidden && attempt == 0 && v.cfg.K8sRole != "" && v.cfg.Token == "":
			v.mu.Lock()
			v.token, v.tokenExp = "", time.Time{}
			v.mu.Unlock()
			continue
		case resp.StatusCode != http.StatusOK:
			return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
		case derr != nil || body.Data.Data == nil:
			return nil, fmt.Errorf("malformed response")
		}
		if v.cfg.CacheTTL > 0 {
			v.mu.Lock()
			v.cache[ck] = cacheEntry{data: body.Data.Data, exp: time.Now().Add(v.cfg.CacheTTL)}
			v.mu.Unlock()
		}
		return body.Data.Data, nil
	}
	return nil, fmt.Errorf("permission denied")
}

func (v *VaultProvider) getToken(ctx context.Context) (string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.token != "" && (v.tokenExp.IsZero() || time.Now().Before(v.tokenExp)) {
		return v.token, nil
	}
	jwt, err := os.ReadFile(v.cfg.K8sJWTPath)
	if err != nil {
		return "", fmt.Errorf("k8s auth: cannot read service account token")
	}
	payload, _ := json.Marshal(map[string]string{"role": v.cfg.K8sRole, "jwt": strings.TrimSpace(string(jwt))})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, v.cfg.Addr+"/v1/auth/"+v.cfg.K8sAuthMount+"/login", bytes.NewReader(payload))
	if v.cfg.Namespace != "" {
		req.Header.Set("X-Vault-Namespace", v.cfg.Namespace)
	}
	resp, err := v.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("k8s auth: request failed")
	}
	defer resp.Body.Close()
	var out struct {
		Auth struct {
			ClientToken   string `json:"client_token"`
			LeaseDuration int    `json:"lease_duration"`
		} `json:"auth"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&out) != nil || out.Auth.ClientToken == "" {
		return "", fmt.Errorf("k8s auth: login failed (status %d)", resp.StatusCode)
	}
	v.token = out.Auth.ClientToken
	if d := out.Auth.LeaseDuration; d > 0 {
		v.tokenExp = time.Now().Add(time.Duration(d) * time.Second * 4 / 5)
	} else {
		v.tokenExp = time.Time{}
	}
	return v.token, nil
}
