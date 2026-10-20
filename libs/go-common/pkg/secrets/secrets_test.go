package secrets

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSecretRedaction(t *testing.T) {
	s := New("hunter2")
	var buf bytes.Buffer
	slog.New(slog.NewTextHandler(&buf, nil)).Info("x", "s", s)
	j, _ := json.Marshal(map[string]Secret{"k": s})
	all := fmt.Sprintf("%v %+v %#v %s %q", s, s, s, s, s) + buf.String() + string(j)
	if strings.Contains(all, "hunter2") {
		t.Fatalf("leak: %s", all)
	}
	if s.Reveal() != "hunter2" {
		t.Fatal("reveal")
	}
}

func TestEnvAndFile(t *testing.T) {
	t.Setenv("PGPW", "abc")
	r := DefaultResolver(nil)
	s, err := Resolve(context.Background(), r, "env:PGPW")
	if err != nil || s.Reveal() != "abc" {
		t.Fatal(err)
	}
	if _, err := r.Resolve(context.Background(), "env:NOPE_X"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "s")
	os.WriteFile(p, []byte("v1\n"), 0o644)
	var buf bytes.Buffer
	fp := NewFileProvider(slog.New(slog.NewTextHandler(&buf, nil)))
	r = NewResolver(map[string]Provider{"file": fp})
	s, _ = r.Resolve(context.Background(), "file:"+p)
	if s.Reveal() != "v1" || !strings.Contains(buf.String(), "world-readable") {
		t.Fatalf("got %q log %q", s.Reveal(), buf.String())
	}
	os.WriteFile(p, []byte("v2\r\n"), 0o600)
	s, _ = r.Resolve(context.Background(), "file:"+p)
	if s.Reveal() != "v2" {
		t.Fatal("no rotation")
	}
	if _, err := r.Resolve(context.Background(), "bogus:x"); err == nil {
		t.Fatal("want error")
	}
	if _, err := r.Resolve(context.Background(), "noscheme"); err == nil {
		t.Fatal("want error")
	}
}

func TestVaultStaticTokenAndCache(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Vault-Token") != "tok" {
			w.WriteHeader(403)
			return
		}
		if r.URL.Path != "/v1/secret/data/astryon/pg" {
			w.WriteHeader(404)
			return
		}
		atomic.AddInt32(&hits, 1)
		fmt.Fprint(w, `{"data":{"data":{"password":"s3cr3t"}}}`)
	}))
	defer srv.Close()
	v, err := NewVaultProvider(VaultConfig{Addr: srv.URL, Token: "tok", CacheTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		s, err := v.Get(context.Background(), "secret/astryon/pg#password")
		if err != nil || s.Reveal() != "s3cr3t" {
			t.Fatal(err)
		}
	}
	if hits != 1 {
		t.Fatalf("hits=%d", hits)
	}
	if _, err := v.Get(context.Background(), "secret/astryon/pg#nokey"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := v.Get(context.Background(), "secret/other#x"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := v.Get(context.Background(), "bad"); err == nil {
		t.Fatal("want error")
	}
}

func TestVaultK8sAuthNoLeakInErrors(t *testing.T) {
	var logins int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/auth/kubernetes/login":
			atomic.AddInt32(&logins, 1)
			var b map[string]string
			json.NewDecoder(r.Body).Decode(&b)
			if b["role"] != "astryon" || b["jwt"] != "jwt-value" {
				w.WriteHeader(403)
				return
			}
			fmt.Fprint(w, `{"auth":{"client_token":"k8stok","lease_duration":3600}}`)
		case "/v1/kv/data/x":
			if r.Header.Get("X-Vault-Token") != "k8stok" {
				w.WriteHeader(403)
				return
			}
			fmt.Fprint(w, `{"data":{"data":{"k":"v"}}}`)
		case "/v1/kv/data/boom":
			w.WriteHeader(500)
			fmt.Fprint(w, `{"errors":["LEAKME"]}`)
		}
	}))
	defer srv.Close()
	jwt := filepath.Join(t.TempDir(), "jwt")
	os.WriteFile(jwt, []byte("jwt-value\n"), 0o600)
	v, err := NewVaultProvider(VaultConfig{Addr: srv.URL, K8sRole: "astryon", K8sJWTPath: jwt, CacheTTL: -1})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if s, err := v.Get(context.Background(), "kv/x#k"); err != nil || s.Reveal() != "v" {
			t.Fatal(err)
		}
	}
	if logins != 1 {
		t.Fatalf("logins=%d", logins)
	}
	_, err = v.Get(context.Background(), "kv/boom#k")
	if err == nil || strings.Contains(err.Error(), "LEAKME") {
		t.Fatalf("err=%v", err)
	}
}

func TestVaultConfigErrors(t *testing.T) {
	t.Setenv("VAULT_ADDR", "")
	t.Setenv("VAULT_TOKEN", "")
	t.Setenv("VAULT_K8S_ROLE", "")
	if _, err := NewVaultProvider(VaultConfig{}); err == nil {
		t.Fatal("want error")
	}
	if _, err := NewVaultProvider(VaultConfig{Addr: "http://x"}); err == nil {
		t.Fatal("want error")
	}
}
