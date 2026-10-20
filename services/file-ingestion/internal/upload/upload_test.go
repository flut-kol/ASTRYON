package upload

import (
	"context"
	"errors"
	"testing"
	"time"
)

type store struct{}

func (store) CreateMultipart(_ context.Context, key string, parts int, _ time.Time) (string, []string, error) {
	u := make([]string, parts)
	for i := range u {
		u[i] = key
	}
	return "upload-1", u, nil
}

func TestCreateScopesObjectToTrustedTenant(t *testing.T) {
	s := Service{Store: store{}, Now: func() time.Time { return time.Unix(1, 0) }, MaxBytes: 100, MaxParts: 3, ContentTypes: map[string]bool{"text/csv": true}}
	r := Request{Filename: "input.csv", ContentType: "text/csv", SizeBytes: 10, Parts: 2, SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	got, err := s.Create(context.Background(), "tenant-a", r)
	if err != nil || got.ObjectKey[:8] != "tenant-a" || len(got.PartURLs) != 2 {
		t.Fatalf("%+v %v", got, err)
	}
}
func TestCreateRejectsUntrustedTenantInput(t *testing.T) {
	s := Service{Store: store{}, Now: time.Now, MaxBytes: 100, MaxParts: 3, ContentTypes: map[string]bool{"text/csv": true}}
	_, err := s.Create(context.Background(), "tenant-a", Request{TenantID: "tenant-b", Filename: "../x", ContentType: "text/csv", SizeBytes: 1, Parts: 1, SHA256: "bad"})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err=%v", err)
	}
}
