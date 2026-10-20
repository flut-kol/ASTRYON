package upload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

var ErrInvalid = errors.New("invalid multipart upload request")

type Request struct {
	TenantID, Filename, ContentType, SHA256 string
	SizeBytes                               int64
	Parts                                   int
}

type Session struct {
	UploadID, ObjectKey string
	PartURLs            []string
	ExpiresAt           time.Time
}

// Store creates a multipart session. Implementations must issue scoped,
// short-lived URLs only and must not accept a tenant from HTTP input.
type Store interface {
	CreateMultipart(context.Context, string, int, time.Time) (string, []string, error)
}

type Service struct {
	Store        Store
	Now          func() time.Time
	MaxBytes     int64
	MaxParts     int
	ContentTypes map[string]bool
}

func (s Service) Create(ctx context.Context, trustedTenant string, r Request) (Session, error) {
	if trustedTenant == "" || r.TenantID != "" || r.SizeBytes <= 0 || r.SizeBytes > s.MaxBytes ||
		r.Parts < 1 || r.Parts > s.MaxParts || !s.ContentTypes[r.ContentType] ||
		len(r.SHA256) != 64 || !safeFilename(r.Filename) {
		return Session{}, ErrInvalid
	}
	if _, err := hex.DecodeString(r.SHA256); err != nil {
		return Session{}, ErrInvalid
	}
	sum := sha256.Sum256([]byte(trustedTenant + ":" + r.Filename + ":" + r.SHA256))
	key := fmt.Sprintf("%s/uploads/%x/%s", trustedTenant, sum[:12], path.Base(r.Filename))
	expires := s.Now().Add(15 * time.Minute)
	id, urls, err := s.Store.CreateMultipart(ctx, key, r.Parts, expires)
	if err != nil || len(urls) != r.Parts {
		if err == nil {
			err = errors.New("store returned incomplete multipart URLs")
		}
		return Session{}, err
	}
	return Session{UploadID: id, ObjectKey: key, PartURLs: urls, ExpiresAt: expires}, nil
}

func safeFilename(v string) bool {
	return v != "" && path.Base(v) == v && !strings.Contains(v, "..") && !strings.Contains(v, "\\") && !strings.ContainsRune(v, 0)
}
