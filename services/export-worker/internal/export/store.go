package export

import (
	"context"
	"io"
	"time"
)

// ObjectStore receives the export stream. Put must not buffer the whole stream in memory.
type ObjectStore interface {
	// Put streams r into key and returns the number of bytes stored. On any error no object may remain.
	Put(ctx context.Context, key string, r io.Reader) (int64, error)
	// Delete removes key (best effort cleanup of partial objects); a missing key is not an error.
	Delete(ctx context.Context, key string) error
}

// Presigner issues short-lived download URLs.
type Presigner interface {
	PresignGet(key string, ttl time.Duration) (url string, expires time.Time, err error)
}
