package export

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"
)

// UploadLister is the multipart surface of S3Store the sweeper needs.
type UploadLister interface {
	ListMultipartUploads(ctx context.Context, prefix string) ([]Upload, error)
	AbortUpload(ctx context.Context, key, uploadID string) error
}

// JobReader looks a job up by tenant and id (PGJobStore.Get).
type JobReader interface {
	Get(tenantID, id string) (Job, error)
}

// UploadSweeper aborts orphaned multipart uploads left by a worker that died (kill -9, OOM, node loss) between
// CreateMultipartUpload and Complete/Abort; S3 keeps (and bills) such parts until aborted. An upload under
// exports/<tenant>/<export_id>.<ext> is orphaned when
//   - its job is terminal (SUCCEEDED/FAILED, e.g. FAILED worker_lost after recovery) or unknown, and the upload is older
//     than Grace (a finished job has no live uploader; Grace covers clock skew and the write-after-abort race), or
//   - it is older than MaxAge (default: job timeout + margin) whatever the job says.
//
// Uploads of QUEUED/RUNNING jobs younger than MaxAge are never touched. Safe on every replica (abort is idempotent).
// The S3 lifecycle rule AbortIncompleteMultipartUpload (README) remains the independent safety net.
type UploadSweeper struct {
	Store  UploadLister
	Jobs   JobReader
	Prefix string        // default "exports/"
	Grace  time.Duration // default 1 min
	MaxAge time.Duration // default 70 min (> default job timeout of 1 h)
	Now    func() time.Time
}

func (u *UploadSweeper) now() time.Time {
	if u.Now != nil {
		return u.Now()
	}
	return time.Now()
}

// Sweep runs one pass and returns how many uploads it aborted.
func (u *UploadSweeper) Sweep(ctx context.Context) (aborted int, err error) {
	prefix, grace, maxAge := u.Prefix, u.Grace, u.MaxAge
	if prefix == "" {
		prefix = "exports/"
	}
	if grace <= 0 {
		grace = time.Minute
	}
	if maxAge <= 0 {
		maxAge = 70 * time.Minute
	}
	// List without a server-side prefix and filter here: MinIO's ListMultipartUploads only matches an EXACT key as prefix
	// (found live: prefix "exports/" returned nothing while the upload existed); real S3 would accept a prefix.
	all, err := u.Store.ListMultipartUploads(ctx, "")
	if err != nil {
		return 0, err
	}
	ups := all[:0:0]
	for _, up := range all {
		if strings.HasPrefix(up.Key, prefix) {
			ups = append(ups, up)
		}
	}
	var firstErr error
	if len(ups) > 0 {
		log.Printf("sweeper: %d incomplete multipart upload(s) under %s", len(ups), prefix)
	}
	for _, up := range ups {
		age := u.now().Sub(up.Initiated)
		if up.Initiated.IsZero() || age < grace {
			continue
		}
		orphan := age > maxAge
		if !orphan {
			parts := strings.Split(strings.TrimPrefix(up.Key, prefix), "/")
			if len(parts) == 2 && uuidRe.MatchString(parts[0]) {
				id := strings.TrimSuffix(parts[1], extOf(parts[1]))
				j, gerr := u.Jobs.Get(parts[0], id)
				switch {
				case errors.Is(gerr, ErrJobNotFound):
					orphan = true
				case gerr != nil:
					log.Printf("sweeper: cannot look up job %s/%s: %v (upload left)", parts[0], id, gerr)
					continue // cannot decide: leave it
				default:
					orphan = j.Status == StatusSucceeded || j.Status == StatusFailed
				}
			} // a key outside exports/<tenant>/<id>.<ext>: only MaxAge decides (handled above)
		}
		if !orphan {
			continue
		}
		if aerr := u.Store.AbortUpload(ctx, up.Key, up.UploadID); aerr != nil {
			if firstErr == nil {
				firstErr = aerr
			}
			continue
		}
		aborted++
		log.Printf("sweeper: aborted orphan multipart upload key=%s upload_id=%s age=%s", up.Key, up.UploadID, age.Round(time.Second))
	}
	return aborted, firstErr
}

func extOf(name string) string {
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		return name[i:]
	}
	return ""
}

// Run sweeps immediately and then every `every` until ctx ends.
func (u *UploadSweeper) Run(ctx context.Context, every time.Duration, wg *sync.WaitGroup) {
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		if wg != nil {
			defer wg.Done()
		}
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			sctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			if _, err := u.Sweep(sctx); err != nil && ctx.Err() == nil {
				log.Printf("sweeper: %v", err)
			}
			cancel()
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}
