package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/parse"
	"github.com/astryon/file-ingestion/internal/profile"
)

func (w *Worker) parquetLimits(prof profile.Profile) parse.ParquetLimits {
	l := w.Parquet
	l.MaxRecordBytes = prof.MaxRecord(w.MaxRecordBytes)
	return l
}

func (w *Worker) openParquet(ctx context.Context, j jobs.Job) (io.ReaderAt, int64, string, error) {
	rs, ok := w.Source.(RandomSource)
	if !ok {
		return nil, 0, "", Permanent("parquet_unsupported_source")
	}
	return rs.OpenAt(ctx, j)
}

// mapParquetErr turns reader errors that retrying cannot fix into permanent failures.
func mapParquetErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, parse.ErrParquetRowGroupTooLarge):
		return Permanent("parquet_rowgroup_too_large")
	case errors.Is(err, parse.ErrCheckpoint):
		return Permanent("source_changed")
	case errors.Is(err, parse.ErrParquet):
		return Permanent("parquet_invalid")
	}
	return err
}

// runParquet emits one stream.row per Parquet row. Schemas are read from the footer; rows become JSON objects.
func (s *stream) runParquet(ra io.ReaderAt, size int64, lim parse.ParquetLimits, group int, row int64, emit func(row) error) error {
	return parse.ParquetFrom(ra, size, lim, group, row, func(p parse.ParquetRecord) error {
		return emit(rowOf(p))
	})
}

func rowOf(p parse.ParquetRecord) row {
	return row{Rec: p.Record, Group: p.Group, GroupEnd: p.GroupEnd}
}

// runParquet processes a Parquet object row group by row group. The sink is fed in BatchSize slices (it is
// idempotent on DedupKey), but the checkpoint advances only at row-group ends, so a restart re-reads at most one
// row group and never starts inside one.
func (w *Worker) runParquet(ctx context.Context, j jobs.Job, prof profile.Profile, scanner *classify.Scanner, seen classify.Labels) error {
	var cp parse.Checkpoint
	haveCP := len(j.Checkpoint) > 0
	if haveCP {
		if err := json.Unmarshal(j.Checkpoint, &cp); err != nil {
			return Permanent("bad_checkpoint")
		}
	}
	ra, size, fp, err := w.openParquet(ctx, j)
	if err != nil {
		return err
	}
	if !haveCP {
		if cp, err = parse.NewCheckpoint(fp); err != nil {
			return Permanent("no_fingerprint")
		}
	} else if err := cp.ValidateSource(fp); err != nil {
		return Permanent("source_changed")
	}

	st := &stream{f: formatParquet}
	var valid, invalid []Item
	var pending []parse.Record // rows since the last checkpoint
	var batchStart int         // index into pending of the first row not yet given to the sink
	var okRows, badRows int64  // counts since the last checkpoint
	sinkBatch := func() error {
		if batchStart == len(pending) {
			return nil
		}
		if err := w.accept(ctx, j, fp, valid, invalid, pending[batchStart].Number, pending[len(pending)-1].Number); err != nil {
			return err
		}
		okRows += int64(len(valid))
		badRows += int64(len(invalid))
		valid, invalid, batchStart = nil, nil, len(pending)
		return nil
	}
	checkpoint := func(group int) error {
		if err := sinkBatch(); err != nil {
			return err
		}
		next := cp
		for _, r := range pending {
			var aerr error
			if next, aerr = next.Advance(r); aerr != nil {
				return Permanent("checkpoint_gap")
			}
		}
		next.NextGroup = group + 1
		raw, _ := json.Marshal(next)
		var delta classify.Labels
		if scanner != nil {
			delta = scanner.Take()
			seen.Merge(delta)
		}
		if err := w.Store.SaveCheckpoint(ctx, j, raw, okRows, badRows, delta); err != nil {
			return err
		}
		w.M.Batches.Add(1)
		w.M.Records.Add(okRows)
		w.M.InvalidRecords.Add(badRows)
		cp, pending, batchStart, okRows, badRows = next, nil, 0, 0, 0
		return nil
	}
	err = st.runParquet(ra, size, w.parquetLimits(prof), cp.NextGroup, cp.NextOffset, func(rw row) error {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		r := rw.Rec
		if prof.MaxRecords > 0 && st.f.dataNumber(r) > prof.MaxRecords {
			return Permanent(profile.CodeRowLimit)
		}
		if r.Err == nil {
			if cerr := prof.CheckRecord(r.Raw); cerr != nil {
				r.Err = cerr
			}
		}
		if scanner != nil && r.Err == nil {
			rw.Rec = r
			st.scan(scanner, rw)
		}
		key, kerr := r.DedupKey(fp)
		if kerr != nil {
			return Permanent("dedup_key")
		}
		it := Item{Record: r, DedupKey: key}
		if r.Err != nil {
			invalid = append(invalid, it)
		} else {
			valid = append(valid, it)
		}
		pending = append(pending, r)
		if rw.GroupEnd {
			return checkpoint(rw.Group)
		}
		if len(pending)-batchStart >= w.BatchSize {
			return sinkBatch()
		}
		return nil
	})
	return mapParquetErr(err)
}
