package parse

import "errors"

var ErrCheckpoint = errors.New("invalid NDJSON checkpoint")

// Checkpoint is the durable resume position for an immutable NDJSON object.
// It advances only in record order, after the caller has durably written the
// normalized or quarantined result.
type Checkpoint struct {
	SourceFingerprint string
	NextOffset        int64
	NextRecordNumber  int
	// Header holds the CSV column names (record 1) so a resume from a later offset can name columns without
	// re-reading the start of the object. Names only, never values. Empty for NDJSON.
	Header []string `json:",omitempty"`
}

func NewCheckpoint(sourceFingerprint string) (Checkpoint, error) {
	if sourceFingerprint == "" {
		return Checkpoint{}, ErrCheckpoint
	}
	return Checkpoint{SourceFingerprint: sourceFingerprint, NextRecordNumber: 1}, nil
}

// ValidateSource prevents resuming an offset against replaced object content.
func (c Checkpoint) ValidateSource(sourceFingerprint string) error {
	if c.SourceFingerprint == "" || c.SourceFingerprint != sourceFingerprint || c.NextOffset < 0 || c.NextRecordNumber < 1 {
		return ErrCheckpoint
	}
	return nil
}

// Advance rejects gaps and out-of-order completion so a persisted checkpoint
// can never silently skip an uncommitted record.
func (c Checkpoint) Advance(record Record) (Checkpoint, error) {
	if c.SourceFingerprint == "" || record.Offset != c.NextOffset || record.Number != c.NextRecordNumber || record.NextOffset <= record.Offset {
		return Checkpoint{}, ErrCheckpoint
	}
	c.NextOffset = record.NextOffset
	c.NextRecordNumber++
	return c, nil
}
