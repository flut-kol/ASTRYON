package eventcontracts

import (
	"errors"
	"fmt"
	"strings"
)

// Code is a stable diagnostic error code for envelope validation.
type Code string

const (
	ErrEventIDInvalid         Code = "ERR_EVENT_ID_INVALID"
	ErrTenantIDInvalid        Code = "ERR_TENANT_ID_INVALID"
	ErrSourceIDInvalid        Code = "ERR_SOURCE_ID_INVALID"
	ErrSchemaIDInvalid        Code = "ERR_SCHEMA_ID_INVALID"
	ErrSchemaVersionInvalid   Code = "ERR_SCHEMA_VERSION_INVALID"
	ErrEventTimeMissing       Code = "ERR_EVENT_TIME_MISSING"
	ErrEventTimeNotUTC        Code = "ERR_EVENT_TIME_NOT_UTC"
	ErrEventTimeFuture        Code = "ERR_EVENT_TIME_FUTURE"
	ErrEventTimeTooOld        Code = "ERR_EVENT_TIME_TOO_OLD"
	ErrIngestedAtInvalid      Code = "ERR_INGESTED_AT_INVALID"
	ErrEntityKeyInvalid       Code = "ERR_ENTITY_KEY_INVALID"
	ErrTraceIDInvalid         Code = "ERR_TRACE_ID_INVALID"
	ErrPayloadFormatInvalid   Code = "ERR_PAYLOAD_FORMAT_INVALID"
	ErrPayloadXorObjectRef    Code = "ERR_PAYLOAD_XOR_OBJECT_REF"
	ErrPayloadTooLarge        Code = "ERR_PAYLOAD_TOO_LARGE"
	ErrPayloadMalformed       Code = "ERR_PAYLOAD_MALFORMED"
	ErrObjectRefInvalid       Code = "ERR_OBJECT_REF_INVALID"
	ErrChecksumInvalid        Code = "ERR_CHECKSUM_INVALID"
	ErrChecksumMismatch       Code = "ERR_CHECKSUM_MISMATCH"
	ErrChecksumRequired       Code = "ERR_CHECKSUM_REQUIRED"
	ErrProcessingVersionInval Code = "ERR_PROCESSING_VERSION_INVALID"
)

// Error is a single typed validation failure.
type Error struct {
	Code    Code
	Field   string
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s: %s", e.Code, e.Field, e.Message)
}

// Is lets errors.Is(err, &Error{Code: X}) match by code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

// Errors is the aggregate returned by Validate (never empty when non-nil).
type Errors []*Error

func (es Errors) Error() string {
	parts := make([]string, len(es))
	for i, e := range es {
		parts[i] = e.Error()
	}
	return strings.Join(parts, "; ")
}

// Is matches if any contained error matches target.
func (es Errors) Is(target error) bool {
	for _, e := range es {
		if e.Is(target) {
			return true
		}
	}
	return false
}

// Codes returns the codes of all failures, in order.
func (es Errors) Codes() []Code {
	out := make([]Code, len(es))
	for i, e := range es {
		out[i] = e.Code
	}
	return out
}

// HasCode reports whether err is (or contains) a validation failure with code c.
func HasCode(err error, c Code) bool {
	return errors.Is(err, &Error{Code: c})
}
