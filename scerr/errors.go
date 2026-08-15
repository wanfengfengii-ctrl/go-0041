// Package scerr defines the structured error contract used across the
// specimen-custody-graph service. Every failure surfaced to callers — whether
// from the command handler, the event store, the recovery tool or the LIMS
// batch adapter — is represented as a Error so that transport layers (HTTP,
// CLI) can map it to a stable status code and so that tests can assert on the
// machine-readable Code field without parsing prose.
package scerr

import (
	"errors"
	"fmt"
	"sort"
)

// Code is the machine-readable error identifier. The set below is the closed
// contract enumerated in the project plan; additional codes may be added but
// the listed codes must always be produced for their respective conditions.
type Code string

const (
	CodeParseError          Code = "PARSE_ERROR"
	CodeUnsupportedVersion  Code = "UNSUPPORTED_VERSION"
	CodeSignatureMismatch   Code = "SIGNATURE_MISMATCH"
	CodeBatchPartialInvalid Code = "BATCH_PARTIAL_INVALID"
	CodeRevisionConflict    Code = "REVISION_CONFLICT"
	CodePermissionDenied    Code = "PERMISSION_DENIED"
	CodeQuantityViolation   Code = "QUANTITY_VIOLATION"
	CodeTerminalState       Code = "TERMINAL_STATE"
	CodeLogTruncated        Code = "LOG_TRUNCATED"
	CodeLogCorrupt          Code = "LOG_CORRUPT"
	CodeStorage             Code = "STORAGE_ERROR"
	CodeNotFound            Code = "NOT_FOUND"
	CodeConflict            Code = "STATE_CONFLICT"
)

// Error is the structured error value. Fields are intentionally flat so the
// value serialises directly to JSON for HTTP responses and to text for the
// CLI. Not every field is relevant to every code; irrelevant fields are left
// at their zero value.
type Error struct {
	Code        Code     `json:"code"`
	Message     string   `json:"message"`
	Operation   string   `json:"operation,omitempty"`
	EntityID    string   `json:"entity_id,omitempty"`
	Field       string   `json:"field,omitempty"`
	RecordIndex int      `json:"record_index,omitempty"`
	LogOffset   int64    `json:"log_offset,omitempty"`
	ExpectedLen int      `json:"expected_len,omitempty"`
	LastSeq     uint64   `json:"last_seq,omitempty"`
	Retryable   bool     `json:"retryable"`
	Details     []Detail `json:"details,omitempty"`
	cause       error    `json:"-"`
}

// Detail is one record-level problem within a batch. RecordIndex is the
// zero-based position of the offending record in the input batch.
type Detail struct {
	RecordIndex int    `json:"record_index"`
	Code        Code   `json:"code"`
	Message     string `json:"message"`
	Field       string `json:"field,omitempty"`
}

// New constructs an Error with a cause.
func New(code Code, message string) *Error {
	return &Error{Code: code, Message: message}
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	msg := fmt.Sprintf("%s: %s", e.Code, e.Message)
	if e.Operation != "" {
		msg = fmt.Sprintf("%s [op=%s", msg, e.Operation)
		if e.EntityID != "" {
			msg += " entity=" + e.EntityID
		}
		msg += "]"
	}
	if len(e.Details) > 0 {
		msg += fmt.Sprintf(" (%d record problem(s))", len(e.Details))
	}
	if e.cause != nil {
		msg += ": " + e.cause.Error()
	}
	return msg
}

// Unwrap exposes the underlying cause for errors.Is / errors.As.
func (e *Error) Unwrap() error { return e.cause }

// WithOperation sets the operation name.
func (e *Error) WithOperation(op string) *Error { e.Operation = op; return e }

// WithEntity sets the entity identifier.
func (e *Error) WithEntity(id string) *Error { e.EntityID = id; return e }

// WithField sets the offending field.
func (e *Error) WithField(f string) *Error { e.Field = f; return e }

// WithRecord sets the record index.
func (e *Error) WithRecord(i int) *Error { e.RecordIndex = i; return e }

// WithLogOffset sets the log offset, expected length and last valid sequence.
func (e *Error) WithLogOffset(off int64, expected int, lastSeq uint64) *Error {
	e.LogOffset = off
	e.ExpectedLen = expected
	e.LastSeq = lastSeq
	return e
}

// WithRetryable marks the error as retryable.
func (e *Error) WithRetryable(r bool) *Error { e.Retryable = r; return e }

// WithCause attaches a cause.
func (e *Error) WithCause(c error) *Error { e.cause = c; return e }

// WithDetails attaches sorted record-level details (sorted by RecordIndex).
func (e *Error) WithDetails(d []Detail) *Error {
	cp := append([]Detail(nil), d...)
	sort.Slice(cp, func(i, j int) bool {
		if cp[i].RecordIndex != cp[j].RecordIndex {
			return cp[i].RecordIndex < cp[j].RecordIndex
		}
		return cp[i].Code < cp[j].Code
	})
	e.Details = cp
	return e
}

// As returns the *Error if err is one, else nil.
func As(err error) *Error {
	var se *Error
	if errors.As(err, &se) {
		return se
	}
	return nil
}

// Is reports whether err is an *Error with the given code.
func Is(err error, code Code) bool {
	var se *Error
	if errors.As(err, &se) {
		return se.Code == code
	}
	return false
}

// HTTPStatus maps a Code to a stable HTTP status code.
func HTTPStatus(code Code) int {
	switch code {
	case CodeNotFound:
		return 404
	case CodeParseError, CodeUnsupportedVersion:
		return 400
	case CodeSignatureMismatch, CodePermissionDenied:
		return 403
	case CodeRevisionConflict, CodeConflict, CodeTerminalState:
		return 409
	case CodeQuantityViolation, CodeBatchPartialInvalid:
		return 422
	case CodeLogTruncated, CodeLogCorrupt, CodeStorage:
		return 500
	default:
		return 500
	}
}
