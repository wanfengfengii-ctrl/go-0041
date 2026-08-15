package scerr

import (
	"errors"
	"testing"
)

func TestNewAndFields(t *testing.T) {
	e := New(CodeRevisionConflict, "stale").
		WithOperation("aliquot").
		WithEntity("t1").
		WithField("volume").
		WithRecord(3).
		WithRetryable(true)
	if e.Code != CodeRevisionConflict {
		t.Fatalf("code = %s", e.Code)
	}
	if e.Operation != "aliquot" || e.EntityID != "t1" || e.Field != "volume" || e.RecordIndex != 3 || !e.Retryable {
		t.Fatalf("fields: %+v", e)
	}
	if e.Error() == "" {
		t.Fatal("empty error string")
	}
}

func TestLogOffsetFields(t *testing.T) {
	e := New(CodeLogTruncated, "trunc").WithLogOffset(128, 24, 7)
	if e.LogOffset != 128 || e.ExpectedLen != 24 || e.LastSeq != 7 {
		t.Fatalf("log fields: %+v", e)
	}
}

func TestDetailsSorting(t *testing.T) {
	d := []Detail{
		{RecordIndex: 2, Code: CodeQuantityViolation, Message: "b"},
		{RecordIndex: 0, Code: CodeParseError, Message: "a"},
		{RecordIndex: 1, Code: CodePermissionDenied, Message: "c"},
	}
	e := New(CodeBatchPartialInvalid, "batch").WithDetails(d)
	if e.Details[0].RecordIndex != 0 || e.Details[2].RecordIndex != 2 {
		t.Fatalf("details not sorted: %+v", e.Details)
	}
}

func TestAsAndIs(t *testing.T) {
	e := New(CodePermissionDenied, "denied")
	if !Is(e, CodePermissionDenied) {
		t.Fatal("Is should match")
	}
	if Is(e, CodeRevisionConflict) {
		t.Fatal("Is should not match")
	}
	if As(errors.New("plain")) != nil {
		t.Fatal("As should return nil for non-scerr")
	}
	if As(nil) != nil {
		t.Fatal("As(nil) should be nil")
	}
}

func TestHTTPStatusMapping(t *testing.T) {
	cases := map[Code]int{
		CodeParseError:          400,
		CodeUnsupportedVersion:  400,
		CodeNotFound:            404,
		CodeSignatureMismatch:   403,
		CodePermissionDenied:    403,
		CodeRevisionConflict:    409,
		CodeConflict:            409,
		CodeTerminalState:       409,
		CodeQuantityViolation:   422,
		CodeBatchPartialInvalid: 422,
		CodeLogTruncated:        500,
		CodeLogCorrupt:          500,
		CodeStorage:             500,
	}
	for code, want := range cases {
		if got := HTTPStatus(code); got != want {
			t.Errorf("%s: got %d, want %d", code, got, want)
		}
	}
}

func TestUnwrap(t *testing.T) {
	inner := errors.New("root")
	e := New(CodeStorage, "msg").WithCause(inner)
	if !errors.Is(e, inner) {
		t.Fatal("Unwrap did not expose cause")
	}
}
