package lims_test

import (
	"testing"

	"specimen-custody-graph/domain"
	"specimen-custody-graph/infra"
	"specimen-custody-graph/lims"
	"specimen-custody-graph/scerr"
	"specimen-custody-graph/testutil"
)

// keys for the test key store
var (
	keyID  = "k1"
	secret = []byte("test-secret-key")
)

func newAdapter(t *testing.T) (*lims.Adapter, *testutil.System) {
	t.Helper()
	s := testutil.NewSystem(t)
	keys := infra.NewMapKeyStore(map[string][]byte{keyID: secret})
	return lims.NewAdapter(s.Coord, keys), s
}

// recordsFor registers a mother then describes an aliquot + consumption, used
// across the JSON/CSV equivalence tests.
func sampleRecords(familyID string) []lims.Record {
	return []lims.Record{
		{Op: domain.OpRegister, FamilyID: familyID, EntityID: "m1", Source: "donor", Volume: 1000, Operator: "o", Department: "lab", Role: "operator"},
		{Op: domain.OpAliquot, FamilyID: familyID, ParentID: "m1", ChildID: "t1", Volume: 200, Operator: "o", Department: "lab", Role: "operator"},
	}
}

// TestJSONAndCSVSameCanonicalPayload verifies that equivalent JSON and CSV
// batches produce the same canonical payload (and therefore the same signature).
func TestJSONAndCSVSameCanonicalPayload(t *testing.T) {
	records := sampleRecords("fam-eq")
	jp, err := lims.CanonicalPayload(records)
	if err != nil {
		t.Fatalf("canonical json: %v", err)
	}
	cp, err := lims.CanonicalPayload(records)
	if err != nil {
		t.Fatalf("canonical csv: %v", err)
	}
	if string(jp) != string(cp) {
		t.Fatalf("canonical payloads differ:\njson: %s\ncsv:  %s", jp, cp)
	}
	sig, err := lims.Sign(secret, records)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if sig == "" {
		t.Fatal("empty signature")
	}
}

// TestEquivalentBatchImports verifies that equivalent JSON and CSV batches both
// import successfully under the shared signature vector.
func TestEquivalentBatchImports(t *testing.T) {
	records := sampleRecords("fam-eq")
	sig, err := lims.Sign(secret, records)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	jb, err := lims.EncodeJSON(records, keyID, sig)
	if err != nil {
		t.Fatalf("encode json: %v", err)
	}
	cb, err := lims.EncodeCSV(records, keyID, sig)
	if err != nil {
		t.Fatalf("encode csv: %v", err)
	}

	aj, sj := newAdapter(t)
	if _, err := aj.Import("json", jb); err != nil {
		t.Fatalf("import json: %v", err)
	}
	jDigest, _ := sj.Coord.DigestAll()

	ac, sc := newAdapter(t)
	if _, err := ac.Import("csv", cb); err != nil {
		t.Fatalf("import csv: %v", err)
	}
	cDigest, _ := sc.Coord.DigestAll()

	if jDigest != cDigest {
		t.Fatalf("json and csv imports produced different state: %s != %s", jDigest, cDigest)
	}
}

// TestUnsupportedVersion verifies an unknown protocol version is rejected.
func TestUnsupportedVersion(t *testing.T) {
	a, _ := newAdapter(t)
	// hand-craft a v2 envelope
	bad := []byte(`{"protocol_version":2,"key_id":"k1","signature":"x","records":[]}`)
	_, err := a.Import("json", bad)
	if !scerr.Is(err, scerr.CodeUnsupportedVersion) {
		t.Fatalf("expected UNSUPPORTED_VERSION, got %v", err)
	}
}

// TestMalformedJSON verifies malformed JSON is rejected with PARSE_ERROR.
func TestMalformedJSON(t *testing.T) {
	a, _ := newAdapter(t)
	_, err := a.Import("json", []byte("{not json"))
	if !scerr.Is(err, scerr.CodeParseError) {
		t.Fatalf("expected PARSE_ERROR, got %v", err)
	}
}

// TestMalformedCSV verifies malformed CSV is rejected with PARSE_ERROR.
func TestMalformedCSV(t *testing.T) {
	a, _ := newAdapter(t)
	// a CSV with an invalid quoted field
	_, err := a.Import("csv", []byte("1,k1,sig\nbad\n\"unclosed"))
	if !scerr.Is(err, scerr.CodeParseError) {
		t.Fatalf("expected PARSE_ERROR, got %v", err)
	}
}

// TestSignatureMismatch verifies a bad signature is rejected.
func TestSignatureMismatch(t *testing.T) {
	records := sampleRecords("fam-sig")
	jb, err := lims.EncodeJSON(records, keyID, "deadbeef")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	a, _ := newAdapter(t)
	_, err = a.Import("json", jb)
	if !scerr.Is(err, scerr.CodeSignatureMismatch) {
		t.Fatalf("expected SIGNATURE_MISMATCH, got %v", err)
	}
}

// TestUnknownKeyID verifies an unknown key id is treated as a signature failure.
func TestUnknownKeyID(t *testing.T) {
	records := sampleRecords("fam-k")
	sig, _ := lims.Sign(secret, records)
	jb, _ := lims.EncodeJSON(records, "unknown", sig)
	a, _ := newAdapter(t)
	_, err := a.Import("json", jb)
	if !scerr.Is(err, scerr.CodeSignatureMismatch) {
		t.Fatalf("expected SIGNATURE_MISMATCH, got %v", err)
	}
}

// TestBatchPartialInvalid verifies a batch with valid and multiple invalid
// records is wholly rejected with BATCH_PARTIAL_INVALID and a sorted per-record
// problem list, and that nothing is written.
func TestBatchPartialInvalid(t *testing.T) {
	a, s := newAdapter(t)
	// record 0: valid register
	// record 1: invalid (over-aliquot: 2000 > 1000)
	// record 2: invalid (unknown op)
	// record 3: invalid (destroy by operator -> permission)
	records := []lims.Record{
		{Op: domain.OpRegister, FamilyID: "fam-p", EntityID: "m1", Source: "s", Volume: 1000, Operator: "o", Department: "lab", Role: "operator"},
		{Op: domain.OpAliquot, FamilyID: "fam-p", ParentID: "m1", ChildID: "t1", Volume: 2000, Operator: "o", Department: "lab", Role: "operator"},
		{Op: "frobnicate", FamilyID: "fam-p", Operator: "o", Department: "lab", Role: "operator"},
		{Op: domain.OpDestroy, FamilyID: "fam-p", EntityID: "m1", DestructionID: "d1", Operator: "o", Department: "lab", Role: "operator"},
	}
	sig, err := lims.Sign(secret, records)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	jb, _ := lims.EncodeJSON(records, keyID, sig)

	seqBefore := s.Coord.AppliedSeq()
	_, err = a.Import("json", jb)
	if !scerr.Is(err, scerr.CodeBatchPartialInvalid) {
		t.Fatalf("expected BATCH_PARTIAL_INVALID, got %v", err)
	}
	se := scerr.As(err)
	if len(se.Details) < 3 {
		t.Fatalf("expected at least 3 record problems, got %d", len(se.Details))
	}
	// details must be sorted by record index
	for i := 1; i < len(se.Details); i++ {
		if se.Details[i-1].RecordIndex > se.Details[i].RecordIndex {
			t.Fatalf("details not sorted by record index: %d > %d", se.Details[i-1].RecordIndex, se.Details[i].RecordIndex)
		}
	}
	// nothing was written
	if got := s.Coord.AppliedSeq(); got != seqBefore {
		t.Fatalf("batch partial invalid wrote events: seq %d != %d", got, seqBefore)
	}
	if fam := s.Coord.Family("fam-p"); fam != nil {
		t.Fatalf("family should not exist after rejected batch")
	}
}

func TestBatchExpectedRevisionPrecondition(t *testing.T) {
	for _, format := range []string{"json", "csv"} {
		t.Run(format, func(t *testing.T) {
			a, s := newAdapter(t)
			testutil.Register(s, t, "fam-stale", "m1", "donor", "lab", 100)
			testutil.Aliquot(s, t, "fam-stale", "m1", "existing", "lab", 10)
			records := []lims.Record{
				{Op: domain.OpAliquot, FamilyID: "fam-stale", ParentID: "m1", ChildID: "stale-1", Volume: 10, ExpectedRevision: 1, Operator: "o", Department: "lab", Role: "operator"},
				{Op: domain.OpAliquot, FamilyID: "fam-stale", ParentID: "m1", ChildID: "stale-2", Volume: 10, ExpectedRevision: 1, Operator: "o", Department: "lab", Role: "operator"},
			}
			sig, err := lims.Sign(secret, records)
			if err != nil {
				t.Fatalf("sign: %v", err)
			}
			var body []byte
			if format == "json" {
				body, err = lims.EncodeJSON(records, keyID, sig)
			} else {
				body, err = lims.EncodeCSV(records, keyID, sig)
			}
			if err != nil {
				t.Fatalf("encode %s: %v", format, err)
			}

			before := s.Coord.Family("fam-stale")
			seqBefore := s.Coord.AppliedSeq()
			committedBefore := s.Store.CommittedAt()
			_, err = a.Import(format, body)
			if !scerr.Is(err, scerr.CodeBatchPartialInvalid) {
				t.Fatalf("expected BATCH_PARTIAL_INVALID, got %v", err)
			}
			se := scerr.As(err)
			if len(se.Details) != 2 || se.Details[0].Code != scerr.CodeRevisionConflict || se.Details[1].Code != scerr.CodeRevisionConflict {
				t.Fatalf("expected two REVISION_CONFLICT details, got %+v", se.Details)
			}
			after := s.Coord.Family("fam-stale")
			if after.Revision != before.Revision || after.Mother.Available != before.Mother.Available || len(after.Tubes) != len(before.Tubes) {
				t.Fatalf("rejected %s batch changed family: before=%+v after=%+v", format, before, after)
			}
			if s.Coord.AppliedSeq() != seqBefore || s.Store.CommittedAt() != committedBefore {
				t.Fatalf("rejected %s batch changed log", format)
			}
		})
	}
}

// TestBatchRetryAfterFix verifies that after fixing the invalid records the
// batch imports and produces exactly one set of events.
func TestBatchRetryAfterFix(t *testing.T) {
	a, s := newAdapter(t)
	bad := []lims.Record{
		{Op: domain.OpRegister, FamilyID: "fam-r", EntityID: "m1", Source: "s", Volume: 1000, Operator: "o", Department: "lab", Role: "operator"},
		{Op: domain.OpAliquot, FamilyID: "fam-r", ParentID: "m1", ChildID: "t1", Volume: 2000, Operator: "o", Department: "lab", Role: "operator"},
	}
	sig, _ := lims.Sign(secret, bad)
	jb, _ := lims.EncodeJSON(bad, keyID, sig)
	if _, err := a.Import("json", jb); !scerr.Is(err, scerr.CodeBatchPartialInvalid) {
		t.Fatalf("expected first import to fail: %v", err)
	}
	// fix the aliquot volume
	good := []lims.Record{
		{Op: domain.OpRegister, FamilyID: "fam-r", EntityID: "m1", Source: "s", Volume: 1000, Operator: "o", Department: "lab", Role: "operator"},
		{Op: domain.OpAliquot, FamilyID: "fam-r", ParentID: "m1", ChildID: "t1", Volume: 200, Operator: "o", Department: "lab", Role: "operator"},
	}
	sig2, _ := lims.Sign(secret, good)
	jb2, _ := lims.EncodeJSON(good, keyID, sig2)
	events, err := a.Import("json", jb2)
	if err != nil {
		t.Fatalf("import fixed: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	// exactly one set of events: family exists with one tube
	fam := s.Coord.Family("fam-r")
	if fam == nil || len(fam.Tubes) != 1 {
		t.Fatalf("unexpected family state: %+v", fam)
	}
}
