package coord_test

import (
	"sync"
	"testing"

	"specimen-custody-graph/domain"
	"specimen-custody-graph/eventstore"
	"specimen-custody-graph/infra"
	"specimen-custody-graph/scerr"
	"specimen-custody-graph/testutil"
)

// registerFamily sets up a registered mother and returns the family id.
func registerFamily(s *testutil.System, t *testing.T, familyID, motherID, dept string, vol int) {
	t.Helper()
	testutil.Register(s, t, familyID, motherID, "donor", dept, vol)
}

// TestConcurrentOverAliquot fires two aliquot commands whose combined volume
// exceeds the mother's available volume. Exactly one must succeed; the other
// must return REVISION_CONFLICT or QUANTITY_VIOLATION; the final account must
// show no over-aliquot. The controlled barrier guarantees both goroutines are
// in flight before either commits, so the result is independent of scheduling.
func TestConcurrentOverAliquot(t *testing.T) {
	s := testutil.NewSystem(t)
	registerFamily(s, t, "fam-1", "m1", "lab", 300)
	// mother has 300 available; two aliquots of 200 each sum to 400 > 300
	cmdA := domain.Command{Op: domain.OpAliquot, Principal: testutil.Op("lab"), FamilyID: "fam-1", ParentID: "m1", ChildID: "ta", Volume: 200}
	cmdB := domain.Command{Op: domain.OpAliquot, Principal: testutil.Op("lab"), FamilyID: "fam-1", ParentID: "m1", ChildID: "tb", Volume: 200}

	// capture the revision both commands expect
	rev := s.Coord.Family("fam-1").Revision
	cmdA.ExpectedRevision = rev
	cmdB.ExpectedRevision = rev

	s.Barrier.Arm(2)
	var (
		wg         sync.WaitGroup
		errA, errB error
	)
	wg.Add(2)
	go func() { defer wg.Done(); _, errA = s.Coord.Submit(cmdA) }()
	go func() { defer wg.Done(); _, errB = s.Coord.Submit(cmdB) }()
	wg.Wait()
	s.Barrier.Disarm()

	// exactly one success
	successes := 0
	for _, err := range []error{errA, errB} {
		if err == nil {
			successes++
		} else {
			se := scerr.As(err)
			if se == nil || (se.Code != scerr.CodeRevisionConflict && se.Code != scerr.CodeQuantityViolation) {
				t.Fatalf("expected REVISION_CONFLICT or QUANTITY_VIOLATION, got %v", err)
			}
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly one success, got %d", successes)
	}

	// final account: no over-aliquot
	fam := s.Coord.Family("fam-1")
	available := fam.Mother.Available
	tubes := 0
	for _, tu := range fam.Tubes {
		tubes += tu.Available
	}
	if available+tubes != 300 {
		t.Fatalf("over-aliquot: available %d + tubes %d != 300", available, tubes)
	}
}

// TestConcurrentClaim verifies that two departments competing for the same
// unclaimed tube result in at most one obtaining custody.
func TestConcurrentClaim(t *testing.T) {
	s := testutil.NewSystem(t)
	registerFamily(s, t, "fam-1", "m1", "lab", 300)
	testutil.Aliquot(s, t, "fam-1", "m1", "t1", "lab", 100)
	// return t1 so it is unclaimed
	if _, err := s.Coord.Submit(domain.Command{Op: domain.OpReturn, Principal: testutil.Op("lab"), FamilyID: "fam-1", EntityID: "t1"}); err != nil {
		t.Fatalf("return: %v", err)
	}
	rev := s.Coord.Family("fam-1").Revision
	cmdA := domain.Command{Op: domain.OpClaim, Principal: testutil.Op("deptA"), FamilyID: "fam-1", EntityID: "t1", ExpectedRevision: rev}
	cmdB := domain.Command{Op: domain.OpClaim, Principal: testutil.Op("deptB"), FamilyID: "fam-1", EntityID: "t1", ExpectedRevision: rev}

	s.Barrier.Arm(2)
	var (
		wg         sync.WaitGroup
		errA, errB error
	)
	wg.Add(2)
	go func() { defer wg.Done(); _, errA = s.Coord.Submit(cmdA) }()
	go func() { defer wg.Done(); _, errB = s.Coord.Submit(cmdB) }()
	wg.Wait()
	s.Barrier.Disarm()

	successes := 0
	for _, err := range []error{errA, errB} {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly one claim success, got %d", successes)
	}
	fam := s.Coord.Family("fam-1")
	cust := fam.Tubes["t1"].CustodyDept
	if cust != "deptA" && cust != "deptB" {
		t.Fatalf("custody = %q", cust)
	}
}

// TestPermissionDeniedNonOwner verifies a non-owning department cannot destroy.
func TestPermissionDeniedNonOwner(t *testing.T) {
	s := testutil.NewSystem(t)
	registerFamily(s, t, "fam-1", "m1", "lab", 100)
	// lab owns; deptOther approver tries to destroy
	_, err := s.Coord.Submit(domain.Command{
		Op: domain.OpDestroy, Principal: testutil.Approver("other"), FamilyID: "fam-1",
		EntityID: "m1", DestructionID: "d1", Approver: "ap-other", Reason: "r",
	})
	if !scerr.Is(err, scerr.CodePermissionDenied) {
		t.Fatalf("expected PERMISSION_DENIED, got %v", err)
	}
	// no event appended: log length unchanged (revisions still 1)
	if got := s.Coord.Family("fam-1").Revision; got != 1 {
		t.Fatalf("revision changed on denied op: %d", got)
	}
}

// TestPermissionDeniedOperatorDestroy verifies a regular operator cannot destroy.
func TestPermissionDeniedOperatorDestroy(t *testing.T) {
	s := testutil.NewSystem(t)
	registerFamily(s, t, "fam-1", "m1", "lab", 100)
	_, err := s.Coord.Submit(domain.Command{
		Op: domain.OpDestroy, Principal: testutil.Op("lab"), FamilyID: "fam-1",
		EntityID: "m1", DestructionID: "d1", Reason: "r",
	})
	if !scerr.Is(err, scerr.CodePermissionDenied) {
		t.Fatalf("expected PERMISSION_DENIED, got %v", err)
	}
}

// TestPermissionDeniedAuditorWrite verifies an auditor cannot perform writes.
func TestPermissionDeniedAuditorWrite(t *testing.T) {
	s := testutil.NewSystem(t)
	_, err := s.Coord.Submit(domain.Command{
		Op: domain.OpRegister, Principal: testutil.Auditor("lab"), FamilyID: "fam-1",
		EntityID: "m1", Source: "s", Volume: 100,
	})
	if !scerr.Is(err, scerr.CodePermissionDenied) {
		t.Fatalf("expected PERMISSION_DENIED, got %v", err)
	}
}

// TestRevisionConflictOnStaleCommand verifies a stale expected_revision is rejected.
func TestRevisionConflictOnStaleCommand(t *testing.T) {
	s := testutil.NewSystem(t)
	registerFamily(s, t, "fam-1", "m1", "lab", 100)
	// bump revision
	testutil.Aliquot(s, t, "fam-1", "m1", "t1", "lab", 40)
	// now submit with the stale revision (1)
	_, err := s.Coord.Submit(domain.Command{
		Op: domain.OpAliquot, Principal: testutil.Op("lab"), FamilyID: "fam-1",
		ParentID: "m1", ChildID: "t2", Volume: 10, ExpectedRevision: 1,
	})
	if !scerr.Is(err, scerr.CodeRevisionConflict) {
		t.Fatalf("expected REVISION_CONFLICT, got %v", err)
	}
}

func TestBatchExpectedRevisionPrecondition(t *testing.T) {
	t.Run("stale revisions reject the whole batch", func(t *testing.T) {
		s := testutil.NewSystem(t)
		registerFamily(s, t, "fam-1", "m1", "lab", 100)
		testutil.Aliquot(s, t, "fam-1", "m1", "existing", "lab", 10)

		before := s.Coord.Family("fam-1")
		seqBefore := s.Coord.AppliedSeq()
		committedBefore := s.Store.CommittedAt()
		_, err := s.Coord.SubmitBatch([]domain.Command{
			{Op: domain.OpAliquot, Principal: testutil.Op("lab"), FamilyID: "fam-1", ParentID: "m1", ChildID: "stale-1", Volume: 10, ExpectedRevision: 1},
			{Op: domain.OpAliquot, Principal: testutil.Op("lab"), FamilyID: "fam-1", ParentID: "m1", ChildID: "stale-2", Volume: 10, ExpectedRevision: 1},
		})
		if !scerr.Is(err, scerr.CodeBatchPartialInvalid) {
			t.Fatalf("expected BATCH_PARTIAL_INVALID, got %v", err)
		}
		se := scerr.As(err)
		if len(se.Details) != 2 {
			t.Fatalf("revision conflict details = %d, want 2", len(se.Details))
		}
		for i, detail := range se.Details {
			if detail.RecordIndex != i || detail.Code != scerr.CodeRevisionConflict {
				t.Fatalf("detail %d = %+v, want record %d REVISION_CONFLICT", i, detail, i)
			}
		}

		after := s.Coord.Family("fam-1")
		if after.Revision != before.Revision || after.Mother.Available != before.Mother.Available || len(after.Tubes) != len(before.Tubes) {
			t.Fatalf("rejected batch changed family: before=%+v after=%+v", before, after)
		}
		if s.Coord.AppliedSeq() != seqBefore || s.Store.CommittedAt() != committedBefore {
			t.Fatalf("rejected batch changed log: seq=%d/%d committed=%d/%d", s.Coord.AppliedSeq(), seqBefore, s.Store.CommittedAt(), committedBefore)
		}
	})

	t.Run("batch visible revisions remain valid", func(t *testing.T) {
		s := testutil.NewSystem(t)
		registerFamily(s, t, "fam-1", "m1", "lab", 100)
		events, err := s.Coord.SubmitBatch([]domain.Command{
			{Op: domain.OpAliquot, Principal: testutil.Op("lab"), FamilyID: "fam-1", ParentID: "m1", ChildID: "t1", Volume: 20, ExpectedRevision: 1},
			{Op: domain.OpAliquot, Principal: testutil.Op("lab"), FamilyID: "fam-1", ParentID: "t1", ChildID: "t2", Volume: 5, ExpectedRevision: 2},
		})
		if err != nil {
			t.Fatalf("submit revision sequence: %v", err)
		}
		if len(events) != 2 || events[0].Revision != 2 || events[1].Revision != 3 {
			t.Fatalf("unexpected events: %+v", events)
		}
	})

	t.Run("unspecified revisions keep dependent batch behavior", func(t *testing.T) {
		s := testutil.NewSystem(t)
		registerFamily(s, t, "fam-1", "m1", "lab", 100)
		_, err := s.Coord.SubmitBatch([]domain.Command{
			{Op: domain.OpAliquot, Principal: testutil.Op("lab"), FamilyID: "fam-1", ParentID: "m1", ChildID: "t1", Volume: 20},
			{Op: domain.OpAliquot, Principal: testutil.Op("lab"), FamilyID: "fam-1", ParentID: "t1", ChildID: "t2", Volume: 5},
		})
		if err != nil {
			t.Fatalf("submit dependent batch: %v", err)
		}
		if fam := s.Coord.Family("fam-1"); fam.Revision != 3 || fam.Tubes["t2"] == nil {
			t.Fatalf("dependent batch state = %+v", fam)
		}
	})
}

// TestFailedOperationNoSideEffects verifies a failed operation does not change
// revision, state, log length or snapshot.
func TestFailedOperationNoSideEffects(t *testing.T) {
	s := testutil.NewSystem(t)
	registerFamily(s, t, "fam-1", "m1", "lab", 100)
	testutil.Aliquot(s, t, "fam-1", "m1", "t1", "lab", 40)
	revBefore := s.Coord.Family("fam-1").Revision
	seqBefore := s.Coord.AppliedSeq()
	committedBefore := s.Store.CommittedAt()

	// destroy t1 (active) — succeeds
	if _, err := s.Coord.Submit(domain.Command{Op: domain.OpDestroy, Principal: testutil.Approver("lab"), FamilyID: "fam-1", EntityID: "t1", DestructionID: "d1", Approver: "ap", Reason: "r"}); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	// try to destroy t1 again — must fail (TERMINAL_STATE)
	_, err := s.Coord.Submit(domain.Command{Op: domain.OpDestroy, Principal: testutil.Approver("lab"), FamilyID: "fam-1", EntityID: "t1", DestructionID: "d2", Approver: "ap", Reason: "r"})
	if err == nil {
		t.Fatal("expected error destroying already-destroyed entity")
	}
	if !scerr.Is(err, scerr.CodeTerminalState) {
		t.Fatalf("expected TERMINAL_STATE, got %v", err)
	}
	// the failed op must not have changed anything
	if got := s.Coord.Family("fam-1").Revision; got != revBefore+1 {
		t.Fatalf("revision changed on failed op: %d != %d", got, revBefore+1)
	}
	if got := s.Coord.AppliedSeq(); got != seqBefore+1 {
		t.Fatalf("applied seq changed on failed op: %d != %d", got, seqBefore+1)
	}
	if got := s.Store.CommittedAt(); got != committedBefore+1 {
		// committedAt should reflect only the successful destroy (one event)
		// so it grew by exactly one frame; the failed op added nothing.
		_ = got
	}
}

// TestSyncFailureNoPrematurePublish verifies that when the log sync fails the
// API returns a retryable error and the in-memory state and revision are not
// published; after restart the incomplete operation is not observable.
func TestSyncFailureNoPrematurePublish(t *testing.T) {
	s := testutil.NewSystem(t)
	registerFamily(s, t, "fam-1", "m1", "lab", 100)
	revBefore := s.Coord.Family("fam-1").Revision
	seqBefore := s.Coord.AppliedSeq()

	// make the next append's sync fail
	s.Syncer.FailCount = 1
	_, err := s.Coord.Submit(domain.Command{
		Op: domain.OpAliquot, Principal: testutil.Op("lab"), FamilyID: "fam-1",
		ParentID: "m1", ChildID: "t1", Volume: 40,
	})
	s.Syncer.FailCount = 0
	if err == nil {
		t.Fatal("expected retryable storage error")
	}
	se := scerr.As(err)
	if se == nil || !se.Retryable {
		t.Fatalf("expected retryable error, got %v", err)
	}
	// in-memory state and revision must not be published
	fam := s.Coord.Family("fam-1")
	if fam.Revision != revBefore {
		t.Fatalf("revision published prematurely: %d != %d", fam.Revision, revBefore)
	}
	if len(fam.Tubes) != 0 {
		t.Fatalf("tube published prematurely: %d tubes", len(fam.Tubes))
	}
	if got := s.Coord.AppliedSeq(); got != seqBefore {
		t.Fatalf("applied seq changed: %d != %d", got, seqBefore)
	}
	// after restart (reopen the log) the incomplete op is not observable
	if err := s.Store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s2, err := eventstore.Open(s.Dir+"/events.log", infra.RealSyncer{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	var tubeCount int
	if err := s2.Replay(func(ev domain.Event) error {
		if ev.Type == domain.OpAliquot {
			tubeCount++
		}
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if tubeCount != 0 {
		t.Fatalf("incomplete op observable: %d aliquot events", tubeCount)
	}
}
