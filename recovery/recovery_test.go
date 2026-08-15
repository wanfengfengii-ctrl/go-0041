package recovery_test

import (
	"testing"

	"specimen-custody-graph/domain"
	"specimen-custody-graph/eventstore"
	"specimen-custody-graph/infra"
	"specimen-custody-graph/recovery"
	"specimen-custody-graph/scerr"
	"specimen-custody-graph/testutil"
)

// buildPopulatedSystem creates a system with a multi-event family and returns
// it along with the expected final digest of the published state.
func buildPopulatedSystem(t *testing.T) *testutil.System {
	t.Helper()
	s := testutil.NewSystem(t)
	testutil.Register(s, t, "fam-1", "m1", "donor", "lab", 1000)
	testutil.Aliquot(s, t, "fam-1", "m1", "t1", "lab", 200)
	testutil.Aliquot(s, t, "fam-1", "t1", "t2", "lab", 50)
	// load t2, consume, unload
	if _, err := s.Coord.Submit(domain.Command{Op: domain.OpLoadStation, Principal: testutil.Op("lab"), FamilyID: "fam-1", EntityID: "t2", StationID: "stn-1"}); err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := s.Coord.Submit(domain.Command{Op: domain.OpConfirmConsumption, Principal: testutil.Op("lab"), FamilyID: "fam-1", StationID: "stn-1", Volume: 20}); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := s.Coord.Submit(domain.Command{Op: domain.OpUnload, Principal: testutil.Op("lab"), FamilyID: "fam-1", StationID: "stn-1"}); err != nil {
		t.Fatalf("unload: %v", err)
	}
	if _, err := s.Coord.Submit(domain.Command{Op: domain.OpDestroy, Principal: testutil.Approver("lab"), FamilyID: "fam-1", EntityID: "t1", DestructionID: "d1", Approver: "ap", Reason: "r"}); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	return s
}

// TestRebuildDigestsAgree verifies that replaying the whole log, recovering
// from a snapshot, and a forced rebuild all produce the same canonical digest.
func TestRebuildDigestsAgree(t *testing.T) {
	s := buildPopulatedSystem(t)

	// write a snapshot from the current published state
	if err := s.Coord.Snapshot(s.Snaps); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	logRes, err := recovery.FromLog(s.Store)
	if err != nil {
		t.Fatalf("from log: %v", err)
	}
	snapRes, err := recovery.FromSnapshot(s.Store, s.Snaps)
	if err != nil {
		t.Fatalf("from snapshot: %v", err)
	}
	forcedRes, err := recovery.ForcedRebuild(s.Store)
	if err != nil {
		t.Fatalf("forced: %v", err)
	}

	if logRes.Digest != snapRes.Digest {
		t.Fatalf("log digest %s != snapshot digest %s", logRes.Digest, snapRes.Digest)
	}
	if logRes.Digest != forcedRes.Digest {
		t.Fatalf("log digest %s != forced digest %s", logRes.Digest, forcedRes.Digest)
	}
	// snapshot path skipped the events it already had
	if !snapRes.FromSnapshot {
		t.Fatal("expected from-snapshot path")
	}
}

// TestRecoverFromScratch verifies a fresh recovery (no snapshot) replays the
// whole log and matches the published state.
func TestRecoverFromScratch(t *testing.T) {
	s := buildPopulatedSystem(t)
	pubDigest, err := s.Coord.DigestAll()
	if err != nil {
		t.Fatalf("digest published: %v", err)
	}
	logRes, err := recovery.FromLog(s.Store)
	if err != nil {
		t.Fatalf("from log: %v", err)
	}
	if logRes.Digest != pubDigest {
		t.Fatalf("recovered digest %s != published %s", logRes.Digest, pubDigest)
	}
}

// TestCorruptSnapshotFallsBackToLog verifies that a corrupt snapshot is not
// silently accepted; recovery falls back to the full log and the snapshot
// mismatch is surfaced, while the recovered state still matches the log.
func TestCorruptSnapshotFallsBackToLog(t *testing.T) {
	s := buildPopulatedSystem(t)
	if err := s.Coord.Snapshot(s.Snaps); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	pubDigest, _ := s.Coord.DigestAll()

	// corrupt the snapshot file
	if err := corruptSnapshotFile(s.Snaps.Path()); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	// snapshot read must fail (not silently accept mismatched anchor)
	_, err := s.Snaps.Read()
	if err == nil {
		t.Fatal("expected snapshot read to fail on corruption")
	}
	se := scerr.As(err)
	if se == nil || se.Code != scerr.CodeLogCorrupt {
		t.Fatalf("expected LOG_CORRUPT, got %v", err)
	}
	// FromSnapshot falls back to the full log (does not fail) and reports the skip
	res, err := recovery.FromSnapshot(s.Store, s.Snaps)
	if err != nil {
		t.Fatalf("from snapshot with corrupt snapshot: %v", err)
	}
	if !res.SnapshotSkipped {
		t.Fatal("expected SnapshotSkipped to be true")
	}
	if res.Digest != pubDigest {
		t.Fatalf("recovered digest %s != published %s", res.Digest, pubDigest)
	}
}

// TestSnapshotAnchorMismatchRejected verifies that a snapshot whose anchor
// (last_seq) is ahead of the log is not silently accepted; recovery falls back
// to the full log.
func TestSnapshotAnchorMismatchRejected(t *testing.T) {
	s := buildPopulatedSystem(t)
	// write a snapshot claiming a last_seq far ahead of the actual log
	if err := s.Snaps.Write(s.Coord.Families(), 9999); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	pubDigest, _ := s.Coord.DigestAll()
	res, err := recovery.FromSnapshot(s.Store, s.Snaps)
	if err != nil {
		t.Fatalf("from snapshot: %v", err)
	}
	if !res.SnapshotSkipped {
		t.Fatal("expected anchor mismatch to trigger SnapshotSkipped")
	}
	if res.Digest != pubDigest {
		t.Fatalf("recovered digest %s != published %s", res.Digest, pubDigest)
	}
}

// TestSnapshotIncrementalReplay verifies recovery from the latest snapshot
// replays only the events after the anchor.
func TestSnapshotIncrementalReplay(t *testing.T) {
	s := buildPopulatedSystem(t)
	if err := s.Coord.Snapshot(s.Snaps); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	snapSeq := s.Coord.AppliedSeq()
	// add more events after the snapshot
	testutil.Aliquot(s, t, "fam-1", "t2", "t3", "lab", 10)

	snapRes, err := recovery.FromSnapshot(s.Store, s.Snaps)
	if err != nil {
		t.Fatalf("from snapshot: %v", err)
	}
	if snapRes.SkippedEvents != 1 {
		t.Fatalf("skipped events = %d, want 1", snapRes.SkippedEvents)
	}
	_ = snapSeq
	pubDigest, _ := s.Coord.DigestAll()
	if snapRes.Digest != pubDigest {
		t.Fatalf("snapshot digest %s != published %s", snapRes.Digest, pubDigest)
	}
}

// TestReopenAfterRestart verifies that reopening the store (simulating a
// restart) recovers the full state.
func TestReopenAfterRestart(t *testing.T) {
	s := buildPopulatedSystem(t)
	pubDigest, _ := s.Coord.DigestAll()

	_ = s.Store.Close()
	s2, err := eventstore.Open(s.Dir+"/events.log", infra.RealSyncer{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	res, err := recovery.FromLog(s2)
	if err != nil {
		t.Fatalf("from log: %v", err)
	}
	if res.Digest != pubDigest {
		t.Fatalf("reopened digest %s != published %s", res.Digest, pubDigest)
	}
}

// TestRecoveryRejectsNonContiguousLog verifies that recovery treats a
// non-contiguous sequence number (here a jumped last-frame seq, with no
// recomputed checksum) as log corruption: it surfaces LOG_CORRUPT carrying the
// last valid sequence and publishes no state.
func TestRecoveryRejectsNonContiguousLog(t *testing.T) {
	s := buildPopulatedSystem(t)
	// buildPopulatedSystem appends 7 events (seqs 1..7); jump the last seq to 99.
	path := s.Dir + "/events.log"
	off := lastFrameOffset(t, path)
	setSeqAt(t, path, off, 99)

	res, err := recovery.FromLog(s.Store)
	if err == nil {
		t.Fatal("expected recovery to reject non-contiguous seq")
	}
	if res != nil {
		t.Fatalf("expected nil result (no published state), got %+v", res)
	}
	se := scerr.As(err)
	if se == nil || se.Code != scerr.CodeLogCorrupt {
		t.Fatalf("expected LOG_CORRUPT, got %v", err)
	}
	if se.LastSeq != 6 {
		t.Fatalf("last seq = %d, want 6 (last valid)", se.LastSeq)
	}
}
