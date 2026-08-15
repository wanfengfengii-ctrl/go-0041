package recovery_test

import (
	"encoding/binary"
	"os"
	"testing"

	"specimen-custody-graph/domain"
	"specimen-custody-graph/eventstore"
	"specimen-custody-graph/infra"
	"specimen-custody-graph/recovery"
	"specimen-custody-graph/scerr"
	"specimen-custody-graph/testutil"
)

func regressionFrameOffsets(t *testing.T, path string) []int64 {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	var offsets []int64
	for off := int64(0); off < int64(len(data)); {
		offsets = append(offsets, off)
		if off+int64(eventstore.FrameHeaderSize) > int64(len(data)) {
			break
		}
		bodyLen := int64(binary.BigEndian.Uint32(data[off+12 : off+16]))
		off += int64(eventstore.FrameHeaderSize) + bodyLen
	}
	return offsets
}

func rewriteRegressionFrameSeq(t *testing.T, path string, offset int64, seq uint64) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log for rewrite: %v", err)
	}
	binary.BigEndian.PutUint64(data[offset+4:offset+12], seq)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("rewrite log: %v", err)
	}
}

// TestRejectsNonMonotonicLogSequence verifies that every log reader rejects
// duplicate, backward, skipped, and invalid initial sequence numbers without
// delivering the damaged frame to recovery.
func TestRejectsNonMonotonicLogSequence(t *testing.T) {
	events := []domain.Event{
		{Type: domain.OpRegister, FamilyID: "fam-1", Revision: 1, Timestamp: 1, EntityID: "m1", Source: "s", Volume: 1000, Kind: domain.KindMother},
		{Type: domain.OpAliquot, FamilyID: "fam-1", Revision: 2, Timestamp: 2, ParentID: "m1", ChildID: "t1", Volume: 200, Kind: domain.KindTube},
		{Type: domain.OpDestroy, FamilyID: "fam-1", Revision: 3, Timestamp: 3, EntityID: "t1", DestructionID: "d1", Volume: 200},
	}
	cases := []struct {
		name  string
		frame int
		seq   uint64
	}{
		{name: "normal", frame: -1},
		{name: "duplicate-middle", frame: 1, seq: 1},
		{name: "backward-last", frame: 2, seq: 1},
		{name: "jump-middle", frame: 1, seq: 5},
		{name: "unexpected-first", frame: 0, seq: 2},
		{name: "duplicate-last", frame: 2, seq: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := dir + "/events.log"
			store, err := eventstore.Open(path, infra.RealSyncer{})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if _, err := store.Append(events); err != nil {
				t.Fatalf("append: %v", err)
			}
			offsets := regressionFrameOffsets(t, path)
			if tc.frame >= 0 {
				rewriteRegressionFrameSeq(t, path, offsets[tc.frame], tc.seq)
			}

			if tc.frame < 0 {
				var replayed int
				if err := store.Replay(func(domain.Event) error { replayed++; return nil }); err != nil {
					t.Fatalf("replay normal log: %v", err)
				}
				if replayed != len(events) {
					t.Fatalf("replayed %d events, want %d", replayed, len(events))
				}
				res, err := recovery.FromLog(store)
				if err != nil || res.LastSeq != 3 {
					t.Fatalf("recover normal log: result=%+v err=%v", res, err)
				}
				if err := store.Close(); err != nil {
					t.Fatalf("close normal log: %v", err)
				}
				reopened, err := eventstore.Open(path, infra.RealSyncer{})
				if err != nil {
					t.Fatalf("reopen normal log: %v", err)
				}
				defer reopened.Close()
				if reopened.LastSeq() != 3 {
					t.Fatalf("reopened last seq = %d, want 3", reopened.LastSeq())
				}
				return
			}

			wantLast := uint64(tc.frame)
			checkCorrupt := func(label string, err error, callbacks int) {
				t.Helper()
				se := scerr.As(err)
				if se == nil || se.Code != scerr.CodeLogCorrupt {
					t.Fatalf("%s: expected LOG_CORRUPT, got %v", label, err)
				}
				if se.LastSeq != wantLast {
					t.Fatalf("%s: last seq = %d, want %d", label, se.LastSeq, wantLast)
				}
				if se.LogOffset != offsets[tc.frame] {
					t.Fatalf("%s: log offset = %d, want %d", label, se.LogOffset, offsets[tc.frame])
				}
				if callbacks >= 0 && callbacks != tc.frame {
					t.Fatalf("%s: callbacks = %d, want %d", label, callbacks, tc.frame)
				}
			}

			var replayed int
			err = store.Replay(func(domain.Event) error { replayed++; return nil })
			checkCorrupt("replay", err, replayed)
			replayed = 0
			err = store.ReplayFrom(0, func(domain.Event) error { replayed++; return nil })
			checkCorrupt("replay from", err, replayed)
			_, err = recovery.FromLog(store)
			checkCorrupt("recovery", err, -1)
			if err := store.Close(); err != nil {
				t.Fatalf("close corrupt log: %v", err)
			}
			_, err = eventstore.Open(path, infra.RealSyncer{})
			checkCorrupt("open", err, -1)
		})
	}
}

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
