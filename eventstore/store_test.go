package eventstore

import (
	"encoding/binary"
	"io"
	"os"
	"testing"
	"time"

	"specimen-custody-graph/domain"
	"specimen-custody-graph/infra"
	"specimen-custody-graph/scerr"
)

func sampleEvents() []domain.Event {
	return []domain.Event{
		{Type: domain.OpRegister, FamilyID: "fam-1", Revision: 1, Timestamp: 1, EntityID: "m1", Source: "s", Volume: 1000, Kind: domain.KindMother},
		{Type: domain.OpAliquot, FamilyID: "fam-1", Revision: 2, Timestamp: 2, ParentID: "m1", ChildID: "t1", Volume: 200, Kind: domain.KindTube},
		{Type: domain.OpDestroy, FamilyID: "fam-1", Revision: 3, Timestamp: 3, EntityID: "t1", DestructionID: "d1", Volume: 200},
	}
}

func openStore(t *testing.T, syncer infra.Syncer) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir+"/events.log", syncer)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestAppendReplayRoundtrip(t *testing.T) {
	s := openStore(t, infra.RealSyncer{})
	events := sampleEvents()
	if _, err := s.Append(events); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := s.LastSeq(); got != 3 {
		t.Fatalf("last seq = %d, want 3", got)
	}
	var replayed []domain.Event
	if err := s.Replay(func(ev domain.Event) error {
		replayed = append(replayed, ev)
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 3 {
		t.Fatalf("replayed %d events, want 3", len(replayed))
	}
	if replayed[0].Seq != 1 || replayed[2].Seq != 3 {
		t.Fatalf("seqs = %d,%d,%d", replayed[0].Seq, replayed[1].Seq, replayed[2].Seq)
	}
	if replayed[1].ChildID != "t1" {
		t.Fatalf("child id = %q", replayed[1].ChildID)
	}
}

func TestAppendContinuesSequence(t *testing.T) {
	s := openStore(t, infra.RealSyncer{})
	if _, err := s.Append(sampleEvents()[:1]); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := s.Append(sampleEvents()[1:]); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := s.LastSeq(); got != 3 {
		t.Fatalf("last seq = %d, want 3", got)
	}
}

// TestAppendSyncFailureRollback verifies that a sync failure during Append
// rolls back the uncommitted frame so the log and lastSeq are unchanged.
func TestAppendSyncFailureRollback(t *testing.T) {
	dir := t.TempDir()
	syncer := &infra.FailSyncer{FailCount: 0, Inner: infra.RealSyncer{}}
	s, err := Open(dir+"/events.log", syncer)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	if _, err := s.Append(sampleEvents()[:1]); err != nil {
		t.Fatalf("append first: %v", err)
	}
	committedBefore := s.CommittedAt()
	// the next append's sync fails
	syncer.FailCount = 1
	_, err = s.Append(sampleEvents()[1:2])
	if err == nil {
		t.Fatal("expected sync failure error")
	}
	se := scerr.As(err)
	if se == nil || !se.Retryable {
		t.Fatalf("expected retryable storage error, got %v", err)
	}
	if got := s.LastSeq(); got != 1 {
		t.Fatalf("last seq = %d, want 1 after rollback", got)
	}
	if got := s.CommittedAt(); got != committedBefore {
		t.Fatalf("committed at = %d, want %d after rollback", got, committedBefore)
	}
	// the file must not contain the rolled-back frame: a fresh open reads only 1
	s2, err := Open(s.Path(), infra.RealSyncer{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if got := s2.LastSeq(); got != 1 {
		t.Fatalf("reopened last seq = %d, want 1", got)
	}
}

// truncateAt cuts the file to n bytes and re-syncs.
func truncateAt(t *testing.T, path string, n int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open for truncate: %v", err)
	}
	if err := f.Truncate(n); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	_ = f.Sync()
	_ = f.Close()
}

// frameOffsets returns the byte offsets where each frame starts.
func frameOffsets(t *testing.T, path string) []int64 {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var offs []int64
	off := int64(0)
	for off < int64(len(data)) {
		offs = append(offs, off)
		if int(off)+FrameHeaderSize > len(data) {
			break
		}
		bodyLen := int64(data[off+12])<<24 | int64(data[off+13])<<16 | int64(data[off+14])<<8 | int64(data[off+15])
		off += FrameHeaderSize + bodyLen
	}
	return offs
}

// setSeqAt overwrites the sequence number (big-endian uint64 at header bytes
// 4:12) of the frame starting at off, without recomputing any digest or CRC.
// This simulates header tampering that the checksum chain alone cannot detect
// on the last frame.
func setSeqAt(t *testing.T, path string, off int64, seq uint64) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	binary.BigEndian.PutUint64(data[off+4:off+12], seq)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// TestLogTruncatedInBody truncates the log partway through a frame body and
// expects Replay to return LOG_TRUNCATED with the last valid sequence.
func TestLogTruncatedInBody(t *testing.T) {
	s := openStore(t, infra.RealSyncer{})
	if _, err := s.Append(sampleEvents()); err != nil {
		t.Fatalf("append: %v", err)
	}
	offs := frameOffsets(t, s.Path())
	// cut into the body of frame 3 (start of frame 3 + header + 1 byte)
	cutAt := offs[2] + int64(FrameHeaderSize) + 1
	truncateAt(t, s.Path(), cutAt)

	err := s.Replay(func(ev domain.Event) error { return nil })
	if err == nil || err == io.EOF {
		t.Fatal("expected LOG_TRUNCATED")
	}
	se := scerr.As(err)
	if se == nil || se.Code != scerr.CodeLogTruncated {
		t.Fatalf("expected LOG_TRUNCATED, got %v", err)
	}
	if se.LastSeq != 2 {
		t.Fatalf("last seq = %d, want 2", se.LastSeq)
	}
	if se.LogOffset <= 0 {
		t.Fatalf("expected positive offset, got %d", se.LogOffset)
	}
}

// TestLogTruncatedInHeader truncates the log partway through a frame header.
func TestLogTruncatedInHeader(t *testing.T) {
	s := openStore(t, infra.RealSyncer{})
	if _, err := s.Append(sampleEvents()); err != nil {
		t.Fatalf("append: %v", err)
	}
	offs := frameOffsets(t, s.Path())
	// leave only 4 bytes of frame 3's header
	cutAt := offs[2] + 4
	truncateAt(t, s.Path(), cutAt)

	err := s.Replay(func(ev domain.Event) error { return nil })
	if err == nil || err == io.EOF {
		t.Fatal("expected LOG_TRUNCATED")
	}
	se := scerr.As(err)
	if se == nil || se.Code != scerr.CodeLogTruncated {
		t.Fatalf("expected LOG_TRUNCATED, got %v", err)
	}
	if se.LastSeq != 2 {
		t.Fatalf("last seq = %d, want 2", se.LastSeq)
	}
}

// TestLogCorruptByteFlip flips a byte inside a completed frame body and
// expects LOG_CORRUPT, with no panic and no silent drop.
func TestLogCorruptByteFlip(t *testing.T) {
	s := openStore(t, infra.RealSyncer{})
	if _, err := s.Append(sampleEvents()); err != nil {
		t.Fatalf("append: %v", err)
	}
	offs := frameOffsets(t, s.Path())
	// flip a byte inside frame 2's body
	flipAt := offs[1] + int64(FrameHeaderSize) + 2
	data, err := os.ReadFile(s.Path())
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	data[flipAt] ^= 0xFF
	if err := os.WriteFile(s.Path(), data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	err = s.Replay(func(ev domain.Event) error { return nil })
	if err == nil || err == io.EOF {
		t.Fatal("expected LOG_CORRUPT")
	}
	se := scerr.As(err)
	if se == nil || se.Code != scerr.CodeLogCorrupt {
		t.Fatalf("expected LOG_CORRUPT, got %v", err)
	}
	if se.LastSeq != 1 {
		t.Fatalf("last valid seq = %d, want 1", se.LastSeq)
	}
}

// TestOpenRejectsNonContiguousSeq verifies that reopening a log whose last
// frame carries a duplicate, backward or jumped sequence number fails with
// LOG_CORRUPT and reports the last valid sequence, rather than silently
// accepting the tampered seq and continuing appends from it. The last frame is
// corrupted without recomputing any digest or CRC, which the checksum chain
// alone cannot detect.
func TestOpenRejectsNonContiguousSeq(t *testing.T) {
	cases := []struct {
		name string
		seq  uint64 // seq written into frame 3's header
	}{
		{"duplicate", 2}, // == frame 2's seq
		{"backward", 1},  // < frame 2's seq
		{"jump", 5},      // > frame 2's seq + 1
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := dir + "/events.log"
			s, err := Open(path, infra.RealSyncer{})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if _, err := s.Append(sampleEvents()); err != nil {
				t.Fatalf("append: %v", err)
			}
			offs := frameOffsets(t, path)
			// tamper with the last frame's seq without touching any checksum
			setSeqAt(t, path, offs[len(offs)-1], tc.seq)
			if err := s.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			_, err = Open(path, infra.RealSyncer{})
			if err == nil {
				t.Fatal("expected open to reject non-contiguous seq")
			}
			se := scerr.As(err)
			if se == nil || se.Code != scerr.CodeLogCorrupt {
				t.Fatalf("expected LOG_CORRUPT, got %v", err)
			}
			if se.LastSeq != 2 {
				t.Fatalf("last seq = %d, want 2 (last valid)", se.LastSeq)
			}
		})
	}
}

// TestReplayRejectsNonContiguousSeq verifies that full replay treats a
// non-contiguous sequence as log corruption and stops, retaining the last
// valid sequence, instead of replaying the tampered frame.
func TestReplayRejectsNonContiguousSeq(t *testing.T) {
	s := openStore(t, infra.RealSyncer{})
	if _, err := s.Append(sampleEvents()); err != nil {
		t.Fatalf("append: %v", err)
	}
	offs := frameOffsets(t, s.Path())
	// jump the last frame's seq from 3 to 5 without recomputing any checksum
	setSeqAt(t, s.Path(), offs[len(offs)-1], 5)

	err := s.Replay(func(ev domain.Event) error { return nil })
	if err == nil || err == io.EOF {
		t.Fatal("expected replay to reject non-contiguous seq")
	}
	se := scerr.As(err)
	if se == nil || se.Code != scerr.CodeLogCorrupt {
		t.Fatalf("expected LOG_CORRUPT, got %v", err)
	}
	if se.LastSeq != 2 {
		t.Fatalf("last seq = %d, want 2", se.LastSeq)
	}
}

// TestReplayFromRejectsNonContiguousSeq verifies that incremental replay also
// detects a non-contiguous sequence even when the tampered frame's seq is
// greater than the afterSeq filter (the contiguity check runs before the
// filter callback).
func TestReplayFromRejectsNonContiguousSeq(t *testing.T) {
	s := openStore(t, infra.RealSyncer{})
	if _, err := s.Append(sampleEvents()); err != nil {
		t.Fatalf("append: %v", err)
	}
	offs := frameOffsets(t, s.Path())
	setSeqAt(t, s.Path(), offs[len(offs)-1], 5) // frame 3 seq jumps to 5

	err := s.ReplayFrom(1, func(ev domain.Event) error { return nil })
	if err == nil || err == io.EOF {
		t.Fatal("expected replay-from to reject non-contiguous seq")
	}
	se := scerr.As(err)
	if se == nil || se.Code != scerr.CodeLogCorrupt {
		t.Fatalf("expected LOG_CORRUPT, got %v", err)
	}
	if se.LastSeq != 2 {
		t.Fatalf("last seq = %d, want 2", se.LastSeq)
	}
}

// TestContiguousSeqAccepted verifies that a normally-written log — whose
// sequences are consecutive — opens, replays and reopens without error, and
// that an append after reopen continues the sequence (no false rejection).
func TestContiguousSeqAccepted(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/events.log"
	s, err := Open(path, infra.RealSyncer{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.Append(sampleEvents()); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := s.LastSeq(); got != 3 {
		t.Fatalf("last seq = %d, want 3", got)
	}
	var n int
	if err := s.Replay(func(ev domain.Event) error { n++; return nil }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if n != 3 {
		t.Fatalf("replayed %d events, want 3", n)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// reopen continues the sequence
	s2, err := Open(path, infra.RealSyncer{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if got := s2.LastSeq(); got != 3 {
		t.Fatalf("reopened last seq = %d, want 3", got)
	}
	evs, err := s2.Append(sampleEvents()[:1])
	if err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if evs[0].Seq != 4 {
		t.Fatalf("seq after reopen = %d, want 4", evs[0].Seq)
	}
}

func TestSnapshotWriteRead(t *testing.T) {
	dir := t.TempDir()
	snaps := NewSnapshotStore(dir+"/snapshot.json", infra.RealSyncer{})
	f := domain.NewFamily("fam-1")
	_, err := f.Apply(domain.Command{Op: domain.OpRegister, Principal: domain.Principal{Department: "lab", Role: domain.RoleOperator}, FamilyID: "fam-1", EntityID: "m1", Source: "s", Volume: 1000, Now: time.Unix(1, 0)})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if err := snaps.Write(map[string]*domain.Family{"fam-1": f}, 7); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	got, err := snaps.Read()
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if got.LastSeq != 7 {
		t.Fatalf("last seq = %d, want 7", got.LastSeq)
	}
	if len(got.Families) != 1 || got.Families[0].FamilyID != "fam-1" {
		t.Fatalf("families = %+v", got.Families)
	}
	if got.Families[0].Mother == nil || got.Families[0].Mother.Available != 1000 {
		t.Fatalf("mother = %+v", got.Families[0].Mother)
	}
}

func TestSnapshotRejectsTamperedDigest(t *testing.T) {
	dir := t.TempDir()
	snaps := NewSnapshotStore(dir+"/snapshot.json", infra.RealSyncer{})
	f := domain.NewFamily("fam-1")
	_, _ = f.Apply(domain.Command{Op: domain.OpRegister, Principal: domain.Principal{Department: "lab", Role: domain.RoleOperator}, FamilyID: "fam-1", EntityID: "m1", Source: "s", Volume: 1000, Now: time.Unix(1, 0)})
	if err := snaps.Write(map[string]*domain.Family{"fam-1": f}, 1); err != nil {
		t.Fatalf("write: %v", err)
	}
	// tamper: flip a byte in the family state (after the digest field) so the
	// recomputed digest no longer matches the stored digest
	data, _ := os.ReadFile(snaps.Path())
	if len(data) < 10 {
		t.Fatalf("snapshot too short")
	}
	data[len(data)/2] ^= 0xFF
	_ = os.WriteFile(snaps.Path(), data, 0o644)
	_, err := snaps.Read()
	if err == nil {
		t.Fatal("expected error reading tampered snapshot")
	}
	se := scerr.As(err)
	if se == nil || se.Code != scerr.CodeLogCorrupt {
		t.Fatalf("expected LOG_CORRUPT, got %v", err)
	}
}
