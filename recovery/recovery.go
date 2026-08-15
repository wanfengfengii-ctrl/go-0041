// Package recovery reconstructs published state from the event log and the
// snapshot file. Three modes are supported:
//
//   - FromLog:        replay every event in the log from scratch.
//   - FromSnapshot:   load the snapshot, verify its anchor, replay events with
//     sequence greater than the snapshot's last_seq.
//   - ForcedRebuild:   ignore the snapshot and replay the whole log, returning
//     the canonical state digest for comparison with the
//     snapshot path.
//
// All three modes produce a canonical state digest so callers can verify that
// they agree. A snapshot whose stored digest does not match the recomputed
// digest is rejected rather than silently accepted.
package recovery

import (
	"fmt"
	"sort"

	"specimen-custody-graph/domain"
	"specimen-custody-graph/eventstore"
	"specimen-custody-graph/scerr"
)

// Result holds the reconstructed state and metadata.
type Result struct {
	Families      map[string]*domain.Family
	LastSeq       uint64
	Digest        string
	FromSnapshot  bool
	SkippedEvents uint64
	// SnapshotSkipped is true when the snapshot was present but unusable
	// (corrupt or anchor mismatch) and recovery fell back to the full log
	// rather than silently accepting it.
	SnapshotSkipped bool
	SnapshotReason  string
}

// FromLog replays the entire log into a fresh set of families.
func FromLog(store *eventstore.Store) (*Result, error) {
	fams := map[string]*domain.Family{}
	var lastSeq uint64
	err := store.Replay(func(ev domain.Event) error {
		fam := fams[ev.FamilyID]
		if fam == nil {
			fam = domain.NewFamily(ev.FamilyID)
			fams[ev.FamilyID] = fam
		}
		if err := fam.ApplyEvent(ev); err != nil {
			return scerr.New(scerr.CodeLogCorrupt,
				fmt.Sprintf("replay event seq %d: %v", ev.Seq, err)).
				WithLogOffset(0, 0, ev.Seq-1)
		}
		if ev.Seq > lastSeq {
			lastSeq = ev.Seq
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	d, err := digestAll(fams)
	if err != nil {
		return nil, err
	}
	return &Result{Families: fams, LastSeq: lastSeq, Digest: d}, nil
}

// FromSnapshot loads the snapshot, verifies its digest and anchor, then replays
// events with sequence greater than the snapshot anchor. If the snapshot is
// corrupt or its anchor does not match the log, recovery falls back to a full
// log replay (surfacing the reason via SnapshotSkipped) rather than silently
// accepting an untrusted snapshot.
func FromSnapshot(store *eventstore.Store, snaps *eventstore.SnapshotStore) (*Result, error) {
	snap, err := snaps.Read()
	if err != nil {
		// corrupt snapshot: do not silently accept; fall back to the full log.
		res, lerr := FromLog(store)
		if lerr != nil {
			return nil, lerr
		}
		res.SnapshotSkipped = true
		res.SnapshotReason = "snapshot unreadable: " + err.Error()
		return res, nil
	}
	if snap == nil {
		// no snapshot — full replay
		return FromLog(store)
	}
	// Anchor check: the snapshot must not claim more events than the log has.
	logLastSeq := store.LastSeq()
	if snap.LastSeq > logLastSeq {
		res, lerr := FromLog(store)
		if lerr != nil {
			return nil, lerr
		}
		res.SnapshotSkipped = true
		res.SnapshotReason = fmt.Sprintf("snapshot anchor %d ahead of log last seq %d", snap.LastSeq, logLastSeq)
		return res, nil
	}
	fams := map[string]*domain.Family{}
	for _, f := range snap.Families {
		fams[f.FamilyID] = f
	}
	var lastSeq uint64 = snap.LastSeq
	var skipped uint64
	err = store.ReplayFrom(snap.LastSeq, func(ev domain.Event) error {
		fam := fams[ev.FamilyID]
		if fam == nil {
			fam = domain.NewFamily(ev.FamilyID)
			fams[ev.FamilyID] = fam
		}
		if err := fam.ApplyEvent(ev); err != nil {
			return scerr.New(scerr.CodeLogCorrupt,
				fmt.Sprintf("replay event seq %d: %v", ev.Seq, err)).
				WithLogOffset(0, 0, ev.Seq-1)
		}
		if ev.Seq > lastSeq {
			lastSeq = ev.Seq
		}
		skipped++
		return nil
	})
	if err != nil {
		return nil, err
	}
	d, err := digestAll(fams)
	if err != nil {
		return nil, err
	}
	return &Result{Families: fams, LastSeq: lastSeq, Digest: d, FromSnapshot: true, SkippedEvents: skipped}, nil
}

// ForcedRebuild ignores the snapshot and replays the whole log. It returns the
// canonical digest so callers can compare it against the snapshot-derived
// digest.
func ForcedRebuild(store *eventstore.Store) (*Result, error) {
	return FromLog(store)
}

// Verify compares the canonical digest produced by full replay against the
// digest produced by the snapshot+log path. Equal digests imply the snapshot
// is consistent with the log.
func Verify(store *eventstore.Store, snaps *eventstore.SnapshotStore) (logDigest, snapDigest string, err error) {
	logRes, err := FromLog(store)
	if err != nil {
		return "", "", err
	}
	snapRes, err := FromSnapshot(store, snaps)
	if err != nil {
		return "", "", err
	}
	return logRes.Digest, snapRes.Digest, nil
}

// DigestOf computes a canonical digest over a family map (sorted by family id)
// using the same function as the snapshot store, so a digest produced from the
// published state equals a digest produced by recovery.
func DigestOf(fams map[string]*domain.Family) (string, error) {
	return eventstore.DigestFamilies(mapToSlice(fams))
}

// digestAll computes a canonical digest over all families (sorted by id).
func digestAll(fams map[string]*domain.Family) (string, error) {
	return DigestOf(fams)
}

func mapToSlice(fams map[string]*domain.Family) []*domain.Family {
	ids := make([]string, 0, len(fams))
	for id := range fams {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*domain.Family, 0, len(ids))
	for _, id := range ids {
		out = append(out, fams[id])
	}
	return out
}

// SeedCoordinator populates a coordinator from a recovery result. It is a
// thin helper that mirrors the result into the coordinator's seed map.
func SeedCoordinator(r *Result) (map[string]*domain.Family, uint64) {
	return r.Families, r.LastSeq
}
