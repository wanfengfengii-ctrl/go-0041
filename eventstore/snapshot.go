package eventstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"specimen-custody-graph/domain"
	"specimen-custody-graph/infra"
	"specimen-custody-graph/scerr"
)

// SnapshotFile is the on-disk snapshot envelope. Families are stored in
// canonical (sorted) order so that two snapshots of equal state are
// byte-identical.
type SnapshotFile struct {
	LastSeq   uint64           `json:"last_seq"`
	Digest    string           `json:"digest"`
	Families  []*domain.Family `json:"families"`
	SealCount int              `json:"seal_count"`
}

// SnapshotStore reads and writes snapshot files atomically (temp file + sync +
// rename) and verifies the embedded digest on load.
type SnapshotStore struct {
	path   string
	syncer infra.Syncer
	mu     sync.Mutex
}

// NewSnapshotStore returns a SnapshotStore at path.
func NewSnapshotStore(path string, syncer infra.Syncer) *SnapshotStore {
	if syncer == nil {
		syncer = infra.RealSyncer{}
	}
	return &SnapshotStore{path: path, syncer: syncer}
}

// Write writes the snapshot atomically. The digest is computed over the
// canonical family states; LastSeq anchors the snapshot to a position in the
// log. On any failure no partial file is published (the temp file is removed).
func (s *SnapshotStore) Write(families map[string]*domain.Family, lastSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]string, 0, len(families))
	for id := range families {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	sorted := make([]*domain.Family, 0, len(ids))
	for _, id := range ids {
		sorted = append(sorted, families[id])
	}

	// compute digest over canonical states
	h, err := digestFamilies(sorted)
	if err != nil {
		return err
	}
	snap := SnapshotFile{
		LastSeq:  lastSeq,
		Digest:   h,
		Families: sorted,
	}
	sealCount := 0
	for _, f := range sorted {
		sealCount += len(f.Seals)
	}
	snap.SealCount = sealCount

	data, err := json.Marshal(snap)
	if err != nil {
		return scerr.New(scerr.CodeStorage, fmt.Sprintf("marshal snapshot: %v", err)).WithRetryable(true)
	}

	dir := filepath.Dir(s.path)
	if dir == "" {
		dir = "."
	}
	tmp, err := os.CreateTemp(dir, ".snapshot-*")
	if err != nil {
		return scerr.New(scerr.CodeStorage, fmt.Sprintf("create temp snapshot: %v", err)).WithRetryable(true)
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return scerr.New(scerr.CodeStorage, fmt.Sprintf("write snapshot: %v", err)).WithRetryable(true)
	}
	if err := s.syncer.Sync(tmp); err != nil {
		cleanup()
		return scerr.New(scerr.CodeStorage, fmt.Sprintf("sync snapshot: %v", err)).WithRetryable(true)
	}
	if err := tmp.Close(); err != nil {
		return scerr.New(scerr.CodeStorage, fmt.Sprintf("close snapshot: %v", err)).WithRetryable(true)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		_ = os.Remove(tmpName)
		return scerr.New(scerr.CodeStorage, fmt.Sprintf("rename snapshot: %v", err)).WithRetryable(true)
	}

	// fsync the directory so the rename is durable
	if d, err := os.Open(dir); err == nil {
		_ = s.syncer.Sync(d)
		_ = d.Close()
	}
	return nil
}

// Read loads and validates the snapshot. It recomputes the digest and rejects
// a snapshot whose anchor does not match rather than silently accepting it.
// Returns (nil, nil) when no snapshot file exists.
func (s *SnapshotStore) Read() (*SnapshotFile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.Open(s.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, scerr.New(scerr.CodeStorage, fmt.Sprintf("open snapshot: %v", err)).WithRetryable(true)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, scerr.New(scerr.CodeStorage, fmt.Sprintf("read snapshot: %v", err)).WithRetryable(true)
	}
	var snap SnapshotFile
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&snap); err != nil {
		return nil, scerr.New(scerr.CodeLogCorrupt, fmt.Sprintf("snapshot is not valid JSON: %v", err)).
			WithLogOffset(0, len(data), 0)
	}
	for i, fam := range snap.Families {
		if fam == nil {
			return nil, scerr.New(scerr.CodeLogCorrupt,
				fmt.Sprintf("snapshot family at index %d is null", i)).
				WithLogOffset(0, len(data), snap.LastSeq)
		}
	}
	// verify digest
	got, err := digestFamilies(snap.Families)
	if err != nil {
		return nil, err
	}
	if got != snap.Digest {
		return nil, scerr.New(scerr.CodeLogCorrupt,
			fmt.Sprintf("snapshot digest mismatch: stored %s computed %s", snap.Digest, got)).
			WithLogOffset(0, len(data), snap.LastSeq)
	}
	// verify seal count
	sealCount := 0
	for _, fam := range snap.Families {
		sealCount += len(fam.Seals)
	}
	if sealCount != snap.SealCount {
		return nil, scerr.New(scerr.CodeLogCorrupt,
			fmt.Sprintf("snapshot seal count mismatch: stored %d computed %d", snap.SealCount, sealCount)).
			WithLogOffset(0, len(data), snap.LastSeq)
	}
	return &snap, nil
}

// Path returns the snapshot file path.
func (s *SnapshotStore) Path() string { return s.path }

// DigestFamilies computes a hex SHA-256 over the canonical JSON of all
// families. Equal family sets produce equal digests. It is exported so that
// the recovery package can compute comparable digests.
func DigestFamilies(families []*domain.Family) (string, error) {
	return digestFamilies(families)
}

// digestFamilies computes a hex SHA-256 over the canonical JSON of all
// families (sorted by family id). Equal family sets produce equal digests
// regardless of input order.
func digestFamilies(families []*domain.Family) (string, error) {
	type familyDigest struct {
		FamilyID string `json:"family_id"`
		Digest   string `json:"digest"`
	}
	sorted := append([]*domain.Family(nil), families...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].FamilyID < sorted[j].FamilyID })
	items := make([]familyDigest, 0, len(sorted))
	for _, f := range sorted {
		d, err := f.Digest()
		if err != nil {
			return "", scerr.New(scerr.CodeStorage, fmt.Sprintf("digest family %s: %v", f.FamilyID, err)).WithRetryable(true)
		}
		items = append(items, familyDigest{FamilyID: f.FamilyID, Digest: d})
	}
	data, err := canonicalJSON(items)
	if err != nil {
		return "", err
	}
	return sha256hex(data), nil
}
