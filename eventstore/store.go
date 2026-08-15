package eventstore

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"specimen-custody-graph/domain"
	"specimen-custody-graph/infra"
	"specimen-custody-graph/scerr"
)

// Store is the append-only event log. It is safe for concurrent use: Append
// holds a writer mutex so that sequence assignment and the committed-offset
// advance are atomic.
type Store struct {
	path        string
	mu          sync.Mutex
	f           *os.File
	syncer      infra.Syncer
	lastSeq     uint64
	lastDigest  uint32
	committedAt int64 // file offset after the last synced frame
}

// Open opens or creates the log file at path. If the file already contains
// frames they are scanned to recover lastSeq, lastDigest and committedAt so
// that subsequent appends continue the chain. A scan error (truncation or
// corruption) is returned so the caller can decide whether to rebuild.
func Open(path string, syncer infra.Syncer) (*Store, error) {
	if syncer == nil {
		syncer = infra.RealSyncer{}
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, scerr.New(scerr.CodeStorage, fmt.Sprintf("open log: %v", err)).WithRetryable(true).WithCause(err)
	}
	s := &Store{path: path, f: f, syncer: syncer}
	if err := s.scan(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

// scan reads existing frames to recover the chain state. It stops at the first
// truncated or corrupt frame and reports it as a structured error carrying the
// last valid sequence number, rather than silently dropping data. A clean EOF
// at a frame boundary is not an error.
func (s *Store) scan() error {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return scerr.New(scerr.CodeStorage, fmt.Sprintf("seek: %v", err)).WithRetryable(true)
	}
	var prevDigest uint32
	var lastGood uint64
	nextSeq := uint64(1)
	off := int64(0)
	for {
		ev, digest, err := decodeFrame(s.f, off, prevDigest, nextSeq)
		if err == io.EOF {
			break
		}
		if err != nil {
			if se := scerr.As(err); se != nil {
				se.LastSeq = lastGood
			}
			return err
		}
		lastGood = ev.Seq
		s.lastSeq = ev.Seq
		s.lastDigest = digest
		prevDigest = digest
		nextSeq++
		off, _ = s.f.Seek(0, io.SeekCurrent)
	}
	s.committedAt = off
	if _, err := s.f.Seek(0, io.SeekEnd); err != nil {
		return scerr.New(scerr.CodeStorage, fmt.Sprintf("seek end: %v", err)).WithRetryable(true)
	}
	return nil
}

// LastSeq returns the sequence number of the most recent committed frame.
func (s *Store) LastSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSeq
}

// Append writes the events as frames in a single write followed by a single
// sync. Sequence numbers are assigned consecutively starting at lastSeq+1. On
// sync failure the file is truncated back to the committed offset and the
// assigned sequences are rolled back, so the log never contains uncommitted
// frames and lastSeq is unchanged.
func (s *Store) Append(events []domain.Event) ([]domain.Event, error) {
	if len(events) == 0 {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Encode all frames relative to the current chain state.
	seq := s.lastSeq
	prevDigest := s.lastDigest
	type framed struct {
		ev   domain.Event
		data []byte
	}
	framedEvents := make([]framed, 0, len(events))
	var buf []byte
	for i := range events {
		seq++
		events[i].Seq = seq
		events[i].FamilyID = orDefault(events[i].FamilyID, events[i].FamilyID)
		body, err := json.Marshal(events[i])
		if err != nil {
			return nil, scerr.New(scerr.CodeStorage, fmt.Sprintf("marshal event: %v", err)).WithRetryable(true)
		}
		frame := encodeFrame(seq, prevDigest, body)
		prevDigest = frameDigest(frame[:FrameHeaderSize], body)
		framedEvents = append(framedEvents, framed{events[i], frame})
		buf = append(buf, frame...)
	}

	// Write at the committed offset. Because the file is opened O_APPEND the
	// write goes to the end, which equals committedAt after a clean scan.
	if _, err := s.f.Write(buf); err != nil {
		return nil, scerr.New(scerr.CodeStorage, fmt.Sprintf("write log: %v", err)).WithRetryable(true).WithCause(err)
	}
	if err := s.syncer.Sync(s.f); err != nil {
		// Roll back the uncommitted bytes so a restart never sees them.
		_ = s.f.Truncate(s.committedAt)
		_ = s.syncer.Sync(s.f)
		return nil, scerr.New(scerr.CodeStorage, fmt.Sprintf("sync log: %v", err)).WithRetryable(true).WithCause(err)
	}

	// Commit: advance the chain state.
	for _, fr := range framedEvents {
		s.lastSeq = fr.ev.Seq
	}
	s.lastDigest = prevDigest
	s.committedAt += int64(len(buf))
	return events, nil
}

// Replay reads the entire log from the beginning, invoking fn for each event
// in sequence order. It stops at the first truncated or corrupt frame and
// returns the structured error carrying the last valid sequence number.
func (s *Store) Replay(fn func(domain.Event) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return scerr.New(scerr.CodeStorage, fmt.Sprintf("seek: %v", err)).WithRetryable(true)
	}
	var prevDigest uint32
	var lastGood uint64
	nextSeq := uint64(1)
	off := int64(0)
	for {
		ev, digest, err := decodeFrame(s.f, off, prevDigest, nextSeq)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if se := scerr.As(err); se != nil {
				se.LastSeq = lastGood
			}
			return err
		}
		if err := fn(ev); err != nil {
			return err
		}
		lastGood = ev.Seq
		prevDigest = digest
		nextSeq++
		off, _ = s.f.Seek(0, io.SeekCurrent)
	}
}

// ReplayFrom reads events with sequence greater than afterSeq.
func (s *Store) ReplayFrom(afterSeq uint64, fn func(domain.Event) error) error {
	return s.Replay(func(ev domain.Event) error {
		if ev.Seq <= afterSeq {
			return nil
		}
		return fn(ev)
	})
}

// Close flushes and closes the log file.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

// Path returns the log file path.
func (s *Store) Path() string { return s.path }

// CommittedAt returns the committed file offset (mostly for tests).
func (s *Store) CommittedAt() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committedAt
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
