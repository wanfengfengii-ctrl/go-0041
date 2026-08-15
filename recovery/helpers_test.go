package recovery_test

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"

	"specimen-custody-graph/eventstore"
)

// corruptSnapshotFile flips a byte inside a family's initial_volume field so the
// recomputed digest no longer matches the stored digest.
func corruptSnapshotFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	idx := bytes.Index(data, []byte("initial_volume"))
	if idx < 0 {
		// fall back to flipping a byte in the body
		if len(data) == 0 {
			return nil
		}
		data[len(data)/2] ^= 0xFF
		return os.WriteFile(path, data, 0o644)
	}
	// flip a byte shortly after the field marker, inside the numeric value
	pos := idx + len("initial_volume") + 3
	if pos >= len(data) {
		pos = len(data) - 1
	}
	data[pos] ^= 0xFF
	return os.WriteFile(path, data, 0o644)
}

// lastFrameOffset returns the byte offset of the last complete frame in the log
// file at path.
func lastFrameOffset(t *testing.T, path string) int64 {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	off := int64(0)
	var last int64
	for off+int64(eventstore.FrameHeaderSize) <= int64(len(data)) {
		last = off
		bodyLen := int64(binary.BigEndian.Uint32(data[off+12 : off+16]))
		off += int64(eventstore.FrameHeaderSize) + bodyLen
	}
	return last
}

// setSeqAt overwrites the sequence number (big-endian uint64 at header bytes
// 4:12) of the frame starting at off, without recomputing any digest or CRC.
func setSeqAt(t *testing.T, path string, off int64, seq uint64) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	binary.BigEndian.PutUint64(data[off+4:off+12], seq)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}
}
