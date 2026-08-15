// Package eventstore implements the append-only event log and the snapshot
// file used to accelerate recovery.
//
// The log is a sequence of binary frames. Each frame is:
//
//	[ 4] magic "SCG1"
//	[ 8] sequence number (uint64 big-endian) — monotonic across the log
//	[ 4] body length   (uint32 big-endian)
//	[ 4] previous-frame digest (CRC32) — chains frames; 0 for the first frame
//	[ 4] body CRC32     (CRC32-IEEE of the body bytes)
//	[ N] canonical-JSON event body (N = body length)
//
// The previous-frame digest is the CRC32 of the prior frame's header+body; a
// mismatch indicates truncation, reordering or corruption at a frame boundary.
// Append writes the frame, calls the Syncer and, on success, advances the
// committed offset. On sync failure the file is truncated back to the
// committed offset so that a restart never observes an uncommitted frame.
package eventstore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"specimen-custody-graph/domain"
	"specimen-custody-graph/scerr"
)

// FrameHeaderSize is the fixed size of a frame header in bytes.
const FrameHeaderSize = 4 + 8 + 4 + 4 + 4

const frameMagic = "SCG1"

// frameDigest returns the CRC32 of a frame's header+body for chaining.
func frameDigest(header []byte, body []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write(header)
	h.Write(body)
	return h.Sum32()
}

// encodeFrame builds a frame for the event, assigning the given sequence and
// the previous frame's digest.
func encodeFrame(seq uint64, prevDigest uint32, body []byte) []byte {
	buf := make([]byte, FrameHeaderSize+len(body))
	copy(buf[0:4], []byte(frameMagic))
	binary.BigEndian.PutUint64(buf[4:12], seq)
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(body)))
	binary.BigEndian.PutUint32(buf[16:20], prevDigest)
	binary.BigEndian.PutUint32(buf[20:24], crc32.ChecksumIEEE(body))
	copy(buf[24:], body)
	return buf
}

// decodeFrame reads one frame from f, returning the event, the frame's own
// digest (for chaining the next frame) and a structured error if the frame is
// truncated or corrupt. expectedSeq is the only valid sequence for this frame.
func decodeFrame(f *os.File, base int64, prevDigest uint32, expectedSeq uint64) (domain.Event, uint32, error) {
	header := make([]byte, FrameHeaderSize)
	n, err := io.ReadFull(f, header)
	if err == io.EOF || (err == io.ErrUnexpectedEOF && n == 0) {
		return domain.Event{}, 0, io.EOF
	}
	if err == io.ErrUnexpectedEOF {
		// truncated header
		return domain.Event{}, 0, scerr.New(scerr.CodeLogTruncated,
			fmt.Sprintf("truncated frame header at offset %d: got %d of %d bytes", base, n, FrameHeaderSize)).
			WithLogOffset(base, FrameHeaderSize, 0)
	}
	if err != nil {
		return domain.Event{}, 0, scerr.New(scerr.CodeLogCorrupt, fmt.Sprintf("read error at offset %d: %v", base, err)).
			WithLogOffset(base, 0, 0)
	}
	if string(header[0:4]) != frameMagic {
		return domain.Event{}, 0, scerr.New(scerr.CodeLogCorrupt,
			fmt.Sprintf("bad frame magic at offset %d", base)).WithLogOffset(base, 0, 0)
	}
	seq := binary.BigEndian.Uint64(header[4:12])
	bodyLen := binary.BigEndian.Uint32(header[12:16])
	prev := binary.BigEndian.Uint32(header[16:20])
	crc := binary.BigEndian.Uint32(header[20:24])
	if seq != expectedSeq {
		return domain.Event{}, 0, scerr.New(scerr.CodeLogCorrupt,
			fmt.Sprintf("non-contiguous frame sequence at offset %d: got %d, want %d", base, seq, expectedSeq)).
			WithLogOffset(base, int(bodyLen), expectedSeq-1)
	}
	if prev != prevDigest {
		return domain.Event{}, 0, scerr.New(scerr.CodeLogCorrupt,
			fmt.Sprintf("previous-frame digest mismatch at offset %d", base)).
			WithLogOffset(base, int(bodyLen), seq-1)
	}
	bodyStart := base + int64(FrameHeaderSize)
	body := make([]byte, bodyLen)
	n, err = io.ReadFull(f, body)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return domain.Event{}, 0, scerr.New(scerr.CodeLogTruncated,
			fmt.Sprintf("truncated frame body at offset %d: got %d of %d bytes", bodyStart, n, bodyLen)).
			WithLogOffset(bodyStart, int(bodyLen), seq-1)
	}
	if err != nil {
		return domain.Event{}, 0, scerr.New(scerr.CodeLogCorrupt, fmt.Sprintf("read error at offset %d: %v", bodyStart, err)).
			WithLogOffset(bodyStart, int(bodyLen), seq-1)
	}
	if crc32.ChecksumIEEE(body) != crc {
		return domain.Event{}, 0, scerr.New(scerr.CodeLogCorrupt,
			fmt.Sprintf("body CRC mismatch at offset %d", base)).WithLogOffset(base, int(bodyLen), seq)
	}
	var ev domain.Event
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&ev); err != nil {
		return domain.Event{}, 0, scerr.New(scerr.CodeLogCorrupt,
			fmt.Sprintf("invalid event JSON at offset %d: %v", base, err)).
			WithLogOffset(base, int(bodyLen), seq)
	}
	ev.Seq = seq
	digest := frameDigest(header, body)
	return ev, digest, nil
}
