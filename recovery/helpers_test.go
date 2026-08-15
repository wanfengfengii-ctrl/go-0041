package recovery_test

import (
	"bytes"
	"os"
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
