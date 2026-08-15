package eventstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// canonicalJSON marshals v with sorted object keys and no extraneous
// whitespace, so the same logical value always produces the same bytes.
func canonicalJSON(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var anyVal any
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&anyVal); err != nil {
		return nil, err
	}
	return json.Marshal(anyVal)
}

// sha256hex returns the hex-encoded SHA-256 of data.
func sha256hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
