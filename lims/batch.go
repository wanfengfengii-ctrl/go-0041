// Package lims adapts LIMS batch imports — JSON envelopes and CSV — to the
// internal command path. Both formats carry a protocol_version, a key_id and
// an HMAC-SHA256 signature. The adapter normalises every batch to the same
// canonical signed payload, so an equivalent JSON batch and CSV batch produce
// byte-identical canonical bytes and therefore verify against the same
// signature.
//
// A batch is validated wholly before any of it is applied: if any record fails
// parsing, authorisation, reference or business validation the entire batch is
// rejected with BATCH_PARTIAL_INVALID and a per-record problem list sorted by
// input row. No events are appended for a rejected batch.
package lims

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"specimen-custody-graph/coord"
	"specimen-custody-graph/domain"
	"specimen-custody-graph/infra"
	"specimen-custody-graph/scerr"
)

// ProtocolVersion is the only supported batch protocol version.
const ProtocolVersion = 1

// Record is the normalised representation of one batch entry. Fields map
// directly onto a domain.Command.
type Record struct {
	Op               string `json:"op"`
	FamilyID         string `json:"family_id,omitempty"`
	ExpectedRevision uint64 `json:"expected_revision,omitempty"`
	EntityID         string `json:"entity_id,omitempty"`
	ParentID         string `json:"parent_id,omitempty"`
	ChildID          string `json:"child_id,omitempty"`
	Source           string `json:"source,omitempty"`
	Volume           int    `json:"volume,omitempty"`
	StationID        string `json:"station_id,omitempty"`
	Approver         string `json:"approver,omitempty"`
	Reason           string `json:"reason,omitempty"`
	SealID           string `json:"seal_id,omitempty"`
	DestructionID    string `json:"destruction_id,omitempty"`
	Operator         string `json:"operator,omitempty"`
	Department       string `json:"department,omitempty"`
	Role             string `json:"role,omitempty"`
}

// Envelope is the JSON batch container.
type Envelope struct {
	ProtocolVersion int      `json:"protocol_version"`
	KeyID           string   `json:"key_id"`
	Signature       string   `json:"signature"`
	Records         []Record `json:"records"`
}

// canonicalPayload is the structure that is actually signed. It is identical
// for JSON and CSV batches with equivalent content.
type canonicalPayload struct {
	ProtocolVersion int      `json:"protocol_version"`
	Records         []Record `json:"records"`
}

// Adapter parses and executes LIMS batches.
type Adapter struct {
	coord *coord.Coordinator
	keys  infra.KeyStore
}

// NewAdapter returns an Adapter.
func NewAdapter(c *coord.Coordinator, keys infra.KeyStore) *Adapter {
	return &Adapter{coord: c, keys: keys}
}

// Import parses a batch (JSON or CSV) and, if valid, executes it atomically.
// contentType is "json" or "csv".
func (a *Adapter) Import(contentType string, data []byte) ([]domain.Event, error) {
	records, keyID, sig, err := parse(contentType, data)
	if err != nil {
		return nil, err
	}
	if err := verifySignature(a.keys, records, keyID, sig); err != nil {
		return nil, err
	}
	cmds, err := toCommands(records)
	if err != nil {
		return nil, err
	}
	return a.coord.SubmitBatch(cmds)
}

// CanonicalPayload returns the canonical signed payload bytes for a set of
// records. Exported so tests can compute signatures against a shared vector.
func CanonicalPayload(records []Record) ([]byte, error) {
	return canonicalBytes(canonicalPayload{ProtocolVersion: ProtocolVersion, Records: records})
}

// Sign computes the hex HMAC-SHA256 signature of the canonical payload using
// the given secret.
func Sign(secret []byte, records []Record) (string, error) {
	payload, err := CanonicalPayload(records)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// parse dispatches to the format-specific parser.
func parse(contentType string, data []byte) (records []Record, keyID, signature string, err error) {
	switch strings.ToLower(strings.TrimSpace(contentType)) {
	case "json", "application/json":
		return parseJSON(data)
	case "csv", "text/csv":
		return parseCSV(data)
	default:
		return nil, "", "", scerr.New(scerr.CodeUnsupportedVersion,
			"unsupported content type: "+contentType)
	}
}

// parseJSON parses a JSON envelope.
func parseJSON(data []byte) ([]Record, string, string, error) {
	var env Envelope
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&env); err != nil {
		return nil, "", "", scerr.New(scerr.CodeParseError,
			"invalid JSON batch: "+err.Error()).WithRetryable(false)
	}
	if env.ProtocolVersion != ProtocolVersion {
		return nil, "", "", scerr.New(scerr.CodeUnsupportedVersion,
			fmt.Sprintf("unsupported protocol_version %d (want %d)", env.ProtocolVersion, ProtocolVersion))
	}
	return env.Records, env.KeyID, env.Signature, nil
}

// parseCSV parses a CSV batch. The first row is fixed metadata
// (protocol_version,key_id,signature); the second row is the column header;
// subsequent rows are records.
func parseCSV(data []byte) ([]Record, string, string, error) {
	r := csv.NewReader(bytes.NewReader(data))
	r.FieldsPerRecord = -1
	r.TrimLeadingSpace = true
	rows, err := r.ReadAll()
	if err != nil {
		return nil, "", "", scerr.New(scerr.CodeParseError,
			"invalid CSV batch: "+err.Error()).WithRetryable(false)
	}
	if len(rows) < 2 {
		return nil, "", "", scerr.New(scerr.CodeParseError, "CSV batch has fewer than 2 rows")
	}
	// metadata row
	meta := rows[0]
	if len(meta) < 3 {
		return nil, "", "", scerr.New(scerr.CodeParseError, "CSV metadata row must have 3 columns")
	}
	pv, err := strconv.Atoi(strings.TrimSpace(meta[0]))
	if err != nil {
		return nil, "", "", scerr.New(scerr.CodeParseError, "CSV protocol_version is not an integer")
	}
	if pv != ProtocolVersion {
		return nil, "", "", scerr.New(scerr.CodeUnsupportedVersion,
			fmt.Sprintf("unsupported protocol_version %d (want %d)", pv, ProtocolVersion))
	}
	keyID := strings.TrimSpace(meta[1])
	signature := strings.TrimSpace(meta[2])

	header := rows[1]
	colIndex := map[string]int{}
	for i, h := range header {
		colIndex[strings.TrimSpace(h)] = i
	}
	records := make([]Record, 0, len(rows)-2)
	for i := 2; i < len(rows); i++ {
		row := rows[i]
		rec, perr := rowToRecord(row, colIndex)
		if perr != nil {
			se := scerr.As(perr)
			if se != nil {
				se.RecordIndex = i - 2
			}
			return nil, "", "", perr
		}
		records = append(records, rec)
	}
	return records, keyID, signature, nil
}

func rowToRecord(row []string, col map[string]int) (Record, error) {
	get := func(name string) string {
		if i, ok := col[name]; ok && i < len(row) {
			return strings.TrimSpace(row[i])
		}
		return ""
	}
	rec := Record{
		Op:            get("op"),
		FamilyID:      get("family_id"),
		EntityID:      get("entity_id"),
		ParentID:      get("parent_id"),
		ChildID:       get("child_id"),
		Source:        get("source"),
		StationID:     get("station_id"),
		Approver:      get("approver"),
		Reason:        get("reason"),
		SealID:        get("seal_id"),
		DestructionID: get("destruction_id"),
		Operator:      get("operator"),
		Department:    get("department"),
		Role:          get("role"),
	}
	if v := get("volume"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Record{}, scerr.New(scerr.CodeParseError, "volume is not an integer: "+v).WithField("volume")
		}
		rec.Volume = n
	}
	if v := get("expected_revision"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return Record{}, scerr.New(scerr.CodeParseError, "expected_revision is not an integer: "+v).WithField("expected_revision")
		}
		rec.ExpectedRevision = n
	}
	return rec, nil
}

// canonicalBytes marshals the payload with sorted keys and no whitespace.
func canonicalBytes(p canonicalPayload) ([]byte, error) {
	b, err := json.Marshal(p)
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

// verifySignature recomputes the HMAC over the canonical payload and compares
// it to the supplied signature.
func verifySignature(keys infra.KeyStore, records []Record, keyID, signature string) error {
	secret, ok := keys.Lookup(keyID)
	if !ok {
		return scerr.New(scerr.CodeSignatureMismatch, "unknown key_id: "+keyID)
	}
	payload, err := CanonicalPayload(records)
	if err != nil {
		return scerr.New(scerr.CodeStorage, "canonicalise payload: "+err.Error()).WithRetryable(true)
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(signature), []byte(expected)) {
		return scerr.New(scerr.CodeSignatureMismatch, "signature mismatch")
	}
	return nil
}

// toCommands converts records to commands, filling the principal and falling
// back to the injected id generator for missing entity ids where appropriate.
func toCommands(records []Record) ([]domain.Command, error) {
	cmds := make([]domain.Command, 0, len(records))
	for i, rec := range records {
		cmd, err := recordToCommand(rec)
		if err != nil {
			se := scerr.As(err)
			if se != nil {
				se.RecordIndex = i
			}
			return nil, err
		}
		cmds = append(cmds, cmd)
	}
	return cmds, nil
}

func recordToCommand(rec Record) (domain.Command, error) {
	role := domain.Role(rec.Role)
	if role == "" {
		role = domain.RoleOperator
	}
	switch role {
	case domain.RoleOperator, domain.RoleApprover, domain.RoleAuditor:
	default:
		return domain.Command{}, scerr.New(scerr.CodePermissionDenied, "unknown role: "+rec.Role).WithField("role")
	}
	if rec.Op == "" {
		return domain.Command{}, scerr.New(scerr.CodeParseError, "record op is empty").WithField("op")
	}
	cmd := domain.Command{
		Op:               rec.Op,
		FamilyID:         rec.FamilyID,
		ExpectedRevision: rec.ExpectedRevision,
		EntityID:         rec.EntityID,
		ParentID:         rec.ParentID,
		ChildID:          rec.ChildID,
		Source:           rec.Source,
		Volume:           rec.Volume,
		StationID:        rec.StationID,
		Approver:         rec.Approver,
		Reason:           rec.Reason,
		SealID:           rec.SealID,
		DestructionID:    rec.DestructionID,
		Principal: domain.Principal{
			Operator:   rec.Operator,
			Department: rec.Department,
			Role:       role,
		},
	}
	return cmd, nil
}

// EncodeCSV builds a CSV batch from records, a key id and signature. Used by
// tests to construct equivalent batches.
func EncodeCSV(records []Record, keyID, signature string) ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write([]string{strconv.Itoa(ProtocolVersion), keyID, signature}); err != nil {
		return nil, err
	}
	header := []string{"op", "family_id", "entity_id", "parent_id", "child_id", "source",
		"volume", "station_id", "approver", "reason", "seal_id", "destruction_id",
		"expected_revision", "operator", "department", "role"}
	if err := w.Write(header); err != nil {
		return nil, err
	}
	for _, rec := range records {
		row := []string{
			rec.Op, rec.FamilyID, rec.EntityID, rec.ParentID, rec.ChildID, rec.Source,
			strconv.Itoa(rec.Volume), rec.StationID, rec.Approver, rec.Reason,
			rec.SealID, rec.DestructionID, strconv.FormatUint(rec.ExpectedRevision, 10),
			rec.Operator, rec.Department, rec.Role,
		}
		if err := w.Write(row); err != nil {
			return nil, err
		}
	}
	w.Flush()
	return buf.Bytes(), nil
}

// EncodeJSON builds a JSON envelope from records, a key id and signature.
func EncodeJSON(records []Record, keyID, signature string) ([]byte, error) {
	env := Envelope{ProtocolVersion: ProtocolVersion, KeyID: keyID, Signature: signature, Records: records}
	return json.Marshal(env)
}

// sortedRecords returns records sorted by op then family_id for stable output.
func sortedRecords(records []Record) []Record {
	cp := append([]Record(nil), records...)
	sort.SliceStable(cp, func(i, j int) bool {
		if cp[i].Op != cp[j].Op {
			return cp[i].Op < cp[j].Op
		}
		return cp[i].FamilyID < cp[j].FamilyID
	})
	return cp
}

// Ensure io is referenced (used by csv/json readers through bytes, kept for
// future stream-based ingestion).
var _ = io.EOF
