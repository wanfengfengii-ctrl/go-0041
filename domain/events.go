package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"specimen-custody-graph/scerr"
)

// ApplyEvent mutates the family to reflect a committed event. It is the single
// source of truth for state transitions and is used both by the live command
// path (after validation) and by recovery (replaying recorded events). It
// assumes the event has already been validated.
func (f *Family) ApplyEvent(e Event) error {
	switch e.Type {
	case OpRegister:
		f.FamilyID = e.FamilyID
		f.Revision = e.Revision
		if f.Tubes == nil {
			f.Tubes = map[string]*Entity{}
		}
		if f.Stations == nil {
			f.Stations = map[string]*Station{}
		}
		f.Mother = &Entity{
			ID:            e.EntityID,
			Kind:          KindMother,
			Source:        e.Source,
			InitialVolume: e.Volume,
			Available:     e.Volume,
			CustodyDept:   e.Department,
			Status:        StatusActive,
			CreatedSeq:    e.Seq,
		}
	case OpClaim:
		f.Revision = e.Revision
		if ent := f.entity(e.EntityID); ent != nil {
			ent.CustodyDept = e.Department
		}
	case OpReturn:
		f.Revision = e.Revision
		if ent := f.entity(e.EntityID); ent != nil {
			ent.CustodyDept = ""
		}
	case OpAliquot:
		f.Revision = e.Revision
		parent := f.entity(e.ParentID)
		if parent == nil {
			return fmt.Errorf("aliquot: parent %q missing", e.ParentID)
		}
		parent.Available -= e.Volume
		parent.AliquotedOut += e.Volume
		f.Tubes[e.ChildID] = &Entity{
			ID:            e.ChildID,
			Kind:          KindTube,
			ParentID:      e.ParentID,
			InitialVolume: e.Volume,
			Available:     e.Volume,
			CustodyDept:   e.Department,
			Status:        StatusActive,
			CreatedSeq:    e.Seq,
		}
	case OpLoadStation:
		f.Revision = e.Revision
		tube := f.entity(e.EntityID)
		if tube == nil {
			return fmt.Errorf("load: tube %q missing", e.EntityID)
		}
		tube.StationID = e.StationID
		tube.CustodyDept = ""
		f.Stations[e.StationID] = &Station{
			ID:           e.StationID,
			LoadedTubeID: e.EntityID,
			Operator:     e.Operator,
			Department:   e.Department,
			Active:       true,
		}
	case OpConfirmConsumption:
		f.Revision = e.Revision
		st := f.Stations[e.StationID]
		if st == nil {
			return fmt.Errorf("consumption: station %q missing", e.StationID)
		}
		tube := f.entity(e.EntityID)
		if tube == nil {
			return fmt.Errorf("consumption: tube %q missing", e.EntityID)
		}
		tube.Available -= e.Volume
		tube.Consumed += e.Volume
		st.ApprovedConsumption += e.Volume
		f.ConsumedTotal += e.Volume
	case OpUnload:
		f.Revision = e.Revision
		st := f.Stations[e.StationID]
		if st == nil {
			return fmt.Errorf("unload: station %q missing", e.StationID)
		}
		st.Active = false
		if tube := f.entity(e.EntityID); tube != nil {
			tube.StationID = ""
			tube.CustodyDept = st.Department
		}
	case OpSeal:
		f.Revision = e.Revision
		if ent := f.entity(e.EntityID); ent != nil {
			ent.Status = StatusSealed
		}
		f.Seals = append(f.Seals, Seal{
			ID:       e.SealID,
			EntityID: e.EntityID,
			Approver: e.Approver,
			Reason:   e.Reason,
			Seq:      e.Seq,
		})
	case OpDestroy:
		f.Revision = e.Revision
		ent := f.entity(e.EntityID)
		if ent == nil {
			return fmt.Errorf("destroy: entity %q missing", e.EntityID)
		}
		ent.DestroyedVolume = ent.Available
		f.DestroyedTotal += ent.Available
		ent.Available = 0
		ent.Status = StatusDestroyed
		ent.CustodyDept = ""
		f.Destructions = append(f.Destructions, Destruction{
			ID:       e.DestructionID,
			EntityID: e.EntityID,
			Approver: e.Approver,
			Reason:   e.Reason,
			Volume:   ent.DestroyedVolume,
			Seq:      e.Seq,
		})
	default:
		return fmt.Errorf("unknown event type %q", e.Type)
	}
	return nil
}

// Clone returns a deep copy of the family. The coordinator clones before
// applying a command so that a persistence failure can discard the working
// copy without affecting the published state.
func (f *Family) Clone() *Family {
	if f == nil {
		return nil
	}
	cp := &Family{
		FamilyID:       f.FamilyID,
		Revision:       f.Revision,
		ConsumedTotal:  f.ConsumedTotal,
		DestroyedTotal: f.DestroyedTotal,
		Tubes:          map[string]*Entity{},
		Stations:       map[string]*Station{},
		Seals:          append([]Seal(nil), f.Seals...),
		Destructions:   append([]Destruction(nil), f.Destructions...),
	}
	if f.Mother != nil {
		m := *f.Mother
		cp.Mother = &m
	}
	for id, t := range f.Tubes {
		tc := *t
		cp.Tubes[id] = &tc
	}
	for id, s := range f.Stations {
		sc := *s
		cp.Stations[id] = &sc
	}
	return cp
}

// CanonicalSnapshot is the canonical, deterministically-ordered representation
// of a family used for snapshot files and digest computation. Every slice is
// sorted by a stable key so that two families with equal logical state
// produce byte-identical serialisations.
type CanonicalSnapshot struct {
	FamilyID       string        `json:"family_id"`
	Revision       uint64        `json:"revision"`
	Mother         *Entity       `json:"mother"`
	Tubes          []*Entity     `json:"tubes"`
	Stations       []*Station    `json:"stations"`
	Seals          []Seal        `json:"seals"`
	Destructions   []Destruction `json:"destructions"`
	ConsumedTotal  int           `json:"consumed_total"`
	DestroyedTotal int           `json:"destroyed_total"`
}

// Canonical returns the canonical representation of the family.
func (f *Family) Canonical() CanonicalSnapshot {
	cs := CanonicalSnapshot{
		FamilyID:       f.FamilyID,
		Revision:       f.Revision,
		ConsumedTotal:  f.ConsumedTotal,
		DestroyedTotal: f.DestroyedTotal,
		Seals:          append([]Seal(nil), f.Seals...),
		Destructions:   append([]Destruction(nil), f.Destructions...),
	}
	if f.Mother != nil {
		m := *f.Mother
		cs.Mother = &m
	}
	tubes := make([]*Entity, 0, len(f.Tubes))
	for _, t := range f.Tubes {
		tc := *t
		tubes = append(tubes, &tc)
	}
	sort.Slice(tubes, func(i, j int) bool { return tubes[i].ID < tubes[j].ID })
	cs.Tubes = tubes
	stations := make([]*Station, 0, len(f.Stations))
	for _, s := range f.Stations {
		sc := *s
		stations = append(stations, &sc)
	}
	sort.Slice(stations, func(i, j int) bool { return stations[i].ID < stations[j].ID })
	cs.Stations = stations
	sort.Slice(cs.Seals, func(i, j int) bool {
		if cs.Seals[i].ID != cs.Seals[j].ID {
			return cs.Seals[i].ID < cs.Seals[j].ID
		}
		return cs.Seals[i].EntityID < cs.Seals[j].EntityID
	})
	sort.Slice(cs.Destructions, func(i, j int) bool {
		if cs.Destructions[i].ID != cs.Destructions[j].ID {
			return cs.Destructions[i].ID < cs.Destructions[j].ID
		}
		return cs.Destructions[i].EntityID < cs.Destructions[j].EntityID
	})
	return cs
}

// Digest returns a hex-encoded SHA-256 over the canonical JSON of the family.
// Equal digests imply equal logical state.
func (f *Family) Digest() (string, error) {
	cs := f.Canonical()
	data, err := canonicalJSON(cs)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:]), nil
}

// canonicalJSON marshals v with sorted map keys and no extraneous whitespace.
func canonicalJSON(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	// Re-encode through a generic decode to guarantee sorted object keys
	// regardless of struct/map origin. json.Marshal already sorts map keys,
	// but round-tripping normalises nested maps produced via omitempty.
	var anyVal any
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&anyVal); err != nil {
		return nil, err
	}
	return json.Marshal(anyVal)
}

// LineageNode is one node in a family's directed pedigree tree.
type LineageNode struct {
	Entity   *Entity  `json:"entity"`
	Children []string `json:"children"`
	Distance int      `json:"distance_from_mother"`
}

// Lineage returns the pedigree of the family: each entity with its direct
// children and distance from the mother. The map is keyed by entity id.
func (f *Family) Lineage() map[string]LineageNode {
	out := map[string]LineageNode{}
	entities := f.allEntities()
	children := map[string][]string{}
	for _, e := range entities {
		if e.ParentID != "" {
			children[e.ParentID] = append(children[e.ParentID], e.ID)
		}
	}
	for _, e := range entities {
		ch := append([]string(nil), children[e.ID]...)
		sort.Strings(ch)
		dist := 0
		cur := e.ParentID
		for cur != "" {
			dist++
			p := f.entity(cur)
			if p == nil {
				break
			}
			cur = p.ParentID
		}
		ent := *e
		out[e.ID] = LineageNode{Entity: &ent, Children: ch, Distance: dist}
	}
	return out
}

// Authorize checks role and department for a command. It returns a *scerr.Error
// when the principal may not perform the command. Department ownership is
// checked in Apply for operations that target a specific custodian; here we
// only enforce the coarse role rules.
func Authorize(cmd Command) error {
	if !cmd.Principal.Role.IsWrite() {
		return errPermission("auditors may not write", cmd.Op, cmd.EntityID)
	}
	if cmd.Principal.Department == "" {
		return errPermission("department is required", cmd.Op, cmd.EntityID).WithField("department")
	}
	switch cmd.Op {
	case OpSeal, OpDestroy:
		if cmd.Principal.Role != RoleApprover {
			return errPermission("only approvers may seal or destroy", cmd.Op, cmd.EntityID)
		}
	}
	return nil
}

// TimestampFromClock returns the current time from a clock, falling back to
// the zero time if clock is nil. This keeps the domain decoupled from the
// concrete clock type.
func TimestampFromClock(now time.Time) time.Time { return now }

// error helpers ------------------------------------------------------------

func errTerminal(msg, op, entity string) *scerr.Error {
	return scerr.New(scerr.CodeTerminalState, msg).WithOperation(op).WithEntity(entity)
}
func errQty(msg, op, entity, field string) *scerr.Error {
	return scerr.New(scerr.CodeQuantityViolation, msg).WithOperation(op).WithEntity(entity).WithField(field)
}
func errConflict(msg, op, entity string) *scerr.Error {
	return scerr.New(scerr.CodeConflict, msg).WithOperation(op).WithEntity(entity)
}
func errPermission(msg, op, entity string) *scerr.Error {
	return scerr.New(scerr.CodePermissionDenied, msg).WithOperation(op).WithEntity(entity)
}
func errNotFound(entity, op string) *scerr.Error {
	return scerr.New(scerr.CodeNotFound, "entity not found: "+entity).WithOperation(op).WithEntity(entity)
}
