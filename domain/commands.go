package domain

import (
	"fmt"
	"time"
)

// Op identifiers for commands and (with the Ev prefix) events.
const (
	OpRegister           = "register"
	OpClaim              = "claim"
	OpReturn             = "return"
	OpAliquot            = "aliquot"
	OpLoadStation        = "load_station"
	OpConfirmConsumption = "confirm_consumption"
	OpUnload             = "unload"
	OpSeal               = "seal"
	OpDestroy            = "destroy"
)

// Command is the union of all commands. Not every field is relevant to every
// Op; the apply logic reads only the fields its Op requires. Commands are
// JSON-serialisable so that the HTTP layer and the LIMS batch adapter can use
// the same representation.
type Command struct {
	Op               string    `json:"op"`
	Principal        Principal `json:"principal"`
	FamilyID         string    `json:"family_id"`
	ExpectedRevision uint64    `json:"expected_revision"`
	EntityID         string    `json:"entity_id,omitempty"`
	ParentID         string    `json:"parent_id,omitempty"`
	ChildID          string    `json:"child_id,omitempty"`
	Source           string    `json:"source,omitempty"`
	Volume           int       `json:"volume,omitempty"`
	StationID        string    `json:"station_id,omitempty"`
	Approver         string    `json:"approver,omitempty"`
	Reason           string    `json:"reason,omitempty"`
	SealID           string    `json:"seal_id,omitempty"`
	DestructionID    string    `json:"destruction_id,omitempty"`
	Now              time.Time `json:"now,omitempty"`
}

// Event records a committed mutation. Seq is assigned by the event store on
// append; Revision is the family revision after the mutation. The payload
// fields mirror the relevant Command fields so that ApplyEvent can replay the
// mutation without the original Command.
type Event struct {
	Seq           uint64 `json:"seq"`
	Type          string `json:"type"`
	FamilyID      string `json:"family_id"`
	Revision      uint64 `json:"revision"`
	Timestamp     int64  `json:"ts"`
	EntityID      string `json:"entity_id,omitempty"`
	ParentID      string `json:"parent_id,omitempty"`
	ChildID       string `json:"child_id,omitempty"`
	Source        string `json:"source,omitempty"`
	Volume        int    `json:"volume,omitempty"`
	StationID     string `json:"station_id,omitempty"`
	Operator      string `json:"operator,omitempty"`
	Department    string `json:"department,omitempty"`
	Approver      string `json:"approver,omitempty"`
	Reason        string `json:"reason,omitempty"`
	SealID        string `json:"seal_id,omitempty"`
	DestructionID string `json:"destruction_id,omitempty"`
	Kind          Kind   `json:"kind,omitempty"`
}

// Apply validates the command against the family state, applies it and
// returns the resulting event. It does NOT perform authorisation or revision
// checking — those are the coordinator's responsibility. Apply mutates the
// receiver; callers that need transactional semantics operate on a clone.
func (f *Family) Apply(cmd Command) (Event, error) {
	switch cmd.Op {
	case OpRegister:
		return f.applyRegister(cmd)
	case OpClaim:
		return f.applyClaim(cmd)
	case OpReturn:
		return f.applyReturn(cmd)
	case OpAliquot:
		return f.applyAliquot(cmd)
	case OpLoadStation:
		return f.applyLoadStation(cmd)
	case OpConfirmConsumption:
		return f.applyConfirmConsumption(cmd)
	case OpUnload:
		return f.applyUnload(cmd)
	case OpSeal:
		return f.applySeal(cmd)
	case OpDestroy:
		return f.applyDestroy(cmd)
	default:
		return Event{}, fmt.Errorf("unknown op %q", cmd.Op)
	}
}

func (f *Family) newEvent(cmd Command) Event {
	return Event{
		Type:       cmd.Op,
		FamilyID:   f.FamilyID,
		Revision:   f.Revision + 1,
		Timestamp:  cmd.Now.UnixNano(),
		Operator:   cmd.Principal.Operator,
		Department: cmd.Principal.Department,
	}
}

func (f *Family) applyRegister(cmd Command) (Event, error) {
	if f.Mother != nil {
		return Event{}, errTerminal("family already registered", cmd.Op, "")
	}
	if cmd.Volume <= 0 {
		return Event{}, errQty("initial volume must be positive", cmd.Op, cmd.EntityID, "volume")
	}
	if cmd.EntityID == "" {
		return Event{}, errConflict("entity_id is required", cmd.Op, "")
	}
	e := f.newEvent(cmd)
	e.EntityID = cmd.EntityID
	e.Source = cmd.Source
	e.Volume = cmd.Volume
	e.Kind = KindMother
	if err := f.ApplyEvent(e); err != nil {
		return Event{}, err
	}
	return e, nil
}

func (f *Family) applyClaim(cmd Command) (Event, error) {
	ent := f.entity(cmd.EntityID)
	if ent == nil {
		return Event{}, errNotFound(cmd.EntityID, cmd.Op)
	}
	if ent.IsTerminal() {
		return Event{}, errTerminal("entity is destroyed", cmd.Op, cmd.EntityID)
	}
	if ent.Status == StatusSealed {
		return Event{}, errTerminal("entity is sealed", cmd.Op, cmd.EntityID)
	}
	if ent.OnStation() {
		return Event{}, errConflict("entity is on a station", cmd.Op, cmd.EntityID)
	}
	if ent.InCustody() {
		return Event{}, errConflict("entity already in custody", cmd.Op, cmd.EntityID)
	}
	e := f.newEvent(cmd)
	e.EntityID = cmd.EntityID
	if err := f.ApplyEvent(e); err != nil {
		return Event{}, err
	}
	return e, nil
}

func (f *Family) applyReturn(cmd Command) (Event, error) {
	ent := f.entity(cmd.EntityID)
	if ent == nil {
		return Event{}, errNotFound(cmd.EntityID, cmd.Op)
	}
	if ent.IsTerminal() {
		return Event{}, errTerminal("entity is destroyed", cmd.Op, cmd.EntityID)
	}
	if ent.OnStation() {
		return Event{}, errConflict("entity is on a station; unload first", cmd.Op, cmd.EntityID)
	}
	if !ent.InCustody() {
		return Event{}, errConflict("entity is not in custody", cmd.Op, cmd.EntityID)
	}
	if ent.CustodyDept != cmd.Principal.Department {
		return Event{}, errPermission("only the custodian may return", cmd.Op, cmd.EntityID)
	}
	e := f.newEvent(cmd)
	e.EntityID = cmd.EntityID
	if err := f.ApplyEvent(e); err != nil {
		return Event{}, err
	}
	return e, nil
}

func (f *Family) applyAliquot(cmd Command) (Event, error) {
	parent := f.entity(cmd.ParentID)
	if parent == nil {
		return Event{}, errNotFound(cmd.ParentID, cmd.Op)
	}
	if parent.IsTerminal() {
		return Event{}, errTerminal("parent is destroyed", cmd.Op, cmd.ParentID)
	}
	if parent.Status == StatusSealed {
		return Event{}, errTerminal("parent is sealed", cmd.Op, cmd.ParentID)
	}
	if parent.OnStation() {
		return Event{}, errConflict("parent is on a station", cmd.Op, cmd.ParentID)
	}
	if parent.CustodyDept != cmd.Principal.Department {
		return Event{}, errPermission("only the custodian may aliquot", cmd.Op, cmd.ParentID)
	}
	if cmd.Volume <= 0 {
		return Event{}, errQty("aliquot volume must be positive", cmd.Op, cmd.ParentID, "volume")
	}
	if parent.Available < cmd.Volume {
		return Event{}, errQty(fmt.Sprintf("insufficient available volume: have %d, want %d", parent.Available, cmd.Volume), cmd.Op, cmd.ParentID, "volume")
	}
	if cmd.ChildID == "" {
		return Event{}, errConflict("child_id is required", cmd.Op, "")
	}
	if f.entity(cmd.ChildID) != nil {
		return Event{}, errConflict("child_id already exists", cmd.Op, cmd.ChildID)
	}
	if f.wouldCycle(cmd.ParentID, cmd.ChildID) {
		return Event{}, errConflict("aliquot would form a cycle", cmd.Op, cmd.ChildID)
	}
	e := f.newEvent(cmd)
	e.ParentID = cmd.ParentID
	e.ChildID = cmd.ChildID
	e.Volume = cmd.Volume
	e.Kind = KindTube
	if err := f.ApplyEvent(e); err != nil {
		return Event{}, err
	}
	return e, nil
}

func (f *Family) applyLoadStation(cmd Command) (Event, error) {
	tube := f.entity(cmd.EntityID)
	if tube == nil || tube.Kind != KindTube {
		return Event{}, errNotFound(cmd.EntityID, cmd.Op)
	}
	if tube.IsTerminal() {
		return Event{}, errTerminal("tube is destroyed", cmd.Op, cmd.EntityID)
	}
	if tube.Status == StatusSealed {
		return Event{}, errTerminal("tube is sealed", cmd.Op, cmd.EntityID)
	}
	if tube.OnStation() {
		return Event{}, errConflict("tube already on a station", cmd.Op, cmd.EntityID)
	}
	if tube.CustodyDept != cmd.Principal.Department {
		return Event{}, errPermission("only the custodian may load the station", cmd.Op, cmd.EntityID)
	}
	stationID := cmd.StationID
	if stationID == "" {
		return Event{}, errConflict("station_id is required", cmd.Op, "")
	}
	if st := f.Stations[stationID]; st != nil {
		return Event{}, errConflict("station task already exists", cmd.Op, stationID)
	}
	e := f.newEvent(cmd)
	e.EntityID = cmd.EntityID
	e.StationID = stationID
	if err := f.ApplyEvent(e); err != nil {
		return Event{}, err
	}
	return e, nil
}

func (f *Family) applyConfirmConsumption(cmd Command) (Event, error) {
	st := f.Stations[cmd.StationID]
	if st == nil {
		return Event{}, errNotFound(cmd.StationID, cmd.Op)
	}
	if !st.Active {
		return Event{}, errTerminal("station task has ended", cmd.Op, cmd.StationID)
	}
	if st.Department != cmd.Principal.Department {
		return Event{}, errPermission("only the loading department may confirm consumption", cmd.Op, cmd.EntityID)
	}
	tube := f.entity(st.LoadedTubeID)
	if tube == nil {
		return Event{}, errNotFound(st.LoadedTubeID, cmd.Op)
	}
	if cmd.Volume <= 0 {
		return Event{}, errQty("consumption must be positive", cmd.Op, tube.ID, "volume")
	}
	if tube.Available < cmd.Volume {
		return Event{}, errQty(fmt.Sprintf("insufficient available volume: have %d, want %d", tube.Available, cmd.Volume), cmd.Op, tube.ID, "volume")
	}
	e := f.newEvent(cmd)
	e.StationID = cmd.StationID
	e.EntityID = tube.ID
	e.Volume = cmd.Volume
	if err := f.ApplyEvent(e); err != nil {
		return Event{}, err
	}
	return e, nil
}

func (f *Family) applyUnload(cmd Command) (Event, error) {
	st := f.Stations[cmd.StationID]
	if st == nil {
		return Event{}, errNotFound(cmd.StationID, cmd.Op)
	}
	if !st.Active {
		return Event{}, errTerminal("station task has ended", cmd.Op, cmd.StationID)
	}
	if st.Department != cmd.Principal.Department {
		return Event{}, errPermission("only the loading department may unload", cmd.Op, st.LoadedTubeID)
	}
	e := f.newEvent(cmd)
	e.StationID = cmd.StationID
	e.EntityID = st.LoadedTubeID
	if err := f.ApplyEvent(e); err != nil {
		return Event{}, err
	}
	return e, nil
}

func (f *Family) applySeal(cmd Command) (Event, error) {
	ent := f.entity(cmd.EntityID)
	if ent == nil {
		return Event{}, errNotFound(cmd.EntityID, cmd.Op)
	}
	if ent.IsTerminal() {
		return Event{}, errTerminal("entity is destroyed", cmd.Op, cmd.EntityID)
	}
	if ent.Status == StatusSealed {
		return Event{}, errConflict("entity already sealed", cmd.Op, cmd.EntityID)
	}
	if ent.OnStation() {
		return Event{}, errConflict("entity is on a station", cmd.Op, cmd.EntityID)
	}
	if ent.CustodyDept != cmd.Principal.Department {
		return Event{}, errPermission("only the custodian may seal", cmd.Op, cmd.EntityID)
	}
	if cmd.SealID == "" {
		return Event{}, errConflict("seal_id is required", cmd.Op, "")
	}
	e := f.newEvent(cmd)
	e.EntityID = cmd.EntityID
	e.SealID = cmd.SealID
	e.Approver = cmd.Approver
	e.Reason = cmd.Reason
	if err := f.ApplyEvent(e); err != nil {
		return Event{}, err
	}
	return e, nil
}

func (f *Family) applyDestroy(cmd Command) (Event, error) {
	ent := f.entity(cmd.EntityID)
	if ent == nil {
		return Event{}, errNotFound(cmd.EntityID, cmd.Op)
	}
	if ent.IsTerminal() {
		return Event{}, errTerminal("entity already destroyed", cmd.Op, cmd.EntityID)
	}
	if ent.OnStation() {
		return Event{}, errConflict("entity is on a station; unload first", cmd.Op, cmd.EntityID)
	}
	if ent.CustodyDept != cmd.Principal.Department {
		return Event{}, errPermission("only the custodian may destroy", cmd.Op, cmd.EntityID)
	}
	if cmd.DestructionID == "" {
		return Event{}, errConflict("destruction_id is required", cmd.Op, "")
	}
	e := f.newEvent(cmd)
	e.EntityID = cmd.EntityID
	e.DestructionID = cmd.DestructionID
	e.Approver = cmd.Approver
	e.Reason = cmd.Reason
	e.Volume = ent.Available
	if err := f.ApplyEvent(e); err != nil {
		return Event{}, err
	}
	return e, nil
}
