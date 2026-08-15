// Package domain defines the specimen-custody-graph domain model: specimen
// families (the consistency boundary), mother samples and aliquot tubes, lab
// stations, seal and destruction records, the commands that mutate them and
// the events that record those mutations.
//
// The model is event-sourced. Each command is validated against the current
// family state, produces an Event and is then applied by Family.ApplyEvent.
// Recovery replays recorded events through ApplyEvent, so the mutation logic
// lives in exactly one place. All volumes are integer microliters; the model
// never uses floating point, which keeps the volume-conservation invariant
// exact.
package domain

import (
	"sort"
)

// Role is the authorisation role of a principal.
type Role string

const (
	RoleOperator Role = "operator"
	RoleApprover Role = "approver"
	RoleAuditor  Role = "auditor"
)

// IsWrite reports whether the role may perform write operations at all.
// Auditors are read-only.
func (r Role) IsWrite() bool { return r == RoleOperator || r == RoleApprover }

// Status is the lifecycle state of an entity.
type Status string

const (
	StatusActive    Status = "ACTIVE"
	StatusSealed    Status = "SEALED"
	StatusDestroyed Status = "DESTROYED"
)

// Kind distinguishes mother samples from derived tubes.
type Kind string

const (
	KindMother Kind = "MOTHER"
	KindTube   Kind = "TUBE"
)

// Principal is the actor issuing a command.
type Principal struct {
	Operator   string `json:"operator"`
	Department string `json:"department"`
	Role       Role   `json:"role"`
}

// Entity is a mother sample or an aliquot tube. Both share the same structure
// because the conservation invariant treats them uniformly; the Kind field
// records which is which.
type Entity struct {
	ID              string `json:"id"`
	Kind            Kind   `json:"kind"`
	ParentID        string `json:"parent_id,omitempty"`
	Source          string `json:"source,omitempty"`
	InitialVolume   int    `json:"initial_volume"`
	Available       int    `json:"available"`
	Consumed        int    `json:"consumed"`
	AliquotedOut    int    `json:"aliquoted_out"`
	DestroyedVolume int    `json:"destroyed_volume,omitempty"`
	CustodyDept     string `json:"custody_dept,omitempty"`
	StationID       string `json:"station_id,omitempty"`
	Status          Status `json:"status"`
	CreatedSeq      uint64 `json:"created_seq"`
}

// IsTerminal reports whether the entity is in an irreversible state.
func (e *Entity) IsTerminal() bool { return e.Status == StatusDestroyed }

// OnStation reports whether the entity is currently loaded on a station.
func (e *Entity) OnStation() bool { return e.StationID != "" }

// InCustody reports whether a department currently holds the entity.
func (e *Entity) InCustody() bool { return e.CustodyDept != "" }

// Station is a lab-station task: a tube loaded by an operator, the approved
// consumption recorded against it and whether the task is still active. A
// station identifier is unique per task; an ended task may not be reopened.
type Station struct {
	ID                  string `json:"id"`
	LoadedTubeID        string `json:"loaded_tube_id"`
	Operator            string `json:"operator"`
	Department          string `json:"department"`
	ApprovedConsumption int    `json:"approved_consumption"`
	Active              bool   `json:"active"`
}

// Seal records the freezing of an entity pending destruction.
type Seal struct {
	ID       string `json:"id"`
	EntityID string `json:"entity_id"`
	Approver string `json:"approver"`
	Reason   string `json:"reason"`
	Seq      uint64 `json:"seq"`
}

// Destruction records the irreversible destruction of an entity.
type Destruction struct {
	ID       string `json:"id"`
	EntityID string `json:"entity_id"`
	Approver string `json:"approver"`
	Reason   string `json:"reason"`
	Volume   int    `json:"volume"`
	Seq      uint64 `json:"seq"`
}

// Family is the aggregate root and the consistency boundary. All commands
// against a family execute within its lock and bump its Revision.
type Family struct {
	FamilyID       string              `json:"family_id"`
	Revision       uint64              `json:"revision"`
	Mother         *Entity             `json:"mother"`
	Tubes          map[string]*Entity  `json:"tubes"`
	Stations       map[string]*Station `json:"stations"`
	Seals          []Seal              `json:"seals"`
	Destructions   []Destruction       `json:"destructions"`
	ConsumedTotal  int                 `json:"consumed_total"`
	DestroyedTotal int                 `json:"destroyed_total"`
}

// NewFamily returns an empty, unregistered family.
func NewFamily(id string) *Family {
	return &Family{
		FamilyID: id,
		Tubes:    map[string]*Entity{},
		Stations: map[string]*Station{},
	}
}

// entity returns the entity with the given id (mother or tube).
func (f *Family) entity(id string) *Entity {
	if f.Mother != nil && f.Mother.ID == id {
		return f.Mother
	}
	return f.Tubes[id]
}

// allEntities returns mother (if present) followed by tubes in id order.
func (f *Family) allEntities() []*Entity {
	var out []*Entity
	if f.Mother != nil {
		out = append(out, f.Mother)
	}
	ids := make([]string, 0, len(f.Tubes))
	for id := range f.Tubes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		out = append(out, f.Tubes[id])
	}
	return out
}

// lineage returns the chain of ancestor entity ids from the given entity up to
// the mother, inclusive of the starting entity.
func (f *Family) lineage(id string) []string {
	var chain []string
	cur := id
	for cur != "" {
		ent := f.entity(cur)
		if ent == nil {
			break
		}
		chain = append(chain, cur)
		cur = ent.ParentID
	}
	return chain
}

// wouldCycle reports whether making child a descendant of parent would create
// a cycle. Because aliquot always creates a brand-new tube, this is only
// non-trivial as a defensive check; it returns true if parent is already a
// descendant of child (which cannot happen for a new child but is checked for
// robustness).
func (f *Family) wouldCycle(parent, child string) bool {
	if parent == "" || child == "" {
		return false
	}
	if parent == child {
		return true
	}
	for _, aid := range f.lineage(parent) {
		if aid == child {
			return true
		}
	}
	return false
}
