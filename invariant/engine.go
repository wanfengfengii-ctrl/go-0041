// Package invariant verifies the custody invariants that the specimen
// family must satisfy at all times. The engine recomputes every quantity from
// first principles using integer arithmetic, so a passing report is a proof
// that the family's state is internally consistent — there are no floating
// point accumulations to drift.
package invariant

import (
	"fmt"
	"sort"

	"specimen-custody-graph/domain"
)

// Report is the result of checking a family. Balanced is true when every
// invariant holds; otherwise Violations lists the failures. The volume
// breakdown is always populated so callers (e.g. the reconciliation HTTP
// endpoint) can present it even when balanced.
type Report struct {
	FamilyID        string   `json:"family_id"`
	Balanced        bool     `json:"balanced"`
	Violations      []string `json:"violations,omitempty"`
	MotherInitial   int      `json:"mother_initial"`
	MotherAvailable int      `json:"mother_available"`
	ActiveAliquots  int      `json:"active_aliquots"`
	Consumed        int      `json:"consumed"`
	Destroyed       int      `json:"destroyed"`
	Total           int      `json:"total"`
}

// Check recomputes the conservation and consistency invariants for a family.
//
//  1. Conservation: mother initial == mother available + sum(active tubes'
//     available) + consumed total + destroyed total.
//  2. Consumed total == sum of every entity's consumed volume.
//  3. Destroyed total == sum of every entity's destroyed volume.
//  4. Per-entity: initial == available + consumed + aliquoted out + destroyed.
//  5. Custody: an entity is held by at most one of {department, station}.
//  6. Acyclicity: every tube's ancestor chain terminates at the mother.
func Check(f *domain.Family) Report {
	r := Report{FamilyID: f.FamilyID, Balanced: true}
	if f.Mother == nil {
		r.Balanced = false
		r.Violations = append(r.Violations, "family has no mother sample")
		return r
	}
	r.MotherInitial = f.Mother.InitialVolume
	r.MotherAvailable = f.Mother.Available
	r.Consumed = f.ConsumedTotal
	r.Destroyed = f.DestroyedTotal

	// active aliquot available volume
	tubeIDs := make([]string, 0, len(f.Tubes))
	for id := range f.Tubes {
		tubeIDs = append(tubeIDs, id)
	}
	sort.Strings(tubeIDs)
	for _, id := range tubeIDs {
		t := f.Tubes[id]
		if t.Status != domain.StatusDestroyed {
			r.ActiveAliquots += t.Available
		}
	}
	r.Total = r.MotherAvailable + r.ActiveAliquots + r.Consumed + r.Destroyed
	if r.MotherInitial != r.Total {
		r.Balanced = false
		r.Violations = append(r.Violations,
			fmt.Sprintf("conservation violated: initial %d != sum %d (mother=%d aliquots=%d consumed=%d destroyed=%d)",
				r.MotherInitial, r.Total, r.MotherAvailable, r.ActiveAliquots, r.Consumed, r.Destroyed))
	}

	// consumed total cross-check
	sumConsumed := f.Mother.Consumed
	for _, t := range f.Tubes {
		sumConsumed += t.Consumed
	}
	if sumConsumed != f.ConsumedTotal {
		r.Balanced = false
		r.Violations = append(r.Violations,
			fmt.Sprintf("consumed total %d != recomputed %d", f.ConsumedTotal, sumConsumed))
	}

	// destroyed total cross-check
	sumDestroyed := f.Mother.DestroyedVolume
	for _, t := range f.Tubes {
		sumDestroyed += t.DestroyedVolume
	}
	if sumDestroyed != f.DestroyedTotal {
		r.Balanced = false
		r.Violations = append(r.Violations,
			fmt.Sprintf("destroyed total %d != recomputed %d", f.DestroyedTotal, sumDestroyed))
	}

	// per-entity conservation
	checkEntity := func(e *domain.Entity) {
		want := e.InitialVolume
		got := e.Available + e.Consumed + e.AliquotedOut + e.DestroyedVolume
		if want != got {
			r.Balanced = false
			r.Violations = append(r.Violations,
				fmt.Sprintf("entity %s: initial %d != available+consumed+aliquoted+destroyed %d", e.ID, want, got))
		}
		if e.CustodyDept != "" && e.StationID != "" {
			r.Balanced = false
			r.Violations = append(r.Violations,
				fmt.Sprintf("entity %s: held by both department %q and station %q", e.ID, e.CustodyDept, e.StationID))
		}
		if e.Status == domain.StatusDestroyed && e.Available != 0 {
			r.Balanced = false
			r.Violations = append(r.Violations,
				fmt.Sprintf("entity %s: destroyed but available=%d", e.ID, e.Available))
		}
	}
	checkEntity(f.Mother)
	for _, t := range f.Tubes {
		checkEntity(t)
	}

	// acyclicity: every tube's ancestor chain terminates at the mother
	for _, id := range tubeIDs {
		t := f.Tubes[id]
		seen := map[string]bool{}
		cur := t.ParentID
		for cur != "" {
			if seen[cur] {
				r.Balanced = false
				r.Violations = append(r.Violations, fmt.Sprintf("cycle detected at %s", id))
				break
			}
			seen[cur] = true
			if cur == f.Mother.ID {
				break
			}
			parent := f.Tubes[cur]
			if parent == nil {
				r.Balanced = false
				r.Violations = append(r.Violations, fmt.Sprintf("tube %s has dangling parent %s", id, cur))
				break
			}
			cur = parent.ParentID
		}
		if cur == "" && t.ParentID != "" {
			r.Balanced = false
			r.Violations = append(r.Violations, fmt.Sprintf("tube %s ancestry does not reach mother", id))
		}
	}

	// station invariants: active station tube must be on station
	for _, st := range f.Stations {
		if st.Active {
			tube := f.Tubes[st.LoadedTubeID]
			if tube == nil || tube.StationID != st.ID {
				r.Balanced = false
				r.Violations = append(r.Violations,
					fmt.Sprintf("station %s: loaded tube not on station", st.ID))
			}
		}
	}

	if len(r.Violations) == 0 {
		r.Balanced = true
	}
	return r
}
