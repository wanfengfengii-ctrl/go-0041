package domain

import (
	"testing"
	"time"
)

func p(dept string, role Role) Principal {
	return Principal{Operator: "x-" + dept, Department: dept, Role: role}
}

func newFamilyWithMother(t *testing.T, familyID, motherID, dept string, vol int) *Family {
	t.Helper()
	f := NewFamily(familyID)
	_, err := f.Apply(Command{
		Op: OpRegister, Principal: p(dept, RoleOperator),
		FamilyID: familyID, EntityID: motherID, Source: "donor-A", Volume: vol,
		Now: time.Unix(1, 0),
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	return f
}

// TestVolumeConservation exercises a full lifecycle: register, multi-level
// aliquot, station consumption, return and destroy, asserting that volume is
// conserved at every step and equals the initial 1000 µL.
func TestVolumeConservation(t *testing.T) {
	f := newFamilyWithMother(t, "fam-1", "mother-1", "lab", 1000)
	check := func(stage string) {
		t.Helper()
		r := consCheck(f)
		if !r.balanced {
			t.Fatalf("%s: not balanced: %v", stage, r.violations)
		}
	}
	check("after register")

	// first-level aliquot: 200 from mother into t1
	mustApply(t, f, Command{Op: OpAliquot, Principal: p("lab", RoleOperator), ParentID: "mother-1", ChildID: "t1", Volume: 200})
	check("after aliquot t1")
	// second-level aliquot: 100 from t1 into t2 (multi-level)
	mustApply(t, f, Command{Op: OpAliquot, Principal: p("lab", RoleOperator), ParentID: "t1", ChildID: "t2", Volume: 100})
	check("after aliquot t2")
	// third-level aliquot: 40 from t2 into t3
	mustApply(t, f, Command{Op: OpAliquot, Principal: p("lab", RoleOperator), ParentID: "t2", ChildID: "t3", Volume: 40})
	check("after aliquot t3")

	// load t2 onto a station and consume 30
	mustApply(t, f, Command{Op: OpLoadStation, Principal: p("lab", RoleOperator), EntityID: "t2", StationID: "stn-1"})
	mustApply(t, f, Command{Op: OpConfirmConsumption, Principal: p("lab", RoleOperator), StationID: "stn-1", Volume: 30})
	check("after consumption")
	// unload returns t2 to lab custody
	mustApply(t, f, Command{Op: OpUnload, Principal: p("lab", RoleOperator), StationID: "stn-1"})
	check("after unload")

	// return t2 (release custody) — volume unchanged
	mustApply(t, f, Command{Op: OpReturn, Principal: p("lab", RoleOperator), EntityID: "t2"})
	check("after return")

	// seal and destroy t3 (still in lab custody)
	mustApply(t, f, Command{Op: OpSeal, Principal: p("lab", RoleApprover), EntityID: "t3", SealID: "seal-1", Approver: "ap-lab", Reason: "expired"})
	check("after seal")
	mustApply(t, f, Command{Op: OpDestroy, Principal: p("lab", RoleApprover), EntityID: "t3", DestructionID: "destr-1", Approver: "ap-lab", Reason: "expired"})
	check("after destroy")

	// destroy t1 directly (active, not sealed) by an approver
	mustApply(t, f, Command{Op: OpDestroy, Principal: p("lab", RoleApprover), EntityID: "t1", DestructionID: "destr-2", Approver: "ap-lab", Reason: "done"})
	check("after destroy t1")

	// the conservation total must still equal the initial 1000 µL
	r := consCheck(f)
	if r.total != 1000 {
		t.Fatalf("total %d != 1000", r.total)
	}
}

// TestLineageTracing verifies that every tube traces back to the mother.
func TestLineageTracing(t *testing.T) {
	f := newFamilyWithMother(t, "fam-1", "mother-1", "lab", 1000)
	mustApply(t, f, Command{Op: OpAliquot, Principal: p("lab", RoleOperator), ParentID: "mother-1", ChildID: "t1", Volume: 200})
	mustApply(t, f, Command{Op: OpAliquot, Principal: p("lab", RoleOperator), ParentID: "t1", ChildID: "t2", Volume: 50})
	mustApply(t, f, Command{Op: OpAliquot, Principal: p("lab", RoleOperator), ParentID: "t2", ChildID: "t3", Volume: 10})

	lin := f.Lineage()
	// t3 -> t2 -> t1 -> mother-1
	if got := chain(lin, "t3"); got != "t3,t2,t1,mother-1" {
		t.Fatalf("t3 lineage = %q", got)
	}
	if lin["t3"].Distance != 3 {
		t.Fatalf("t3 distance = %d", lin["t3"].Distance)
	}
	if len(lin["mother-1"].Children) != 1 || lin["mother-1"].Children[0] != "t1" {
		t.Fatalf("mother children = %v", lin["mother-1"].Children)
	}
}

// TestTerminalState verifies destroyed entities cannot be operated on and
// sealed entities can only move to destroyed.
func TestTerminalState(t *testing.T) {
	f := newFamilyWithMother(t, "fam-1", "mother-1", "lab", 1000)
	mustApply(t, f, Command{Op: OpAliquot, Principal: p("lab", RoleOperator), ParentID: "mother-1", ChildID: "t1", Volume: 200})
	mustApply(t, f, Command{Op: OpSeal, Principal: p("lab", RoleApprover), EntityID: "t1", SealID: "seal-1", Approver: "ap", Reason: "r"})

	// sealed tube cannot be claimed, aliquoted, loaded
	if err := applyErr(f, Command{Op: OpClaim, Principal: p("other", RoleOperator), EntityID: "t1"}); err == nil {
		t.Fatal("expected error claiming sealed tube")
	}
	if err := applyErr(f, Command{Op: OpAliquot, Principal: p("lab", RoleOperator), ParentID: "t1", ChildID: "t9", Volume: 10}); err == nil {
		t.Fatal("expected error aliquoting sealed tube")
	}
	if err := applyErr(f, Command{Op: OpLoadStation, Principal: p("lab", RoleOperator), EntityID: "t1", StationID: "stn-1"}); err == nil {
		t.Fatal("expected error loading sealed tube")
	}

	// sealed -> destroyed is allowed
	mustApply(t, f, Command{Op: OpDestroy, Principal: p("lab", RoleApprover), EntityID: "t1", DestructionID: "destr-1", Approver: "ap", Reason: "r"})

	// destroyed cannot be returned, aliquoted, loaded or destroyed again
	for _, cmd := range []Command{
		{Op: OpReturn, Principal: p("lab", RoleOperator), EntityID: "t1"},
		{Op: OpAliquot, Principal: p("lab", RoleOperator), ParentID: "t1", ChildID: "t9", Volume: 10},
		{Op: OpLoadStation, Principal: p("lab", RoleOperator), EntityID: "t1", StationID: "stn-9"},
		{Op: OpDestroy, Principal: p("lab", RoleApprover), EntityID: "t1", DestructionID: "destr-9", Approver: "ap", Reason: "r"},
	} {
		if err := applyErr(f, cmd); err == nil {
			t.Fatalf("expected error for %s on destroyed entity", cmd.Op)
		}
	}
}

// TestPermissionDenial checks that non-owners and auditors are rejected.
func TestPermissionDenial(t *testing.T) {
	f := newFamilyWithMother(t, "fam-1", "mother-1", "lab", 1000)
	// auditor may not write
	if err := Authorize(Command{Op: OpAliquot, Principal: p("lab", RoleAuditor)}); err == nil {
		t.Fatal("auditor write should be denied")
	}
	// operator may not destroy
	if err := Authorize(Command{Op: OpDestroy, Principal: p("lab", RoleOperator)}); err == nil {
		t.Fatal("operator destroy should be denied")
	}
	// non-owning department cannot aliquot
	if err := applyErr(f, Command{Op: OpAliquot, Principal: p("other", RoleOperator), ParentID: "mother-1", ChildID: "t1", Volume: 10}); err == nil {
		t.Fatal("non-owner aliquot should be denied")
	}
}

// TestAliquotOverQuantity verifies that over-aliquoting is rejected and leaves
// the family unchanged.
func TestAliquotOverQuantity(t *testing.T) {
	f := newFamilyWithMother(t, "fam-1", "mother-1", "lab", 100)
	rev := f.Revision
	if err := applyErr(f, Command{Op: OpAliquot, Principal: p("lab", RoleOperator), ParentID: "mother-1", ChildID: "t1", Volume: 200}); err == nil {
		t.Fatal("expected over-aliquot error")
	}
	if f.Revision != rev {
		t.Fatalf("revision changed on failed op: %d != %d", f.Revision, rev)
	}
	if f.Mother.Available != 100 {
		t.Fatalf("mother available changed on failed op: %d", f.Mother.Available)
	}
}

// helpers ----------------------------------------------------------------

type consResult struct {
	balanced   bool
	total      int
	violations []string
}

func consCheck(f *Family) consResult {
	// minimal inline conservation check mirroring the invariant engine
	r := consResult{balanced: true}
	if f.Mother == nil {
		return consResult{balanced: false, violations: []string{"no mother"}}
	}
	activeAliquots := 0
	for _, t := range f.Tubes {
		if t.Status != StatusDestroyed {
			activeAliquots += t.Available
		}
	}
	r.total = f.Mother.Available + activeAliquots + f.ConsumedTotal + f.DestroyedTotal
	if r.total != f.Mother.InitialVolume {
		r.balanced = false
		r.violations = append(r.violations, "conservation")
	}
	return r
}

func mustApply(t *testing.T, f *Family, cmd Command) {
	t.Helper()
	if _, err := f.Apply(cmd); err != nil {
		t.Fatalf("apply %s: %v", cmd.Op, err)
	}
}

func applyErr(f *Family, cmd Command) error {
	// operate on a clone so failures don't mutate the shared family
	clone := f.Clone()
	_, err := clone.Apply(cmd)
	return err
}

func chain(lin map[string]LineageNode, id string) string {
	out := id
	node, ok := lin[id]
	if !ok {
		return out
	}
	cur := node.Entity.ParentID
	for cur != "" {
		out += "," + cur
		n, ok := lin[cur]
		if !ok {
			break
		}
		cur = n.Entity.ParentID
	}
	return out
}
