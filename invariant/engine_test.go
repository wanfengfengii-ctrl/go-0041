package invariant

import (
	"testing"
	"time"

	"specimen-custody-graph/domain"
)

func p(dept string, role domain.Role) domain.Principal {
	return domain.Principal{Operator: "x-" + dept, Department: dept, Role: role}
}

func buildFamily(t *testing.T) *domain.Family {
	t.Helper()
	f := domain.NewFamily("fam-1")
	apply := func(cmd domain.Command) {
		t.Helper()
		if _, err := f.Apply(cmd); err != nil {
			t.Fatalf("apply %s: %v", cmd.Op, err)
		}
	}
	apply(domain.Command{Op: domain.OpRegister, Principal: p("lab", domain.RoleOperator), FamilyID: "fam-1", EntityID: "m1", Source: "s", Volume: 1000, Now: time.Unix(1, 0)})
	apply(domain.Command{Op: domain.OpAliquot, Principal: p("lab", domain.RoleOperator), ParentID: "m1", ChildID: "t1", Volume: 200})
	apply(domain.Command{Op: domain.OpAliquot, Principal: p("lab", domain.RoleOperator), ParentID: "t1", ChildID: "t2", Volume: 50})
	apply(domain.Command{Op: domain.OpLoadStation, Principal: p("lab", domain.RoleOperator), EntityID: "t2", StationID: "st1"})
	apply(domain.Command{Op: domain.OpConfirmConsumption, Principal: p("lab", domain.RoleOperator), StationID: "st1", Volume: 20})
	apply(domain.Command{Op: domain.OpUnload, Principal: p("lab", domain.RoleOperator), StationID: "st1"})
	apply(domain.Command{Op: domain.OpSeal, Principal: p("lab", domain.RoleApprover), EntityID: "t1", SealID: "seal-1", Approver: "ap", Reason: "r"})
	apply(domain.Command{Op: domain.OpDestroy, Principal: p("lab", domain.RoleApprover), EntityID: "t1", DestructionID: "d1", Approver: "ap", Reason: "r"})
	return f
}

func TestInvariantBalanced(t *testing.T) {
	f := buildFamily(t)
	r := Check(f)
	if !r.Balanced {
		t.Fatalf("expected balanced, got violations: %v", r.Violations)
	}
	if r.Total != 1000 {
		t.Fatalf("total = %d, want 1000", r.Total)
	}
	if r.Consumed != 20 {
		t.Fatalf("consumed = %d, want 20", r.Consumed)
	}
	if r.Destroyed != 150 { // t1 destroyed with 150 available (200-50 aliquoted out)
		t.Fatalf("destroyed = %d, want 150", r.Destroyed)
	}
	if r.ActiveAliquots != 30 { // t2 available 30 (50-20 consumed); t1 destroyed excluded
		t.Fatalf("active aliquots = %d, want 30", r.ActiveAliquots)
	}
	if r.MotherAvailable != 800 {
		t.Fatalf("mother available = %d, want 800", r.MotherAvailable)
	}
}

func TestInvariantDetectsCorruption(t *testing.T) {
	f := buildFamily(t)
	// tamper: mother available inflated without a matching event
	f.Mother.Available += 100
	r := Check(f)
	if r.Balanced {
		t.Fatal("expected imbalance after tampering mother available")
	}
}

func TestInvariantDetectsCustodyConflict(t *testing.T) {
	f := buildFamily(t)
	// tamper: a tube simultaneously in custody and on a station
	f.Tubes["t2"].CustodyDept = "lab"
	f.Tubes["t2"].StationID = "st1"
	r := Check(f)
	if r.Balanced {
		t.Fatal("expected imbalance after custody conflict")
	}
}
