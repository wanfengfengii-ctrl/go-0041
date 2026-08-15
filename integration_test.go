package integration_test

import (
	"testing"

	"specimen-custody-graph/domain"
	"specimen-custody-graph/invariant"
	"specimen-custody-graph/recovery"
	"specimen-custody-graph/testutil"
)

// TestAcceptanceVolumeConservation is the headline acceptance test: register a
// 1000 µL mother, perform multi-level aliquot, detection consumption, return
// and destroy, then assert that the volume account strictly equals 1000 µL at
// every step and after recovery, and that lineage traces every tube to the
// mother.
func TestAcceptanceVolumeConservation(t *testing.T) {
	s := testutil.NewSystem(t)
	testutil.Register(s, t, "fam-1", "m1", "donor", "lab", 1000)
	assertBalanced(t, s, 1000)

	testutil.Aliquot(s, t, "fam-1", "m1", "t1", "lab", 200)
	assertBalanced(t, s, 1000)
	testutil.Aliquot(s, t, "fam-1", "t1", "t2", "lab", 50)
	assertBalanced(t, s, 1000)

	// load t2, consume 20, unload
	mustSubmit(t, s, domain.Command{Op: domain.OpLoadStation, Principal: testutil.Op("lab"), FamilyID: "fam-1", EntityID: "t2", StationID: "stn-1"})
	assertBalanced(t, s, 1000)
	mustSubmit(t, s, domain.Command{Op: domain.OpConfirmConsumption, Principal: testutil.Op("lab"), FamilyID: "fam-1", StationID: "stn-1", Volume: 20})
	assertBalanced(t, s, 1000)
	mustSubmit(t, s, domain.Command{Op: domain.OpUnload, Principal: testutil.Op("lab"), FamilyID: "fam-1", StationID: "stn-1"})
	assertBalanced(t, s, 1000)

	// return t2 (release custody)
	mustSubmit(t, s, domain.Command{Op: domain.OpReturn, Principal: testutil.Op("lab"), FamilyID: "fam-1", EntityID: "t2"})
	assertBalanced(t, s, 1000)

	// destroy t1 (active) by an approver
	mustSubmit(t, s, domain.Command{Op: domain.OpDestroy, Principal: testutil.Approver("lab"), FamilyID: "fam-1", EntityID: "t1", DestructionID: "d1", Approver: "ap", Reason: "done"})
	assertBalanced(t, s, 1000)

	// lineage traces t2 -> t1 -> m1
	fam := s.Coord.Family("fam-1")
	lin := fam.Lineage()
	if got := lineageChain(lin, "t2"); got != "t2,t1,m1" {
		t.Fatalf("t2 lineage = %q", got)
	}

	// after recovery, the account still equals 1000 and the digest matches
	pubDigest, err := s.Coord.DigestAll()
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	res, err := recovery.FromLog(s.Store)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if res.Digest != pubDigest {
		t.Fatalf("recovered digest %s != published %s", res.Digest, pubDigest)
	}
	recFam := res.Families["fam-1"]
	rep := invariant.Check(recFam)
	if !rep.Balanced {
		t.Fatalf("recovered family not balanced: %v", rep.Violations)
	}
	if rep.Total != 1000 {
		t.Fatalf("recovered total = %d, want 1000", rep.Total)
	}
}

// TestAcceptanceMultiFamilyDeterminism verifies that recovery produces a stable
// digest across multiple families (order-independent).
func TestAcceptanceMultiFamilyDeterminism(t *testing.T) {
	s := testutil.NewSystem(t)
	testutil.Register(s, t, "fam-a", "ma", "donor", "lab", 500)
	testutil.Register(s, t, "fam-b", "mb", "donor", "lab", 300)
	testutil.Aliquot(s, t, "fam-a", "ma", "ta", "lab", 100)
	testutil.Aliquot(s, t, "fam-b", "mb", "tb", "lab", 50)

	pubDigest, _ := s.Coord.DigestAll()
	// recover twice; both must match the published digest and each other
	r1, _ := recovery.FromLog(s.Store)
	r2, _ := recovery.FromLog(s.Store)
	if r1.Digest != pubDigest || r2.Digest != pubDigest {
		t.Fatalf("digests differ: r1=%s r2=%s pub=%s", r1.Digest, r2.Digest, pubDigest)
	}
}

func assertBalanced(t *testing.T, s *testutil.System, want int) {
	t.Helper()
	fam := s.Coord.Family("fam-1")
	if fam == nil {
		t.Fatal("family not found")
	}
	rep := invariant.Check(fam)
	if !rep.Balanced {
		t.Fatalf("not balanced: %v", rep.Violations)
	}
	if rep.Total != want {
		t.Fatalf("total = %d, want %d", rep.Total, want)
	}
}

func mustSubmit(t *testing.T, s *testutil.System, cmd domain.Command) {
	t.Helper()
	if _, err := s.Coord.Submit(cmd); err != nil {
		t.Fatalf("submit %s: %v", cmd.Op, err)
	}
}

func lineageChain(lin map[string]domain.LineageNode, id string) string {
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
