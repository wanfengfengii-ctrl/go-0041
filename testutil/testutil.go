// Package testutil provides shared helpers for the specimen-custody-graph test
// suite: a deterministic clock, a sequential id generator, temp-dir backed
// stores and a coordinator wired with fakes. Using these helpers keeps tests
// free of real time, random ids, fixed ports and sleeps.
package testutil

import (
	"testing"
	"time"

	"specimen-custody-graph/coord"
	"specimen-custody-graph/domain"
	"specimen-custody-graph/eventstore"
	"specimen-custody-graph/infra"
)

// System bundles a coordinator with its stores and injection points.
type System struct {
	Coord   *coord.Coordinator
	Store   *eventstore.Store
	Snaps   *eventstore.SnapshotStore
	Clock   infra.FixedClock
	IDGen   *infra.SeqIDGen
	Syncer  *infra.FailSyncer
	Barrier *infra.CountBarrier
	Dir     string
}

// NewSystem creates a fresh system backed by a temp directory. The syncer is
// a FailSyncer with FailCount 0 (i.e. a real syncer) so tests can flip it to
// inject failures. The barrier is disarmed by default.
func NewSystem(t *testing.T) *System {
	t.Helper()
	dir := t.TempDir()
	syncer := &infra.FailSyncer{FailCount: 0, Inner: infra.RealSyncer{}}
	store, err := eventstore.Open(dir+"/events.log", syncer)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	snaps := eventstore.NewSnapshotStore(dir+"/snapshot.json", syncer)
	clock := infra.FixedClock{T: time.Unix(1_700_000_000, 0).UTC()}
	idgen := infra.NewSeqIDGen("t")
	barrier := infra.NewCountBarrier()
	co := coord.New(coord.Options{
		Store:   store,
		Clock:   clock,
		IDGen:   idgen,
		Barrier: barrier,
	}, nil, 0)
	t.Cleanup(func() {
		_ = store.Close()
	})
	return &System{
		Coord:   co,
		Store:   store,
		Snaps:   snaps,
		Clock:   clock,
		IDGen:   idgen,
		Syncer:  syncer,
		Barrier: barrier,
		Dir:     dir,
	}
}

// Op is the operator principal for the given department.
func Op(dept string) domain.Principal {
	return domain.Principal{Operator: "op-" + dept, Department: dept, Role: domain.RoleOperator}
}

// Approver is the approver principal for the given department.
func Approver(dept string) domain.Principal {
	return domain.Principal{Operator: "ap-" + dept, Department: dept, Role: domain.RoleApprover}
}

// Auditor is the auditor principal for the given department.
func Auditor(dept string) domain.Principal {
	return domain.Principal{Operator: "au-" + dept, Department: dept, Role: domain.RoleAuditor}
}

// Register submits a register command and fails the test on error.
func Register(s *System, t *testing.T, familyID, motherID, source, dept string, volume int) domain.Event {
	t.Helper()
	ev, err := s.Coord.Submit(domain.Command{
		Op:        domain.OpRegister,
		Principal: Op(dept),
		FamilyID:  familyID,
		EntityID:  motherID,
		Source:    source,
		Volume:    volume,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	return ev
}

// Aliquot submits an aliquot command and fails the test on error.
func Aliquot(s *System, t *testing.T, familyID, parentID, childID, dept string, volume int) domain.Event {
	t.Helper()
	ev, err := s.Coord.Submit(domain.Command{
		Op:        domain.OpAliquot,
		Principal: Op(dept),
		FamilyID:  familyID,
		ParentID:  parentID,
		ChildID:   childID,
		Volume:    volume,
	})
	if err != nil {
		t.Fatalf("aliquot: %v", err)
	}
	return ev
}

// Tick advances the deterministic clock by one second and returns the new
// time. This lets tests order events without sleeping.
func Tick(s *System) time.Time {
	s.Clock.T = s.Clock.T.Add(time.Second)
	return s.Clock.T
}
