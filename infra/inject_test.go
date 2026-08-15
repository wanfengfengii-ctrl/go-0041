package infra

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestFixedAndRealClock(t *testing.T) {
	fc := FixedClock{T: time.Unix(42, 0)}
	if !fc.Now().Equal(time.Unix(42, 0)) {
		t.Fatal("fixed clock")
	}
	rc := RealClock{}
	if rc.Now().IsZero() {
		t.Fatal("real clock returned zero")
	}
}

func TestSeqIDGen(t *testing.T) {
	g := NewSeqIDGen("t")
	a := g.NewFamilyID()
	b := g.NewFamilyID()
	if a == b {
		t.Fatalf("ids not unique: %s", a)
	}
	if a != "t-fam-1" || b != "t-fam-2" {
		t.Fatalf("unexpected ids: %s %s", a, b)
	}
	if g.NewSealID() != "t-seal-1" {
		t.Fatalf("seal id")
	}
	if g.NewDestructionID() != "t-destr-1" {
		t.Fatalf("destr id")
	}
	if g.NewStationID() != "t-stn-1" {
		t.Fatalf("station id")
	}
}

func TestRandIDGen(t *testing.T) {
	g := NewRandIDGen("scg")
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id := g.NewFamilyID()
		if seen[id] {
			t.Fatalf("duplicate id: %s", id)
		}
		seen[id] = true
	}
}

func TestMapKeyStore(t *testing.T) {
	ks := NewMapKeyStore(map[string][]byte{"k1": []byte("s")})
	got, ok := ks.Lookup("k1")
	if !ok || string(got) != "s" {
		t.Fatalf("lookup k1: %s %v", got, ok)
	}
	if _, ok := ks.Lookup("missing"); ok {
		t.Fatal("missing key should not be found")
	}
	// mutating the returned slice must not affect the store
	got[0] = 'x'
	got2, _ := ks.Lookup("k1")
	if string(got2) != "s" {
		t.Fatal("key store is not copy-safe")
	}
}

func TestFailSyncer(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s := &FailSyncer{FailCount: 2, Inner: RealSyncer{}}
	if err := s.Sync(f); err == nil {
		t.Fatal("expected first sync to fail")
	}
	if err := s.Sync(f); err == nil {
		t.Fatal("expected second sync to fail")
	}
	if err := s.Sync(f); err != nil {
		t.Fatalf("third sync should succeed: %v", err)
	}
	if s.Calls() != 2 {
		t.Fatalf("calls = %d, want 2", s.Calls())
	}
}

// TestCountBarrierConcurrent verifies the barrier releases exactly n concurrent
// arrivals and then disarms.
func TestCountBarrierConcurrent(t *testing.T) {
	b := NewCountBarrier()
	b.Arm(2) // disarmed by default; arm for 2
	var wg sync.WaitGroup
	wg.Add(2)
	started := make(chan struct{}, 2)
	go func() { defer wg.Done(); b.Arrive("k"); started <- struct{}{} }()
	go func() { defer wg.Done(); b.Arrive("k"); started <- struct{}{} }()
	// both must complete (the barrier released them)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("barrier deadlocked")
	}
	// subsequent Arrive is a no-op (disarmed)
	b.Arrive("k")
}
