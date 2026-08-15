// Package infra defines the small injection seams through which the rest of
// the service acquires non-deterministic inputs: the wall clock, identifier
// generation, the HMAC signing key store, file synchronisation and the
// concurrency barrier. Production code wires the real implementations; tests
// wire deterministic fakes so that assertions never depend on real time,
// random identifiers or scheduling timing.
package infra

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"time"
)

// Clock returns a timestamp. Replay never consults the clock — events carry
// their stored timestamp — so the clock is only consulted when creating new
// events or snapshots.
type Clock interface {
	Now() time.Time
}

// RealClock returns the system wall time.
type RealClock struct{}

// Now returns time.Now.
func (RealClock) Now() time.Time { return time.Now() }

// FixedClock always returns the same time. Used by tests.
type FixedClock struct{ T time.Time }

// Now returns the configured time.
func (f FixedClock) Now() time.Time { return f.T }

// IDGen produces identifiers for the entities that the service creates
// autonomously (families, seal records, destruction vouchers and station
// tasks when the caller does not supply one). Lab-assigned identifiers such
// as mother-sample and tube identifiers are supplied by the caller.
type IDGen interface {
	NewFamilyID() string
	NewSealID() string
	NewDestructionID() string
	NewStationID() string
}

// SeqIDGen produces monotonically numbered identifiers with the given prefix.
// It is deterministic, making it suitable for tests.
type SeqIDGen struct {
	prefix string
	mu     sync.Mutex
	fam    int
	seal   int
	destr  int
	stat   int
}

// NewSeqIDGen returns a SeqIDGen that emits identifiers like "<prefix>-fam-1".
func NewSeqIDGen(prefix string) *SeqIDGen {
	if prefix == "" {
		prefix = "scg"
	}
	return &SeqIDGen{prefix: prefix}
}

// NewFamilyID returns the next family identifier.
func (g *SeqIDGen) NewFamilyID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fam++
	return fmt.Sprintf("%s-fam-%d", g.prefix, g.fam)
}

// NewSealID returns the next seal identifier.
func (g *SeqIDGen) NewSealID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.seal++
	return fmt.Sprintf("%s-seal-%d", g.prefix, g.seal)
}

// NewDestructionID returns the next destruction identifier.
func (g *SeqIDGen) NewDestructionID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.destr++
	return fmt.Sprintf("%s-destr-%d", g.prefix, g.destr)
}

// NewStationID returns the next station-task identifier.
func (g *SeqIDGen) NewStationID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stat++
	return fmt.Sprintf("%s-stn-%d", g.prefix, g.stat)
}

// RandIDGen produces random hex identifiers using crypto/rand.
type RandIDGen struct {
	prefix string
}

// NewRandIDGen returns a RandIDGen.
func NewRandIDGen(prefix string) RandIDGen {
	if prefix == "" {
		prefix = "scg"
	}
	return RandIDGen{prefix: prefix}
}

func (g RandIDGen) rand(suffix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%s-%s", g.prefix, suffix, hex.EncodeToString(b[:]))
}

// NewFamilyID returns a random family identifier.
func (g RandIDGen) NewFamilyID() string { return g.rand("fam") }

// NewSealID returns a random seal identifier.
func (g RandIDGen) NewSealID() string { return g.rand("seal") }

// NewDestructionID returns a random destruction identifier.
func (g RandIDGen) NewDestructionID() string { return g.rand("destr") }

// NewStationID returns a random station identifier.
func (g RandIDGen) NewStationID() string { return g.rand("stn") }

// KeyStore maps a key identifier to its HMAC-SHA256 secret.
type KeyStore interface {
	Lookup(keyID string) (secret []byte, ok bool)
}

// MapKeyStore is a simple in-memory key store.
type MapKeyStore struct {
	keys map[string][]byte
}

// NewMapKeyStore returns a MapKeyStore from a key id -> secret map.
func NewMapKeyStore(keys map[string][]byte) *MapKeyStore {
	cp := make(map[string][]byte, len(keys))
	for k, v := range keys {
		cp[k] = append([]byte(nil), v...)
	}
	return &MapKeyStore{keys: cp}
}

// Lookup returns the secret for keyID.
func (m *MapKeyStore) Lookup(keyID string) ([]byte, bool) {
	s, ok := m.keys[keyID]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), s...), true
}

// Syncer flushes file data to durable storage. The event store calls Sync
// after writing frames and after truncating, so that a crash never exposes a
// half-written frame.
type Syncer interface {
	Sync(f *os.File) error
}

// RealSyncer calls f.Sync.
type RealSyncer struct{}

// Sync calls (*os.File).Sync.
func (RealSyncer) Sync(f *os.File) error { return f.Sync() }

// FailSyncer fails the first FailCount Sync calls and then delegates to the
// underlying syncer. It is used to simulate storage sync failures in tests.
type FailSyncer struct {
	FailCount int
	calls     int
	mu        sync.Mutex
	Inner     Syncer
}

// Sync fails until FailCount is exhausted, then succeeds.
func (s *FailSyncer) Sync(f *os.File) error {
	s.mu.Lock()
	if s.calls < s.FailCount {
		s.calls++
		s.mu.Unlock()
		return fmt.Errorf("injected sync failure #%d", s.calls)
	}
	s.mu.Unlock()
	if s.Inner != nil {
		return s.Inner.Sync(f)
	}
	return nil
}

// Calls returns the number of Sync invocations observed.
func (s *FailSyncer) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// Barrier is the concurrency injection point. The coordinator calls Arrive
// immediately before acquiring a family lock, which lets a test guarantee that
// two competing goroutines are both in flight before either commits. The
// production implementation is a no-op.
type Barrier interface {
	Arrive(key string)
}

// NoopBarrier does nothing.
type NoopBarrier struct{}

// Arrive returns immediately.
func (NoopBarrier) Arrive(string) {}

// CountBarrier is a cyclic barrier used by tests to guarantee that competing
// goroutines are both in flight before either commits. It is armed on demand:
// while disarmed (the default) Arrive returns immediately, so the barrier can
// be installed for the whole test without affecting sequential setup. Once
// armed with Arm(n), the first n-1 callers block until the nth arrives, at
// which point all are released and the barrier disarms itself for the next
// round.
type CountBarrier struct {
	mu         sync.Mutex
	cond       *sync.Cond
	armed      bool
	n          int
	count      int
	generation int
}

// NewCountBarrier returns a disarmed barrier.
func NewCountBarrier() *CountBarrier {
	b := &CountBarrier{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// Arm enables the barrier for the next n arrivals.
func (b *CountBarrier) Arm(n int) {
	b.mu.Lock()
	b.armed = true
	b.n = n
	b.count = 0
	b.generation++
	b.cond.Broadcast()
	b.mu.Unlock()
}

// Disarm disables the barrier (Arrive becomes a no-op).
func (b *CountBarrier) Disarm() {
	b.mu.Lock()
	b.armed = false
	b.cond.Broadcast()
	b.mu.Unlock()
}

// Arrive blocks until n armed callers have arrived, then releases all of them.
// When disarmed it returns immediately.
func (b *CountBarrier) Arrive(_ string) {
	b.mu.Lock()
	if !b.armed {
		b.mu.Unlock()
		return
	}
	gen := b.generation
	b.count++
	if b.count >= b.n {
		b.generation++
		b.count = 0
		b.armed = false
		b.cond.Broadcast()
		b.mu.Unlock()
		return
	}
	for b.generation == gen && b.armed {
		b.cond.Wait()
	}
	b.mu.Unlock()
}
