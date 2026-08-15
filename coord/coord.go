// Package coord coordinates concurrent access to specimen families. Each
// family is guarded by its own mutex (lock sharding), and every command
// carries an expected revision so that stale writes are rejected. A command —
// or a LIMS batch — executes within a transaction: the affected family locks
// are acquired in sorted order, the commands are validated and applied to a
// working clone, the resulting events are appended and synced, and only then
// is the published state swapped in. If persistence fails the working clone is
// discarded, so the in-memory state and revision are never published ahead of
// the durable log.
package coord

import (
	"sort"
	"sync"
	"time"

	"specimen-custody-graph/domain"
	"specimen-custody-graph/eventstore"
	"specimen-custody-graph/infra"
	"specimen-custody-graph/scerr"
)

// Coordinator owns the published family state and serialises commands.
type Coordinator struct {
	mu      sync.Mutex
	slots   map[string]*familySlot
	store   *eventstore.Store
	clock   infra.Clock
	idgen   infra.IDGen
	barrier infra.Barrier

	// appliedSeq is the highest event sequence reflected in published state.
	appliedSeq uint64
	seqMu      sync.RWMutex
}

type familySlot struct {
	mu     sync.Mutex
	family *domain.Family
}

// Options configures a Coordinator.
type Options struct {
	Store   *eventstore.Store
	Clock   infra.Clock
	IDGen   infra.IDGen
	Barrier infra.Barrier
}

// New constructs a Coordinator. The published state is populated from seed
// (typically the result of recovery); nil families are created on demand.
func New(opts Options, seed map[string]*domain.Family, appliedSeq uint64) *Coordinator {
	if opts.Clock == nil {
		opts.Clock = infra.RealClock{}
	}
	if opts.IDGen == nil {
		opts.IDGen = infra.NewRandIDGen("scg")
	}
	if opts.Barrier == nil {
		opts.Barrier = infra.NoopBarrier{}
	}
	slots := map[string]*familySlot{}
	for id, f := range seed {
		slots[id] = &familySlot{family: f}
	}
	return &Coordinator{
		slots:      slots,
		store:      opts.Store,
		clock:      opts.Clock,
		idgen:      opts.IDGen,
		barrier:    opts.Barrier,
		appliedSeq: appliedSeq,
	}
}

// slot returns the slot for familyID, creating an empty one if needed. The
// caller must NOT hold c.mu when calling slotFor (it acquires c.mu).
func (c *Coordinator) slotFor(familyID string) *familySlot {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.slots[familyID]
	if !ok {
		s = &familySlot{family: domain.NewFamily(familyID)}
		c.slots[familyID] = s
	}
	return s
}

// IDGen returns the configured identifier generator (used by callers that
// need to pre-allocate ids such as seal/destruction ids).
func (c *Coordinator) IDGen() infra.IDGen { return c.idgen }

// Submit applies a single command. It performs authorisation, the expected
// revision check, validation, persistence and publication.
func (c *Coordinator) Submit(cmd domain.Command) (domain.Event, error) {
	cmd.Now = c.clock.Now()
	if cmd.FamilyID == "" {
		cmd.FamilyID = c.idgen.NewFamilyID()
	}
	// authorise role
	if err := domain.Authorize(cmd); err != nil {
		return domain.Event{}, err
	}
	events, err := c.execute([]domain.Command{cmd})
	if err != nil {
		return domain.Event{}, err
	}
	if len(events) == 0 {
		return domain.Event{}, scerr.New(scerr.CodeStorage, "no event produced").WithOperation(cmd.Op)
	}
	return events[0], nil
}

// SubmitBatch applies a sequence of commands atomically: all succeed and are
// persisted, or none are. It is used by the LIMS batch adapter. Within a batch
// the expected-revision check is relaxed (each command uses the working
// revision) because the batch holds the family locks for its duration.
// Authorisation is performed per-record inside execute so that a batch with
// multiple invalid records reports all of them rather than stopping at the
// first.
func (c *Coordinator) SubmitBatch(cmds []domain.Command) ([]domain.Event, error) {
	now := c.clock.Now()
	for i := range cmds {
		cmds[i].Now = now
		if cmds[i].FamilyID == "" {
			cmds[i].FamilyID = c.idgen.NewFamilyID()
		}
	}
	return c.execute(cmds)
}

// execute runs the commands as one transaction.
func (c *Coordinator) execute(cmds []domain.Command) ([]domain.Event, error) {
	// group commands by family preserving first-seen order for lock acquisition
	famOrder := []string{}
	famCmds := map[string][]int{}
	for i, cmd := range cmds {
		if _, ok := famCmds[cmd.FamilyID]; !ok {
			famOrder = append(famOrder, cmd.FamilyID)
		}
		famCmds[cmd.FamilyID] = append(famCmds[cmd.FamilyID], i)
	}
	// acquire locks in sorted family-id order to avoid deadlock
	sort.Strings(famOrder)

	c.barrier.Arrive(cmds[0].FamilyID)

	slots := make([]*familySlot, len(famOrder))
	for i, id := range famOrder {
		slots[i] = c.slotFor(id)
		slots[i].mu.Lock()
	}
	defer func() {
		for i := len(slots) - 1; i >= 0; i-- {
			slots[i].mu.Unlock()
		}
	}()

	// published state before the transaction (the source of truth for rollback)
	published := make(map[string]*domain.Family, len(famOrder))
	for i, id := range famOrder {
		published[id] = slots[i].family
	}

	// working clones; validation advances these so dependent commands see the
	// effects of earlier ones.
	working := map[string]*domain.Family{}
	for _, id := range famOrder {
		working[id] = published[id].Clone()
	}

	events := make([]domain.Event, 0, len(cmds))
	var problems []scerr.Detail
	for i, cmd := range cmds {
		fam := working[cmd.FamilyID]
		// batch mode: relax revision (use working). single command: enforce.
		if len(cmds) == 1 {
			if cmd.ExpectedRevision != 0 && fam.Revision != cmd.ExpectedRevision {
				return nil, scerr.New(scerr.CodeRevisionConflict,
					"expected revision does not match current").
					WithOperation(cmd.Op).WithEntity(cmd.EntityID)
			}
		} else {
			cmd.ExpectedRevision = fam.Revision
		}
		// authorise per-record; in batch mode failures become per-record details
		if err := domain.Authorize(cmd); err != nil {
			if len(cmds) == 1 {
				return nil, err
			}
			se := scerr.As(err)
			if se != nil {
				problems = append(problems, scerr.Detail{
					RecordIndex: i, Code: se.Code, Message: se.Message, Field: se.Field,
				})
			} else {
				problems = append(problems, scerr.Detail{
					RecordIndex: i, Code: scerr.CodeConflict, Message: err.Error(),
				})
			}
			continue
		}
		ev, err := fam.Apply(cmd)
		if err != nil {
			if len(cmds) == 1 {
				return nil, err
			}
			se := scerr.As(err)
			if se != nil {
				problems = append(problems, scerr.Detail{
					RecordIndex: i, Code: se.Code, Message: se.Message, Field: se.Field,
				})
			} else {
				problems = append(problems, scerr.Detail{
					RecordIndex: i, Code: scerr.CodeConflict, Message: err.Error(),
				})
			}
			continue
		}
		events = append(events, ev)
	}
	if len(problems) > 0 {
		return nil, scerr.New(scerr.CodeBatchPartialInvalid,
			"batch rejected: one or more records are invalid").
			WithDetails(problems).WithRetryable(false)
	}

	// persist
	persisted, err := c.store.Append(events)
	if err != nil {
		return nil, err
	}

	// Re-apply the persisted events (now carrying their assigned sequence
	// numbers) to fresh clones of the pre-transaction state. The earlier
	// validation pass applied events with Seq=0 because the store assigns the
	// real sequence during Append; re-applying makes sequence-derived fields
	// (entity CreatedSeq, Seal.Seq, Destruction.Seq) match what recovery would
	// produce, so the published and recovered digests agree.
	final := map[string]*domain.Family{}
	for _, id := range famOrder {
		final[id] = published[id].Clone()
	}
	for _, ev := range persisted {
		if err := final[ev.FamilyID].ApplyEvent(ev); err != nil {
			return nil, scerr.New(scerr.CodeStorage, "re-apply persisted event: "+err.Error()).
				WithRetryable(true)
		}
	}

	// publish: swap final clones into slots
	for i, id := range famOrder {
		slots[i].family = final[id]
	}
	c.seqMu.Lock()
	c.appliedSeq = persisted[len(persisted)-1].Seq
	c.seqMu.Unlock()
	return persisted, nil
}

// Family returns a clone of the published family for read-only queries. A nil
// family is returned for an unknown id.
func (c *Coordinator) Family(familyID string) *domain.Family {
	slot := c.slotFor(familyID)
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.family == nil || slot.family.Mother == nil {
		return nil
	}
	return slot.family.Clone()
}

// Families returns clones of all published families, keyed by id.
func (c *Coordinator) Families() map[string]*domain.Family {
	c.mu.Lock()
	ids := make([]string, 0, len(c.slots))
	for id, s := range c.slots {
		s.mu.Lock()
		if s.family != nil && s.family.Mother != nil {
			ids = append(ids, id)
		}
		s.mu.Unlock()
	}
	c.mu.Unlock()
	out := map[string]*domain.Family{}
	for _, id := range ids {
		out[id] = c.Family(id)
	}
	return out
}

// AppliedSeq returns the highest applied event sequence.
func (c *Coordinator) AppliedSeq() uint64 {
	c.seqMu.RLock()
	defer c.seqMu.RUnlock()
	return c.appliedSeq
}

// Now returns the current clock time.
func (c *Coordinator) Now() time.Time { return c.clock.Now() }

// Snapshot writes a snapshot of all published families anchored at the current
// applied sequence.
func (c *Coordinator) Snapshot(snaps *eventstore.SnapshotStore) error {
	fams := c.Families()
	return snaps.Write(fams, c.AppliedSeq())
}

// DigestAll returns the canonical digest of all published families, computed
// with the same function as the snapshot store and recovery so that the three
// paths can be compared directly.
func (c *Coordinator) DigestAll() (string, error) {
	fams := c.Families()
	return eventstore.DigestFamilies(mapToSortedSlice(fams))
}

func mapToSortedSlice(fams map[string]*domain.Family) []*domain.Family {
	ids := make([]string, 0, len(fams))
	for id := range fams {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*domain.Family, 0, len(ids))
	for _, id := range ids {
		out = append(out, fams[id])
	}
	return out
}

// withRecord annotates a single-command error with its record index for batch
// reporting.
func withRecord(err error, i int) error {
	if se := scerr.As(err); se != nil {
		cp := *se
		cp.RecordIndex = i
		return &cp
	}
	return err
}
