package host

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/space/simplespace"
)

// MemoryStore is a bounded, concurrency-safe Store for tests and explicit
// single-process deployments. It is not durable across process restarts.
type MemoryStore struct {
	mu            sync.Mutex
	maxSpaces     int
	maxMembers    int
	maxWriters    int
	maxOutbox     int
	spaces        map[atmos.SpaceRef]SpaceState
	members       map[atmos.SpaceRef]map[atmos.DID]simplespace.Member
	writers       map[atmos.SpaceRef]map[atmos.DID]Writer
	registrations map[atmos.SpaceRef]map[string]Registration
	outbox        map[uint64]Delivery
	nextDelivery  uint64
	nextLease     uint64
}

// MemoryStoreOptions sets hard in-memory cardinality bounds.
type MemoryStoreOptions struct {
	MaxSpaces  int
	MaxMembers int
	MaxWriters int
	MaxOutbox  int
}

// NewMemoryStore constructs an explicitly bounded memory store.
func NewMemoryStore(opts MemoryStoreOptions) (*MemoryStore, error) {
	if opts.MaxSpaces <= 0 || opts.MaxMembers <= 0 || opts.MaxWriters <= 0 || opts.MaxOutbox <= 0 {
		return nil, errors.New("space host: every memory-store limit must be positive")
	}
	return &MemoryStore{
		maxSpaces: opts.MaxSpaces, maxMembers: opts.MaxMembers,
		maxWriters: opts.MaxWriters, maxOutbox: opts.MaxOutbox,
		spaces:        make(map[atmos.SpaceRef]SpaceState),
		members:       make(map[atmos.SpaceRef]map[atmos.DID]simplespace.Member),
		writers:       make(map[atmos.SpaceRef]map[atmos.DID]Writer),
		registrations: make(map[atmos.SpaceRef]map[string]Registration),
		outbox:        make(map[uint64]Delivery),
	}, nil
}

func (s *MemoryStore) checkContext(ctx context.Context) error {
	if s == nil {
		return errors.New("space host: nil memory store")
	}
	return ctx.Err()
}

// GetSpace implements Store.
func (s *MemoryStore) GetSpace(ctx context.Context, space atmos.SpaceRef) (SpaceState, error) {
	if err := s.checkContext(ctx); err != nil {
		return SpaceState{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.spaces[space]
	if !ok {
		return SpaceState{}, ErrNotFound
	}
	state.Config = state.Config.Clone()
	return state, nil
}

// CreateSpace implements Store. Tombstones are permanent and cannot be reused.
func (s *MemoryStore) CreateSpace(ctx context.Context, config simplespace.Config, now time.Time) (SpaceState, error) {
	if err := s.checkContext(ctx); err != nil {
		return SpaceState{}, err
	}
	if err := config.Validate(); err != nil {
		return SpaceState{}, err
	}
	if now.IsZero() {
		return SpaceState{}, errors.New("space host: creation time is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.spaces[config.URI]; ok {
		if !current.Active() {
			return SpaceState{}, ErrTombstoned
		}
		return SpaceState{}, ErrAlreadyExists
	}
	if len(s.spaces) >= s.maxSpaces {
		return SpaceState{}, ErrQuota
	}
	state := SpaceState{Config: config.Clone(), Generation: 1, CreatedAt: now}
	s.spaces[config.URI] = state
	s.members[config.URI] = make(map[atmos.DID]simplespace.Member)
	s.writers[config.URI] = make(map[atmos.DID]Writer)
	s.registrations[config.URI] = make(map[string]Registration)
	state.Config = state.Config.Clone()
	return state, nil
}

// UpdateSpace implements Store.
func (s *MemoryStore) UpdateSpace(ctx context.Context, space atmos.SpaceRef, patch simplespace.Patch, _ time.Time) (SpaceState, error) {
	if err := s.checkContext(ctx); err != nil {
		return SpaceState{}, err
	}
	if err := patch.Validate(); err != nil {
		return SpaceState{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.activeSpaceLocked(space)
	if err != nil {
		return SpaceState{}, err
	}
	if patch.ReadPolicy != nil {
		state.Config.ReadPolicy = *patch.ReadPolicy
	}
	if patch.WritePolicy != nil {
		state.Config.WritePolicy = *patch.WritePolicy
	}
	if patch.AppAccess != nil {
		state.Config.AppAccess = *patch.AppAccess
		state.Config.AppAccess.Allowed = slices.Clone(patch.AppAccess.Allowed)
	}
	state.Generation++
	s.spaces[space] = state
	state.Config = state.Config.Clone()
	return state, nil
}

// DeleteSpace atomically tombstones the space and preserves a deletion delivery
// for every live registration before removing authority configuration.
func (s *MemoryStore) DeleteSpace(ctx context.Context, space atmos.SpaceRef, now, deliveryExpiry time.Time) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.spaces[space]
	if !ok {
		return ErrNotFound
	}
	if !state.Active() {
		return nil
	}
	if !deliveryExpiry.After(now) {
		return errors.New("space host: deletion delivery expiry must follow deletion time")
	}
	regs := s.registrations[space]
	live := make([]Registration, 0, len(regs))
	for _, reg := range regs {
		if reg.ExpiresAt.After(now) {
			live = append(live, reg)
		}
	}
	writeDeliveries := 0
	for _, delivery := range s.outbox {
		if delivery.Space == space && delivery.Kind == DeliveryWrite {
			writeDeliveries++
		}
	}
	if len(s.outbox)-writeDeliveries+len(live) > s.maxOutbox {
		return ErrOutboxFull
	}
	for _, reg := range live {
		s.enqueueLocked(Delivery{Kind: DeliverySpaceDeleted, Space: space, Service: reg.Service, ServiceType: reg.ServiceType, ExpiresAt: deliveryExpiry})
	}
	state.Generation++
	state.TombstonedAt = now
	state.Config = simplespace.Config{URI: space}
	s.spaces[space] = state
	delete(s.members, space)
	delete(s.writers, space)
	delete(s.registrations, space)
	for id, delivery := range s.outbox {
		if delivery.Space == space && delivery.Kind == DeliveryWrite {
			delete(s.outbox, id)
		}
	}
	return nil
}

// GetMember implements Store.
func (s *MemoryStore) GetMember(ctx context.Context, space atmos.SpaceRef, did atmos.DID) (simplespace.Member, error) {
	if err := s.checkContext(ctx); err != nil {
		return simplespace.Member{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.activeSpaceLocked(space); err != nil {
		return simplespace.Member{}, err
	}
	member, ok := s.members[space][did]
	if !ok {
		return simplespace.Member{}, ErrNotFound
	}
	return member, nil
}

// ListMembers implements Store with an exclusive DID cursor.
func (s *MemoryStore) ListMembers(ctx context.Context, space atmos.SpaceRef, after atmos.DID, limit int) ([]simplespace.Member, error) {
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, errors.New("space host: member list limit must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.activeSpaceLocked(space); err != nil {
		return nil, err
	}
	values := make([]simplespace.Member, 0, len(s.members[space]))
	for did, member := range s.members[space] {
		if after == "" || did > after {
			values = append(values, member)
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i].DID < values[j].DID })
	return slices.Clone(values[:min(limit, len(values))]), nil
}

// PutMember replaces both member access booleans atomically.
func (s *MemoryStore) PutMember(ctx context.Context, space atmos.SpaceRef, member simplespace.Member, _ time.Time) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	if err := member.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.activeSpaceLocked(space)
	if err != nil {
		return err
	}
	if _, exists := s.members[space][member.DID]; !exists && len(s.members[space]) >= s.maxMembers {
		return ErrQuota
	}
	s.members[space][member.DID] = member
	state.Generation++
	s.spaces[space] = state
	return nil
}

// RemoveMember implements Store. Existing writer rows deliberately remain.
func (s *MemoryStore) RemoveMember(ctx context.Context, space atmos.SpaceRef, did atmos.DID, _ time.Time) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.activeSpaceLocked(space)
	if err != nil {
		return err
	}
	delete(s.members[space], did)
	state.Generation++
	s.spaces[space] = state
	return nil
}

// ListWriters implements Store with an exclusive DID cursor.
func (s *MemoryStore) ListWriters(ctx context.Context, space atmos.SpaceRef, after atmos.DID, limit int) ([]Writer, error) {
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, errors.New("space host: writer list limit must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.activeSpaceLocked(space); err != nil {
		return nil, err
	}
	values := make([]Writer, 0, len(s.writers[space]))
	for did, writer := range s.writers[space] {
		if after == "" || did > after {
			values = append(values, writer)
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i].DID < values[j].DID })
	return slices.Clone(values[:min(limit, len(values))]), nil
}

// AdmitWriter enforces TID monotonicity and atomically enqueues fanout.
func (s *MemoryStore) AdmitWriter(ctx context.Context, space atmos.SpaceRef, generation uint64, writer Writer, now time.Time) (AdmissionResult, error) {
	if err := s.checkContext(ctx); err != nil {
		return 0, err
	}
	if err := validateWriter(writer); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.activeSpaceLocked(space)
	if err != nil {
		return 0, err
	}
	if state.Generation != generation {
		return 0, ErrPolicyChanged
	}
	current, exists := s.writers[space][writer.DID]
	if exists {
		if writer.Revision < current.Revision {
			return 0, ErrStaleRevision
		}
		if writer.Revision == current.Revision {
			if writer.Hash != current.Hash {
				return 0, ErrRevisionConflict
			}
			return AdmissionIdempotent, nil
		}
	} else if len(s.writers[space]) >= s.maxWriters {
		return 0, ErrQuota
	}
	regs := s.registrations[space]
	live := make([]Registration, 0, len(regs))
	for _, reg := range regs {
		if reg.ExpiresAt.After(now) {
			live = append(live, reg)
		}
	}
	if len(s.outbox)+len(live) > s.maxOutbox {
		return 0, ErrOutboxFull
	}
	writer.UpdatedAt = now
	s.writers[space][writer.DID] = writer
	for _, reg := range live {
		s.enqueueLocked(Delivery{Kind: DeliveryWrite, Space: space, Service: reg.Service, ServiceType: reg.ServiceType, Writer: writer, ExpiresAt: reg.ExpiresAt})
	}
	return AdmissionAdvanced, nil
}

// Register creates or replaces a lease while enforcing all quotas atomically.
func (s *MemoryStore) Register(ctx context.Context, reg Registration, limits RegistrationLimits) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	if err := validateRegistration(reg); err != nil {
		return err
	}
	if limits.PerSpace <= 0 || limits.PerCredential <= 0 || limits.PerService <= 0 {
		return errors.New("space host: registration limits must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.activeSpaceLocked(reg.Space); err != nil {
		return err
	}
	regs := s.registrations[reg.Space]
	for registeredService, existing := range regs {
		if !existing.ExpiresAt.After(reg.UpdatedAt) {
			delete(regs, registeredService)
		}
	}
	_, replacing := regs[reg.Service]
	if !replacing && len(regs) >= limits.PerSpace {
		return ErrQuota
	}
	credentialCount, serviceCount := 0, 0
	for _, spaceRegs := range s.registrations {
		for service, existing := range spaceRegs {
			if !existing.ExpiresAt.After(reg.UpdatedAt) {
				continue
			}
			if existing.Space == reg.Space && service == reg.Service {
				continue
			}
			if existing.CredentialID == reg.CredentialID {
				credentialCount++
			}
			if existing.Service == reg.Service {
				serviceCount++
			}
		}
	}
	if credentialCount >= limits.PerCredential || serviceCount >= limits.PerService {
		return ErrQuota
	}
	regs[reg.Service] = reg
	return nil
}

// Unregister removes a lease and its pending write deliveries. Deletion
// deliveries are retained once a tombstone has committed.
func (s *MemoryStore) Unregister(ctx context.Context, space atmos.SpaceRef, service string, _ time.Time) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.activeSpaceLocked(space); err != nil {
		return err
	}
	delete(s.registrations[space], service)
	for id, delivery := range s.outbox {
		if delivery.Space == space && delivery.Service == service && delivery.Kind == DeliveryWrite {
			delete(s.outbox, id)
		}
	}
	return nil
}

// ClaimDeliveries leases ready work in stable ID order.
func (s *MemoryStore) ClaimDeliveries(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]Delivery, error) {
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 || lease <= 0 {
		return nil, errors.New("space host: claim limit and lease must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]uint64, 0, len(s.outbox))
	for id, item := range s.outbox {
		if !item.NextAttempt.After(now) && !item.LeaseExpires.After(now) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	ids = ids[:min(limit, len(ids))]
	result := make([]Delivery, 0, len(ids))
	for _, id := range ids {
		item := s.outbox[id]
		s.nextLease++
		item.LeaseToken = s.nextLease
		item.LeaseExpires = now.Add(lease)
		item.Attempt++
		s.outbox[id] = item
		result = append(result, item)
	}
	return result, nil
}

// CompleteDelivery removes work only for the current lease owner.
func (s *MemoryStore) CompleteDelivery(ctx context.Context, id, lease uint64) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.outbox[id]
	if !ok || item.LeaseToken != lease {
		return ErrLeaseLost
	}
	delete(s.outbox, id)
	return nil
}

// RetryDelivery durably releases work with a bounded next-attempt timestamp.
func (s *MemoryStore) RetryDelivery(ctx context.Context, id, lease uint64, next time.Time, deliveryErr error) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	if deliveryErr == nil || next.IsZero() {
		return errors.New("space host: retry requires an error and next-attempt time")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.outbox[id]
	if !ok || item.LeaseToken != lease {
		return ErrLeaseLost
	}
	item.NextAttempt = next
	item.LeaseExpires = time.Time{}
	item.LeaseToken = 0
	s.outbox[id] = item
	return nil
}

func (s *MemoryStore) activeSpaceLocked(space atmos.SpaceRef) (SpaceState, error) {
	state, ok := s.spaces[space]
	if !ok {
		return SpaceState{}, ErrNotFound
	}
	if !state.Active() {
		return SpaceState{}, ErrTombstoned
	}
	return state, nil
}

func (s *MemoryStore) enqueueLocked(delivery Delivery) {
	s.nextDelivery++
	delivery.ID = s.nextDelivery
	s.outbox[delivery.ID] = delivery
}

var _ Store = (*MemoryStore)(nil)

func (d Delivery) String() string {
	return fmt.Sprintf("delivery %d (%d) to %s", d.ID, d.Kind, d.Service)
}
