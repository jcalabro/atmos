package host

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/space/simplespace"
)

var (
	// ErrNotFound indicates that active authority state does not exist.
	ErrNotFound = errors.New("space host: not found")
	// ErrTombstoned indicates that a space was permanently deleted.
	ErrTombstoned = errors.New("space host: tombstoned")
	// ErrAlreadyExists indicates a duplicate active identity.
	ErrAlreadyExists = errors.New("space host: already exists")
	// ErrPolicyChanged fences an authorization decision made against an older
	// policy generation.
	ErrPolicyChanged = errors.New("space host: policy changed")
	// ErrStaleRevision indicates a writer hint older than the durable row.
	ErrStaleRevision = errors.New("space host: stale writer revision")
	// ErrRevisionConflict indicates an equal writer revision with a different hash.
	ErrRevisionConflict = errors.New("space host: writer revision conflict")
	// ErrQuota indicates that a durable cardinality limit was reached.
	ErrQuota = errors.New("space host: quota exceeded")
	// ErrOutboxFull indicates that an atomic mutation cannot enqueue all required
	// notification work without violating the store's bound.
	ErrOutboxFull = errors.New("space host: delivery outbox full")
	// ErrLeaseLost indicates stale delivery-worker ownership.
	ErrLeaseLost = errors.New("space host: delivery lease lost")
)

// SpaceState is a copy of durable authority policy and lifecycle state.
type SpaceState struct {
	Config       simplespace.Config
	Generation   uint64
	CreatedAt    time.Time
	TombstonedAt time.Time
}

// Active reports whether the state may authorize new work.
func (s SpaceState) Active() bool { return s.TombstonedAt.IsZero() }

// Writer is one admitted writer-directory row.
type Writer struct {
	DID       atmos.DID
	Revision  atmos.TID
	Hash      [32]byte
	UpdatedAt time.Time
}

// Registration is a durable whole-space notification lease.
type Registration struct {
	Space        atmos.SpaceRef
	Service      string
	ServiceType  string
	CredentialID string
	ExpiresAt    time.Time
	UpdatedAt    time.Time
}

// DeliveryKind identifies a durable notification outbox item.
type DeliveryKind uint8

const (
	// DeliveryWrite forwards an admitted writer checkpoint.
	DeliveryWrite DeliveryKind = iota + 1
	// DeliverySpaceDeleted reports an authority tombstone.
	DeliverySpaceDeleted
)

// Delivery is a claimed durable notification. LeaseToken is opaque and must
// be echoed to CompleteDelivery or RetryDelivery.
type Delivery struct {
	ID           uint64
	Kind         DeliveryKind
	Space        atmos.SpaceRef
	Service      string
	ServiceType  string
	Writer       Writer
	Attempt      uint32
	NextAttempt  time.Time
	ExpiresAt    time.Time
	LeaseToken   uint64
	LeaseExpires time.Time
}

// AdmissionResult describes a monotonic writer update.
type AdmissionResult uint8

const (
	// AdmissionAdvanced persisted a newer revision and its fanout atomically.
	AdmissionAdvanced AdmissionResult = iota + 1
	// AdmissionIdempotent observed the already-persisted revision and hash.
	AdmissionIdempotent
)

// RegistrationLimits are enforced atomically by Store.Register.
type RegistrationLimits struct {
	PerSpace      int
	PerCredential int
	PerService    int
}

// Store is the durable authority-state contract. Every mutating method is
// atomic, including its documented outbox changes. Implementations must return
// copies rather than storage-owned mutable buffers.
type Store interface {
	GetSpace(context.Context, atmos.SpaceRef) (SpaceState, error)
	CreateSpace(context.Context, simplespace.Config, time.Time) (SpaceState, error)
	UpdateSpace(context.Context, atmos.SpaceRef, simplespace.Patch, time.Time) (SpaceState, error)
	DeleteSpace(context.Context, atmos.SpaceRef, time.Time, time.Time) error

	GetMember(context.Context, atmos.SpaceRef, atmos.DID) (simplespace.Member, error)
	ListMembers(context.Context, atmos.SpaceRef, atmos.DID, int) ([]simplespace.Member, error)
	PutMember(context.Context, atmos.SpaceRef, simplespace.Member, time.Time) error
	RemoveMember(context.Context, atmos.SpaceRef, atmos.DID, time.Time) error

	ListWriters(context.Context, atmos.SpaceRef, atmos.DID, int) ([]Writer, error)
	AdmitWriter(context.Context, atmos.SpaceRef, uint64, Writer, time.Time) (AdmissionResult, error)

	Register(context.Context, Registration, RegistrationLimits) error
	Unregister(context.Context, atmos.SpaceRef, string, time.Time) error

	ClaimDeliveries(context.Context, time.Time, int, time.Duration) ([]Delivery, error)
	CompleteDelivery(context.Context, uint64, uint64) error
	RetryDelivery(context.Context, uint64, uint64, time.Time, error) error
}

func validateWriter(writer Writer) error {
	if err := writer.DID.Validate(); err != nil {
		return fmt.Errorf("space host: invalid writer DID: %w", err)
	}
	if err := writer.Revision.Validate(); err != nil {
		return fmt.Errorf("space host: invalid writer revision: %w", err)
	}
	return nil
}

func validateRegistration(reg Registration) error {
	if err := reg.Space.Validate(); err != nil {
		return err
	}
	if err := simplespace.ValidateServiceIdentifier(reg.Service); err != nil {
		return err
	}
	if reg.ServiceType == "" || len(reg.ServiceType) > 256 {
		return errors.New("space host: registration service type is required")
	}
	if reg.CredentialID == "" || len(reg.CredentialID) > 256 {
		return errors.New("space host: registration credential id is required")
	}
	if reg.ExpiresAt.IsZero() {
		return errors.New("space host: registration expiry is required")
	}
	return nil
}
