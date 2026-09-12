package host

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sync"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/space/credential"
)

const (
	defaultCredentialLifetime = 2 * time.Hour
	maxRegistrationTTL        = 24 * time.Hour
)

// Limits contains explicit authority resource and scheduling bounds. Every
// field must be positive; the host has no unbounded or silent production mode.
type Limits struct {
	MaxRequestBody      int64
	MaxListMembers      int
	MaxListWriters      int
	RegistrationTTL     time.Duration
	Registration        RegistrationLimits
	DeliveryWorkers     int
	DeliveryBatch       int
	DeliveryLease       time.Duration
	DeliveryPoll        time.Duration
	DeliveryTimeout     time.Duration
	DeliveryRetention   time.Duration
	DeliveryMaxAttempts uint32
	CredentialLifetime  time.Duration
}

// AlphaLimits returns the bounded first-release authority profile. Durable
// stores must enforce the same registration quotas and a bounded outbox
// atomically.
// Deployments should tune worker counts only after measuring their own
// notification destination latency and store contention.
func AlphaLimits() Limits {
	return Limits{
		MaxRequestBody:      1 << 20,
		MaxListMembers:      1000,
		MaxListWriters:      1000,
		RegistrationTTL:     24 * time.Hour,
		Registration:        RegistrationLimits{PerSpace: 1000, PerCredential: 10, PerService: 100},
		DeliveryWorkers:     32,
		DeliveryBatch:       32,
		DeliveryLease:       30 * time.Second,
		DeliveryPoll:        100 * time.Millisecond,
		DeliveryTimeout:     10 * time.Second,
		DeliveryRetention:   24 * time.Hour,
		DeliveryMaxAttempts: 10,
		CredentialLifetime:  2 * time.Hour,
	}
}

// Options provides every authority dependency explicitly.
type Options struct {
	Origin         *url.URL
	EndpointPolicy identity.EndpointPolicy
	Store          Store
	Replay         credential.ReplayStore
	AccountAuth    AccountAuthenticator
	Resolver       identity.Resolver
	Signer         Signer
	Attestations   AttestationVerifier
	Policies       PolicyEvaluator
	Subscribers    SubscriberPolicy
	Delivery       DeliveryTransport
	Clock          Clock
	Events         EventSink
	Limits         Limits
}

// Host is a mountable authority service. Constructing it does not start
// background delivery; call Start and Shutdown explicitly.
type Host struct {
	origin         *url.URL
	endpointPolicy identity.EndpointPolicy
	store          Store
	replay         credential.ReplayStore
	accountAuth    AccountAuthenticator
	resolver       identity.Resolver
	signer         Signer
	attestations   AttestationVerifier
	policies       PolicyEvaluator
	subscribers    SubscriberPolicy
	delivery       DeliveryTransport
	clock          Clock
	events         EventSink
	limits         Limits

	startMu sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
}

// New validates dependencies and constructs an inactive host.
func New(opts Options) (*Host, error) {
	if err := validateOptions(opts); err != nil {
		return nil, err
	}
	origin := *opts.Origin
	return &Host{
		origin: &origin, endpointPolicy: opts.EndpointPolicy,
		store: opts.Store, replay: opts.Replay, accountAuth: opts.AccountAuth,
		resolver: opts.Resolver, signer: opts.Signer, attestations: opts.Attestations,
		policies: opts.Policies, subscribers: opts.Subscribers,
		delivery: opts.Delivery, clock: opts.Clock, events: opts.Events,
		limits: opts.Limits,
	}, nil
}

func validateOptions(opts Options) error {
	for _, dependency := range []struct {
		name  string
		value any
	}{
		{"origin", opts.Origin}, {"store", opts.Store}, {"replay store", opts.Replay},
		{"account authenticator", opts.AccountAuth}, {"identity resolver", opts.Resolver},
		{"signer", opts.Signer}, {"attestation verifier", opts.Attestations},
		{"policy evaluator", opts.Policies}, {"subscriber policy", opts.Subscribers},
		{"delivery transport", opts.Delivery}, {"clock", opts.Clock}, {"event sink", opts.Events},
	} {
		if nilDependency(dependency.value) {
			return fmt.Errorf("space host: %s is required", dependency.name)
		}
	}
	if err := validateOrigin(opts.Origin, opts.EndpointPolicy); err != nil {
		return err
	}
	l := opts.Limits
	if l.MaxRequestBody <= 0 || l.MaxListMembers <= 0 || l.MaxListWriters <= 0 ||
		l.RegistrationTTL <= 0 || l.RegistrationTTL > maxRegistrationTTL ||
		l.Registration.PerSpace <= 0 || l.Registration.PerCredential <= 0 || l.Registration.PerService <= 0 ||
		l.DeliveryWorkers <= 0 || l.DeliveryBatch <= 0 || l.DeliveryLease <= 0 ||
		l.DeliveryPoll <= 0 || l.DeliveryPoll > time.Minute || l.DeliveryTimeout <= 0 ||
		l.DeliveryLease <= l.DeliveryTimeout ||
		l.DeliveryRetention <= 0 || l.DeliveryMaxAttempts == 0 ||
		l.CredentialLifetime <= 0 || l.CredentialLifetime > defaultCredentialLifetime {
		return errors.New("space host: all limits must be positive and within protocol ceilings")
	}
	return nil
}

func nilDependency(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func validateOrigin(origin *url.URL, policy identity.EndpointPolicy) error {
	if origin == nil || !origin.IsAbs() || origin.Host == "" || origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" || origin.RawFragment != "" || origin.Opaque != "" {
		return errors.New("space host: origin must be an absolute URL without userinfo, query, or fragment")
	}
	if origin.Path != "" && origin.Path != "/" {
		return errors.New("space host: origin must not contain a path")
	}
	if origin.Scheme != "https" && (!policy.AllowHTTP || origin.Scheme != "http") {
		return errors.New("space host: origin must use HTTPS")
	}
	return nil
}

// Start begins durable delivery workers. It may be called once until Shutdown
// completes; duplicate concurrent starts fail.
func (h *Host) Start(parent context.Context) error {
	h.startMu.Lock()
	defer h.startMu.Unlock()
	if h.cancel != nil {
		return errors.New("space host: delivery workers already started")
	}
	ctx, cancel := context.WithCancel(parent)
	h.cancel = cancel
	h.done = make(chan struct{})
	go h.runDelivery(ctx)
	return nil
}

// Shutdown cancels delivery attempts and waits for every worker. Claimed work
// remains durable and becomes eligible again after its lease.
func (h *Host) Shutdown(ctx context.Context) error {
	h.startMu.Lock()
	cancel, done := h.cancel, h.done
	h.startMu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		h.startMu.Lock()
		h.cancel, h.done = nil, nil
		h.startMu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Host) emit(ctx context.Context, event Event) {
	event.Timestamp = h.clock.Now()
	h.events.Emit(ctx, event)
}

func (h *Host) endpoint(method string) string {
	copy := *h.origin
	copy.Path = "/xrpc/" + method
	return copy.String()
}

func generatedSpaceKey() (atmos.RecordKey, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("space host: generate space key: %w", err)
	}
	value := base32.NewEncoding(atmos.Base32SortAlphabet).WithPadding(base32.NoPadding).EncodeToString(raw[:])
	return atmos.ParseRecordKey(value)
}
