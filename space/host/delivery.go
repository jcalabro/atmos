package host

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/identity"
	spaceclient "github.com/jcalabro/atmos/space/client"
	"github.com/jcalabro/atmos/space/notificationauth"
)

func (h *Host) runDelivery(ctx context.Context) {
	defer close(h.done)
	jobs := make(chan Delivery, h.limits.DeliveryWorkers)
	var workers sync.WaitGroup
	for range h.limits.DeliveryWorkers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case delivery := <-jobs:
					h.processDelivery(ctx, delivery)
				}
			}
		}()
	}
	ticker := time.NewTicker(h.limits.DeliveryPoll)
	defer ticker.Stop()
	for {
		available := cap(jobs) - len(jobs)
		if available > 0 {
			available = min(available, h.limits.DeliveryBatch)
			claimed, err := h.store.ClaimDeliveries(ctx, h.clock.Now(), available, h.limits.DeliveryLease)
			if err != nil && !errors.Is(err, context.Canceled) {
				h.emit(ctx, Event{Kind: EventDeliveryFailed, Err: err})
			} else {
				for _, delivery := range claimed {
					select {
					case jobs <- delivery:
					case <-ctx.Done():
						workers.Wait()
						return
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			workers.Wait()
			return
		case <-ticker.C:
		}
	}
}

func (h *Host) processDelivery(parent context.Context, delivery Delivery) {
	now := h.clock.Now()
	if !delivery.ExpiresAt.After(now) || delivery.Attempt > h.limits.DeliveryMaxAttempts {
		err := errors.New("space host: notification delivery expired")
		if completeErr := h.store.CompleteDelivery(parent, delivery.ID, delivery.LeaseToken); completeErr != nil {
			err = errors.Join(err, completeErr)
		}
		h.emit(parent, Event{Kind: EventDeliveryFailed, Space: delivery.Space, Service: delivery.Service, Attempt: delivery.Attempt, Err: err})
		return
	}
	// A claimed delivery can wait in the worker queue, so the lease may already
	// be gone before processing starts. Never start work on a lost lease: another
	// claimer may own the row, and a send here would duplicate its notification.
	// The row itself stays reclaimable through ordinary lease recovery.
	if !delivery.LeaseExpires.After(now) {
		h.emit(parent, Event{Kind: EventDeliveryFailed, Space: delivery.Space, Service: delivery.Service, Attempt: delivery.Attempt, Err: errors.New("space host: delivery lease expired before processing")})
		return
	}
	// The deadline is additionally bounded by the lease so a context-honoring
	// dependency cannot keep working into another claimer's ownership window.
	ctx, cancel := context.WithTimeout(parent, min(h.limits.DeliveryTimeout, min(delivery.ExpiresAt.Sub(now), delivery.LeaseExpires.Sub(now))))
	defer cancel()
	endpoint, err := h.resolveService(ctx, delivery.Service, delivery.ServiceType)
	if err == nil && !delivery.ExpiresAt.After(h.clock.Now()) {
		err = errors.New("space host: notification delivery expired during service resolution")
	}
	if err == nil {
		var keyErr error
		var token string
		key, keyErr := h.signer.ServiceKey(ctx, delivery.Space.Authority())
		if keyErr != nil {
			err = keyErr
		} else {
			issuedAt := h.clock.Now()
			if !delivery.ExpiresAt.After(issuedAt) {
				err = errors.New("space host: notification delivery expired during signing")
			}
			expires := issuedAt.Add(time.Minute)
			if delivery.ExpiresAt.Before(expires) {
				expires = delivery.ExpiresAt
			}
			var body any
			var method atmos.NSID
			if err == nil {
				switch delivery.Kind {
				case DeliveryWrite:
					method = notificationauth.NotifyWriteMethod
					token, err = notificationauth.CreateAuthorityNotifyWriteTokenAt(delivery.Space.Authority(), delivery.Service, issuedAt, expires, key)
					body = &comatproto.SpaceNotifyWrite_Input{
						Space: delivery.Space.String(), Repo: delivery.Writer.DID.String(),
						Rev: delivery.Writer.Revision.String(), Hash: delivery.Writer.Hash[:],
					}
				case DeliverySpaceDeleted:
					method = notificationauth.NotifySpaceDeletedMethod
					token, err = notificationauth.CreateAuthorityNotifySpaceDeletedTokenAt(delivery.Space.Authority(), delivery.Service, issuedAt, expires, key)
					body = &comatproto.SpaceNotifySpaceDeleted_Input{Space: delivery.Space.String()}
				default:
					err = errors.New("space host: unknown delivery kind")
				}
			}
			if err == nil && !delivery.ExpiresAt.After(h.clock.Now()) {
				err = errors.New("space host: notification delivery expired before send")
			}
			if err == nil && !delivery.LeaseExpires.After(h.clock.Now()) {
				err = errors.New("space host: delivery lease expired before send")
			}
			if err == nil {
				err = h.delivery.Deliver(ctx, endpoint, method, token, body)
			}
		}
	}
	if err == nil {
		if err := h.store.CompleteDelivery(parent, delivery.ID, delivery.LeaseToken); err != nil {
			h.emit(parent, Event{Kind: EventDeliveryFailed, Space: delivery.Space, Service: delivery.Service, Attempt: delivery.Attempt, Err: err})
			return
		}
		h.emit(parent, Event{Kind: EventDeliverySucceeded, Space: delivery.Space, Service: delivery.Service, Attempt: delivery.Attempt})
		return
	}
	next := h.clock.Now().Add(deliveryBackoff(delivery.Attempt))
	if !next.Before(delivery.ExpiresAt) || delivery.Attempt >= h.limits.DeliveryMaxAttempts {
		completeErr := h.store.CompleteDelivery(parent, delivery.ID, delivery.LeaseToken)
		if completeErr != nil {
			err = errors.Join(err, completeErr)
		}
	} else if retryErr := h.store.RetryDelivery(parent, delivery.ID, delivery.LeaseToken, next, err); retryErr != nil {
		err = errors.Join(err, retryErr)
	}
	h.emit(parent, Event{Kind: EventDeliveryFailed, Space: delivery.Space, Service: delivery.Service, Attempt: delivery.Attempt, Err: err})
}

func deliveryBackoff(attempt uint32) time.Duration {
	shift := min(attempt, uint32(6))
	base := time.Second * time.Duration(1<<shift)
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return base
	}
	// Jitter in [0.5, 1.5), avoiding synchronized replica retries.
	fraction := float64(binary.BigEndian.Uint64(random[:])>>11) / float64(uint64(1)<<53)
	return time.Duration(float64(base) * (0.5 + fraction))
}

func (h *Host) resolveService(ctx context.Context, identifier, serviceType string) (*url.URL, error) {
	endpoint, err := resolveServiceWith(ctx, h.resolver, h.endpointPolicy, identifier, serviceType)
	if err != nil {
		return nil, fmt.Errorf("space host: resolve subscriber service: %w", err)
	}
	return endpoint, nil
}

// HTTPDeliveryTransport sends one bounded JSON POST without redirects using
// the explicit no-reuse HTTP/1 correctness baseline. DNS results are checked at
// dial time so a validated hostname can never redirect delivery to a private
// network: EndpointPolicy.AllowPrivateLiteral only admits endpoints whose host
// is itself a private IP literal, not private DNS answers.
type HTTPDeliveryTransport struct {
	client      *http.Client
	maxResponse int64
}

// NewHTTPDeliveryTransport constructs a bounded single-attempt transport.
func NewHTTPDeliveryTransport(policy identity.EndpointPolicy, timeout time.Duration, maxResponse int64) (*HTTPDeliveryTransport, error) {
	if timeout <= 0 || maxResponse <= 0 || maxResponse == math.MaxInt64 {
		return nil, errors.New("space host: delivery timeout and response limit must be positive and response limit must be below MaxInt64")
	}
	client := spaceclient.NewCorrectnessHTTPClient(spaceclient.NetworkPolicy{
		AllowPrivateNetworks:     policy.AllowPrivateNetworks,
		AllowPrivateLiteralHosts: policy.AllowPrivateLiteral,
	})
	client.Timeout = timeout
	return &HTTPDeliveryTransport{client: client, maxResponse: maxResponse}, nil
}

// Deliver implements DeliveryTransport.
func (t *HTTPDeliveryTransport) Deliver(ctx context.Context, endpoint *url.URL, method atmos.NSID, token string, body any) error {
	if t == nil || t.client == nil || endpoint == nil {
		return errors.New("space host: invalid HTTP delivery transport")
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("space host: encode notification: %w", err)
	}
	target := *endpoint
	target.Path = strings.TrimRight(target.Path, "/") + "/xrpc/" + method.String()
	target.RawQuery, target.Fragment, target.RawFragment = "", "", ""
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	response, err := t.client.Do(req)
	if err != nil {
		return err
	}
	limited := io.LimitReader(response.Body, t.maxResponse+1)
	responseBody, readErr := io.ReadAll(limited)
	closeErr := response.Body.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if int64(len(responseBody)) > t.maxResponse {
		return errors.New("space host: notification response exceeds limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("space host: notification returned HTTP %d", response.StatusCode)
	}
	return nil
}

var _ DeliveryTransport = (*HTTPDeliveryTransport)(nil)
