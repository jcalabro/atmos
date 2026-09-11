// Package storetest provides a reusable conformance harness for host.Store
// implementations.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jcalabro/atmos"
	spacehost "github.com/jcalabro/atmos/space/host"
	"github.com/jcalabro/atmos/space/simplespace"
)

// Factory returns a fresh empty authority store.
type Factory func() (spacehost.Store, error)

// Run checks copy isolation, replace semantics, policy fencing, monotonic
// writer admission, atomic fanout, quotas, delivery leases, deletion delivery,
// and permanent tombstones. It returns the first invariant violation.
func Run(ctx context.Context, factory Factory) error {
	store, err := factory()
	if err != nil {
		return err
	}
	space := atmos.SpaceRef("at://did:plc:abcdefghijklmnopqrstuvwx/space/com.example.board/main")
	memberDID := atmos.DID("did:plc:bcdefghijklmnopqrstuvwxy")
	config := simplespace.Config{
		URI: space, ReadPolicy: simplespace.Policy{Kind: simplespace.PolicyMemberList},
		WritePolicy: simplespace.Policy{Kind: simplespace.PolicyMemberList},
		AppAccess:   simplespace.AppAccess{Kind: simplespace.AppAccessAllowList, Allowed: []string{"https://client.example/metadata.json"}},
	}
	now := time.Now().UTC()
	state, err := store.CreateSpace(ctx, config, now)
	if err != nil || state.Generation == 0 {
		return fmt.Errorf("create space: %w", err)
	}
	config.AppAccess.Allowed[0] = "mutated"
	loaded, err := store.GetSpace(ctx, space)
	if err != nil || loaded.Config.AppAccess.Allowed[0] != "https://client.example/metadata.json" {
		return errors.New("store retained caller-owned policy memory")
	}
	loaded.Config.AppAccess.Allowed[0] = "mutated-again"
	reloaded, err := store.GetSpace(ctx, space)
	if err != nil || reloaded.Config.AppAccess.Allowed[0] != "https://client.example/metadata.json" {
		return errors.New("store returned mutable policy memory")
	}
	member := simplespace.Member{DID: memberDID, Read: true}
	if err := store.PutMember(ctx, space, member, now); err != nil {
		return err
	}
	member.Read, member.Write = false, true
	if err := store.PutMember(ctx, space, member, now); err != nil {
		return err
	}
	storedMember, err := store.GetMember(ctx, space, memberDID)
	if err != nil || storedMember != member {
		return errors.New("putMember did not replace both booleans")
	}
	current, err := store.GetSpace(ctx, space)
	if err != nil {
		return err
	}
	var hash [32]byte
	hash[0] = 1
	writer := spacehost.Writer{DID: memberDID, Revision: "3jzfcijpj2z2a", Hash: hash}
	if _, err := store.AdmitWriter(ctx, space, state.Generation, writer, now); !errors.Is(err, spacehost.ErrPolicyChanged) {
		return errors.New("stale policy generation admitted a writer")
	}
	service := "did:plc:cdefghijklmnopqrstuvwxyz#sync"
	limits := spacehost.RegistrationLimits{PerSpace: 1, PerCredential: 1, PerService: 1}
	reg := spacehost.Registration{Space: space, Service: service, ServiceType: "AtprotoSpaceSyncer", CredentialID: "credential", ExpiresAt: now.Add(time.Hour), UpdatedAt: now}
	if err := store.Register(ctx, reg, limits); err != nil {
		return err
	}
	if err := store.Register(ctx, spacehost.Registration{Space: space, Service: "did:plc:defghijklmnopqrstuvwxyza#sync", ServiceType: "AtprotoSpaceSyncer", CredentialID: "other", ExpiresAt: now.Add(time.Hour), UpdatedAt: now}, limits); !errors.Is(err, spacehost.ErrQuota) {
		return errors.New("registration quota was not enforced atomically")
	}
	result, err := store.AdmitWriter(ctx, space, current.Generation, writer, now)
	if err != nil || result != spacehost.AdmissionAdvanced {
		return fmt.Errorf("admit writer: %w", err)
	}
	result, err = store.AdmitWriter(ctx, space, current.Generation, writer, now)
	if err != nil || result != spacehost.AdmissionIdempotent {
		return errors.New("equal writer checkpoint was not idempotent")
	}
	conflict := writer
	conflict.Hash[0]++
	if _, err := store.AdmitWriter(ctx, space, current.Generation, conflict, now); !errors.Is(err, spacehost.ErrRevisionConflict) {
		return errors.New("equal revision with different hash did not conflict")
	}
	claimed, err := store.ClaimDeliveries(ctx, now, 1, time.Minute)
	if err != nil || len(claimed) != 1 || claimed[0].Kind != spacehost.DeliveryWrite {
		return errors.New("writer advancement and fanout were not atomic")
	}
	if err := store.CompleteDelivery(ctx, claimed[0].ID, claimed[0].LeaseToken+1); !errors.Is(err, spacehost.ErrLeaseLost) {
		return errors.New("stale delivery lease completed work")
	}
	if err := store.CompleteDelivery(ctx, claimed[0].ID, claimed[0].LeaseToken); err != nil {
		return err
	}
	if err := store.DeleteSpace(ctx, space, now.Add(time.Second), now.Add(time.Hour)); err != nil {
		return err
	}
	deletion, err := store.ClaimDeliveries(ctx, now.Add(time.Second), 1, time.Minute)
	if err != nil || len(deletion) != 1 || deletion[0].Kind != spacehost.DeliverySpaceDeleted {
		return errors.New("deletion notification was not preserved")
	}
	if _, err := store.CreateSpace(ctx, reloaded.Config, now.Add(2*time.Second)); !errors.Is(err, spacehost.ErrTombstoned) {
		return errors.New("tombstoned URI was recreated")
	}
	return nil
}
