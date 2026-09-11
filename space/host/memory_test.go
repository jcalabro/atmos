package host

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/space/simplespace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryStore_TombstoneCannotBeRecreated(t *testing.T) {
	t.Parallel()
	store := newTestMemoryStore(t, 8)
	config := testConfig(t)
	now := time.Now().UTC()
	state, err := store.CreateSpace(t.Context(), config, now)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), state.Generation)

	require.NoError(t, store.DeleteSpace(t.Context(), config.URI, now.Add(time.Second), now.Add(time.Hour)))
	state, err = store.GetSpace(t.Context(), config.URI)
	require.NoError(t, err)
	assert.False(t, state.Active())
	assert.Equal(t, uint64(2), state.Generation)

	_, err = store.CreateSpace(t.Context(), config, now.Add(2*time.Second))
	require.ErrorIs(t, err, ErrTombstoned)
	require.NoError(t, store.DeleteSpace(t.Context(), config.URI, now.Add(3*time.Second), now.Add(time.Hour)))
}

func TestMemoryStore_MemberReplaceAndRemovalPreservesWriter(t *testing.T) {
	t.Parallel()
	store := newTestMemoryStore(t, 8)
	config := testConfig(t)
	now := time.Now().UTC()
	state, err := store.CreateSpace(t.Context(), config, now)
	require.NoError(t, err)
	member := simplespace.Member{DID: testMemberDID(), Read: true, Write: false}
	require.NoError(t, store.PutMember(t.Context(), config.URI, member, now))
	member.Read, member.Write = false, true
	require.NoError(t, store.PutMember(t.Context(), config.URI, member, now))
	got, err := store.GetMember(t.Context(), config.URI, member.DID)
	require.NoError(t, err)
	assert.Equal(t, member, got, "putMember replaces both booleans")

	current, err := store.GetSpace(t.Context(), config.URI)
	require.NoError(t, err)
	writer := testWriter(member.DID, "3jzfcijpj2z2a", 1)
	_, err = store.AdmitWriter(t.Context(), config.URI, current.Generation, writer, now)
	require.NoError(t, err)
	require.NoError(t, store.RemoveMember(t.Context(), config.URI, member.DID, now))
	_, err = store.GetMember(t.Context(), config.URI, member.DID)
	require.ErrorIs(t, err, ErrNotFound)
	writers, err := store.ListWriters(t.Context(), config.URI, "", 10)
	require.NoError(t, err)
	require.Equal(t, []Writer{writerWithTime(writer, now)}, writers)
	assert.Greater(t, current.Generation, state.Generation)
}

func TestMemoryStore_MonotonicWriterAdmission(t *testing.T) {
	t.Parallel()
	store := newTestMemoryStore(t, 8)
	config := testConfig(t)
	now := time.Now().UTC()
	state, err := store.CreateSpace(t.Context(), config, now)
	require.NoError(t, err)
	did := testMemberDID()
	first := testWriter(did, "3jzfcijpj2z2a", 1)
	result, err := store.AdmitWriter(t.Context(), config.URI, state.Generation, first, now)
	require.NoError(t, err)
	assert.Equal(t, AdmissionAdvanced, result)

	result, err = store.AdmitWriter(t.Context(), config.URI, state.Generation, first, now)
	require.NoError(t, err)
	assert.Equal(t, AdmissionIdempotent, result)

	stale := testWriter(did, "3jzfcijpj2z27", 2)
	_, err = store.AdmitWriter(t.Context(), config.URI, state.Generation, stale, now)
	require.ErrorIs(t, err, ErrStaleRevision)
	conflict := testWriter(did, first.Revision.String(), 3)
	_, err = store.AdmitWriter(t.Context(), config.URI, state.Generation, conflict, now)
	require.ErrorIs(t, err, ErrRevisionConflict)

	newer := testWriter(did, "3jzfcijpj2z2b", 4)
	result, err = store.AdmitWriter(t.Context(), config.URI, state.Generation, newer, now)
	require.NoError(t, err)
	assert.Equal(t, AdmissionAdvanced, result)
	writers, err := store.ListWriters(t.Context(), config.URI, "", 10)
	require.NoError(t, err)
	require.Equal(t, newer.Revision, writers[0].Revision)
	require.Equal(t, newer.Hash, writers[0].Hash)
}

func TestMemoryStore_PolicyGenerationFencesAdmission(t *testing.T) {
	t.Parallel()
	store := newTestMemoryStore(t, 8)
	config := testConfig(t)
	now := time.Now().UTC()
	state, err := store.CreateSpace(t.Context(), config, now)
	require.NoError(t, err)
	require.NoError(t, store.PutMember(t.Context(), config.URI, simplespace.Member{DID: testMemberDID(), Write: true}, now))
	_, err = store.AdmitWriter(t.Context(), config.URI, state.Generation, testWriter(testMemberDID(), "3jzfcijpj2z2a", 1), now)
	require.ErrorIs(t, err, ErrPolicyChanged)
	writers, listErr := store.ListWriters(t.Context(), config.URI, "", 10)
	require.NoError(t, listErr)
	assert.Empty(t, writers)
}

func TestMemoryStore_OutboxAdmissionIsAtomic(t *testing.T) {
	t.Parallel()
	store := newTestMemoryStore(t, 1)
	config := testConfig(t)
	now := time.Now().UTC()
	state, err := store.CreateSpace(t.Context(), config, now)
	require.NoError(t, err)
	limits := RegistrationLimits{PerSpace: 3, PerCredential: 3, PerService: 3}
	require.NoError(t, store.Register(t.Context(), testRegistration(config.URI, "did:plc:cccccccccccccccccccccccc#sync", "one", now), limits))
	require.NoError(t, store.Register(t.Context(), testRegistration(config.URI, "did:plc:dddddddddddddddddddddddd#sync", "two", now), limits))
	_, err = store.AdmitWriter(t.Context(), config.URI, state.Generation, testWriter(testMemberDID(), "3jzfcijpj2z2a", 1), now)
	require.ErrorIs(t, err, ErrOutboxFull)
	writers, listErr := store.ListWriters(t.Context(), config.URI, "", 10)
	require.NoError(t, listErr)
	assert.Empty(t, writers, "directory row must not advance without fanout persistence")

	require.NoError(t, store.Unregister(t.Context(), config.URI, "did:plc:dddddddddddddddddddddddd#sync", now))
	_, err = store.AdmitWriter(t.Context(), config.URI, state.Generation, testWriter(testMemberDID(), "3jzfcijpj2z2a", 1), now)
	require.NoError(t, err)
	claimed, err := store.ClaimDeliveries(t.Context(), now, 2, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	assert.Equal(t, DeliveryWrite, claimed[0].Kind)
}

func TestMemoryStore_DeleteReplacesPendingWritesAtCapacity(t *testing.T) {
	t.Parallel()
	store := newTestMemoryStore(t, 1)
	config := testConfig(t)
	now := time.Now().UTC()
	state, err := store.CreateSpace(t.Context(), config, now)
	require.NoError(t, err)
	reg := testRegistration(config.URI, "did:plc:cccccccccccccccccccccccc#sync", "one", now)
	require.NoError(t, store.Register(t.Context(), reg, RegistrationLimits{PerSpace: 1, PerCredential: 1, PerService: 1}))
	_, err = store.AdmitWriter(t.Context(), config.URI, state.Generation, testWriter(testMemberDID(), "3jzfcijpj2z2a", 1), now)
	require.NoError(t, err)
	require.NoError(t, store.DeleteSpace(t.Context(), config.URI, now.Add(time.Second), now.Add(time.Hour)))
	claimed, err := store.ClaimDeliveries(t.Context(), now.Add(time.Second), 2, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	assert.Equal(t, DeliverySpaceDeleted, claimed[0].Kind)
}

func TestMemoryStore_RegistrationQuotasIgnoreExpiredLeases(t *testing.T) {
	t.Parallel()
	store := newTestMemoryStore(t, 8)
	config := testConfig(t)
	now := time.Now().UTC()
	_, err := store.CreateSpace(t.Context(), config, now)
	require.NoError(t, err)
	limits := RegistrationLimits{PerSpace: 1, PerCredential: 1, PerService: 1}
	old := testRegistration(config.URI, "did:plc:cccccccccccccccccccccccc#sync", "same", now.Add(-2*time.Hour))
	old.ExpiresAt = now.Add(-time.Hour)
	require.NoError(t, store.Register(t.Context(), old, limits))
	fresh := testRegistration(config.URI, "did:plc:dddddddddddddddddddddddd#sync", "same", now)
	require.NoError(t, store.Register(t.Context(), fresh, limits))
}

func TestMemoryStore_DeliveryLeaseOwnership(t *testing.T) {
	t.Parallel()
	store := newTestMemoryStore(t, 8)
	config := testConfig(t)
	now := time.Now().UTC()
	state, err := store.CreateSpace(t.Context(), config, now)
	require.NoError(t, err)
	require.NoError(t, store.Register(t.Context(), testRegistration(config.URI, "did:plc:cccccccccccccccccccccccc#sync", "one", now), RegistrationLimits{1, 1, 1}))
	_, err = store.AdmitWriter(t.Context(), config.URI, state.Generation, testWriter(testMemberDID(), "3jzfcijpj2z2a", 1), now)
	require.NoError(t, err)
	first, err := store.ClaimDeliveries(t.Context(), now, 1, time.Second)
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.ErrorIs(t, store.CompleteDelivery(t.Context(), first[0].ID, first[0].LeaseToken+1), ErrLeaseLost)
	second, err := store.ClaimDeliveries(t.Context(), now.Add(2*time.Second), 1, time.Second)
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.NotEqual(t, first[0].LeaseToken, second[0].LeaseToken)
	require.ErrorIs(t, store.CompleteDelivery(t.Context(), first[0].ID, first[0].LeaseToken), ErrLeaseLost)
	require.NoError(t, store.RetryDelivery(t.Context(), second[0].ID, second[0].LeaseToken, now.Add(time.Minute), errors.New("temporary")))
	none, err := store.ClaimDeliveries(t.Context(), now.Add(30*time.Second), 1, time.Second)
	require.NoError(t, err)
	assert.Empty(t, none)
}

func TestMemoryStore_ConcurrentMonotonicAdmission(t *testing.T) {
	t.Parallel()
	store := newTestMemoryStore(t, 8)
	config := testConfig(t)
	now := time.Now().UTC()
	state, err := store.CreateSpace(t.Context(), config, now)
	require.NoError(t, err)
	writer := testWriter(testMemberDID(), "3jzfcijpj2z2a", 1)
	var advanced atomic.Int32
	var failed atomic.Int32
	var group sync.WaitGroup
	for range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := store.AdmitWriter(context.Background(), config.URI, state.Generation, writer, now)
			if err != nil {
				failed.Add(1)
			} else if result == AdmissionAdvanced {
				advanced.Add(1)
			}
		}()
	}
	group.Wait()
	assert.Equal(t, int32(1), advanced.Load())
	assert.Zero(t, failed.Load())
}

func newTestMemoryStore(t *testing.T, outbox int) *MemoryStore {
	t.Helper()
	store, err := NewMemoryStore(MemoryStoreOptions{MaxSpaces: 8, MaxMembers: 8, MaxWriters: 8, MaxOutbox: outbox})
	require.NoError(t, err)
	return store
}

func testConfig(t *testing.T) simplespace.Config {
	t.Helper()
	space, err := atmos.ParseSpaceRef("at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/space/com.example.space/room")
	require.NoError(t, err)
	return simplespace.Config{
		URI: space, ReadPolicy: simplespace.Policy{Kind: simplespace.PolicyPublic},
		WritePolicy: simplespace.Policy{Kind: simplespace.PolicyPublic},
		AppAccess:   simplespace.AppAccess{Kind: simplespace.AppAccessOpen},
	}
}

func testMemberDID() atmos.DID { return "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb" }

func testWriter(did atmos.DID, rev string, marker byte) Writer {
	var hash [32]byte
	hash[0] = marker
	return Writer{DID: did, Revision: atmos.TID(rev), Hash: hash}
}

func writerWithTime(writer Writer, now time.Time) Writer {
	writer.UpdatedAt = now
	return writer
}

func testRegistration(space atmos.SpaceRef, service, credentialID string, now time.Time) Registration {
	return Registration{Space: space, Service: service, ServiceType: "AtprotoSpaceSyncer", CredentialID: credentialID, UpdatedAt: now, ExpiresAt: now.Add(time.Hour)}
}
