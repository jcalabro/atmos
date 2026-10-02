package backfill

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jcalabro/atmos"
)

// TestLockDIDs_ExclusiveAndDeadlockFree hammers lockDIDs with overlapping
// sorted DID sets from many goroutines: no DID may ever have two holders, and
// ascending acquisition must never deadlock.
func TestLockDIDs_ExclusiveAndDeadlockFree(t *testing.T) {
	t.Parallel()
	e := &Engine{didLocks: make(map[atmos.DID]chan struct{})}
	dids := make([]atmos.DID, 16)
	for i := range dids {
		dids[i] = atmos.DID(fmt.Sprintf("did:plc:lock%02d", i))
	}
	holders := make([]atomic.Int32, len(dids))
	var wg sync.WaitGroup
	for g := range 32 {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(g), 7))
			for range 300 {
				idx := rng.Perm(len(dids))[:1+rng.IntN(4)]
				slices.Sort(idx)
				set := make([]atmos.DID, len(idx))
				for i, j := range idx {
					set[i] = dids[j]
				}
				unlock := e.lockDIDs(set)
				for _, j := range idx {
					if n := holders[j].Add(1); n != 1 {
						t.Errorf("%s has %d holders", dids[j], n)
					}
				}
				runtime.Gosched()
				for _, j := range idx {
					holders[j].Add(-1)
				}
				unlock()
			}
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("lockDIDs deadlocked")
	}
	e.didLocksMu.Lock()
	defer e.didLocksMu.Unlock()
	if len(e.didLocks) != 0 {
		t.Fatalf("%d DID locks leaked", len(e.didLocks))
	}
}

// TestLockDIDs_WaiterIsDurablyBlocked pins the reason DID locks are channels:
// a goroutine waiting for a DID lock must count as durably blocked under
// testing/synctest, so a deterministic scheduler can park the holder inside a
// Store call. With a sync.Mutex, synctest.Wait would never return here.
func TestLockDIDs_WaiterIsDurablyBlocked(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e := &Engine{didLocks: make(map[atmos.DID]chan struct{})}
		did := []atmos.DID{"did:plc:held"}
		unlock := e.lockDIDs(did)
		acquired := make(chan struct{})
		go func() {
			release := e.lockDIDs(did)
			close(acquired)
			release()
		}()
		synctest.Wait()
		select {
		case <-acquired:
			t.Fatal("second locker acquired a held DID lock")
		default:
		}
		unlock()
		<-acquired
	})
}
