package host

import (
	"testing"
	"time"

	"github.com/jcalabro/atmos"
)

func FuzzMemoryWriterAdmission(f *testing.F) {
	f.Add("3jzfcijpj2z2a", byte(1))
	f.Add("not-a-tid", byte(2))
	f.Fuzz(func(t *testing.T, revision string, marker byte) {
		if len(revision) > 256 {
			t.Skip()
		}
		store, err := NewMemoryStore(MemoryStoreOptions{MaxSpaces: 1, MaxMembers: 1, MaxWriters: 1, MaxOutbox: 1})
		if err != nil {
			t.Fatal(err)
		}
		config := testConfig(t)
		now := time.Now().UTC()
		state, err := store.CreateSpace(t.Context(), config, now)
		if err != nil {
			t.Fatal(err)
		}
		var hash [32]byte
		hash[0] = marker
		result, err := store.AdmitWriter(t.Context(), config.URI, state.Generation, Writer{
			DID: testMemberDID(), Revision: atmos.TID(revision), Hash: hash,
		}, now)
		if err == nil && result != AdmissionAdvanced {
			t.Fatalf("first valid admission returned %d", result)
		}
		writers, listErr := store.ListWriters(t.Context(), config.URI, "", 1)
		if listErr != nil {
			t.Fatal(listErr)
		}
		if err != nil && len(writers) != 0 {
			t.Fatal("failed admission mutated the writer directory")
		}
		if err == nil && (len(writers) != 1 || writers[0].Hash != hash) {
			t.Fatal("successful admission did not publish the exact checkpoint")
		}
	})
}
