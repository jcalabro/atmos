package bsky

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

// FuzzDecodeRecord tests that all generated UnmarshalCBOR methods never panic
// on arbitrary input. Exercises every record type's CBOR decoder.
func FuzzDecodeRecord(f *testing.F) {
	f.Add("app.bsky.feed.post", []byte{0xa0})                                                                   // empty map
	f.Add("app.bsky.actor.profile", []byte{0xa0})                                                               // empty map
	f.Add("app.bsky.feed.like", []byte{0xa0})                                                                   // empty map
	f.Add("app.bsky.graph.follow", []byte{0xa0})                                                                // empty map
	f.Add("unknown.collection", []byte{0xa0})                                                                   // unknown
	f.Add("app.bsky.feed.post", []byte{})                                                                       // empty
	f.Add("app.bsky.feed.post", []byte{0xff})                                                                   // invalid CBOR
	f.Add("app.bsky.feed.post", []byte{0xa1, 0x64, 0x74, 0x65, 0x78, 0x74, 0x65, 0x68, 0x65, 0x6c, 0x6c, 0x6f}) // {"text":"hello"}

	f.Fuzz(func(t *testing.T, collection string, data []byte) {
		_, _ = DecodeRecord(collection, data)
	})
}

// decodeRecordJSON dispatches JSON unmarshal by collection NSID.
func decodeRecordJSON(collection string, data []byte) (any, error) {
	switch collection {
	case "app.bsky.actor.profile":
		var v ActorProfile
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.actor.status":
		var v ActorStatus
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.feed.generator":
		var v FeedGenerator
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.feed.like":
		var v FeedLike
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.feed.post":
		var v FeedPost
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.feed.postgate":
		var v FeedPostgate
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.feed.repost":
		var v FeedRepost
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.feed.threadgate":
		var v FeedThreadgate
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.graph.block":
		var v GraphBlock
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.graph.follow":
		var v GraphFollow
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.graph.list":
		var v GraphList
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.graph.listblock":
		var v GraphListblock
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.graph.listitem":
		var v GraphListitem
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.graph.starterpack":
		var v GraphStarterpack
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.graph.verification":
		var v GraphVerification
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.labeler.service":
		var v LabelerService
		return &v, v.UnmarshalJSON(data)
	case "app.bsky.notification.declaration":
		var v NotificationDeclaration
		return &v, v.UnmarshalJSON(data)
	default:
		return nil, nil
	}
}

// FuzzDecodeRecordJSON tests that all generated UnmarshalJSON methods never panic.
func FuzzDecodeRecordJSON(f *testing.F) {
	f.Add("app.bsky.feed.post", []byte(`{}`))
	f.Add("app.bsky.feed.post", []byte(`{"text":"hello","createdAt":"2024-01-01T00:00:00Z"}`))
	f.Add("app.bsky.actor.profile", []byte(`{}`))
	f.Add("app.bsky.feed.like", []byte(`{}`))
	f.Add("app.bsky.feed.post", []byte{})
	f.Add("app.bsky.feed.post", []byte(`{{{`))
	f.Add("unknown.collection", []byte(`{}`))

	f.Fuzz(func(t *testing.T, collection string, data []byte) {
		_, _ = decodeRecordJSON(collection, data)
	})
}

// FuzzRoundTripCBOR seeds with valid CBOR, mutates, and checks marshal stability.
func FuzzRoundTripCBOR(f *testing.F) {
	// Seed with valid CBOR from known-good values.
	for _, seed := range cborSeeds() {
		f.Add(seed.collection, seed.data)
	}

	f.Fuzz(func(t *testing.T, collection string, data []byte) {
		v, err := DecodeRecord(collection, data)
		if err != nil {
			return
		}

		type cborMarshaler interface {
			MarshalCBOR() ([]byte, error)
			UnmarshalCBOR([]byte) error
		}
		m, ok := v.(cborMarshaler)
		if !ok {
			return
		}

		// Re-marshal
		data2, err := m.MarshalCBOR()
		if err != nil {
			return
		}

		// Unmarshal again — if first succeeded, second should too
		v2, err := DecodeRecord(collection, data2)
		if err != nil {
			t.Fatalf("re-marshal then unmarshal failed: %v", err)
		}

		// Third marshal should be identical to second
		m2, ok := v2.(cborMarshaler)
		if !ok {
			return
		}
		data3, err := m2.MarshalCBOR()
		if err != nil {
			t.Fatalf("third marshal failed: %v", err)
		}
		assert.Equal(t, data2, data3, "CBOR not stable after re-marshal")
	})
}

// FuzzRoundTripJSON seeds with valid JSON, mutates, and checks marshal stability.
func FuzzRoundTripJSON(f *testing.F) {
	for _, seed := range jsonSeeds() {
		f.Add(seed.collection, seed.data)
	}

	f.Fuzz(func(t *testing.T, collection string, data []byte) {
		if !json.Valid(data) {
			return
		}
		v, err := decodeRecordJSON(collection, data)
		if err != nil || v == nil {
			return
		}

		type jsonMarshaler interface {
			MarshalJSON() ([]byte, error)
			UnmarshalJSON([]byte) error
		}
		m, ok := v.(jsonMarshaler)
		if !ok {
			return
		}

		data2, err := m.MarshalJSON()
		if err != nil {
			return
		}

		v2, err := decodeRecordJSON(collection, data2)
		if err != nil {
			t.Fatalf("re-marshal then unmarshal failed: %v", err)
		}
		if v2 == nil {
			return
		}

		m2, ok := v2.(jsonMarshaler)
		if !ok {
			return
		}
		data3, err := m2.MarshalJSON()
		if err != nil {
			t.Fatalf("third marshal failed: %v", err)
		}
		// Re-parse data2 and data3 to compare semantically, since Unicode
		// escapes (e.g. \ufffd) get normalized to raw UTF-8 bytes on first
		// decode, making byte-level comparison unreliable on the first pass.
		// But from the second marshal onward, bytes must be stable.
		v3, err := decodeRecordJSON(collection, data3)
		if err != nil {
			t.Fatalf("unmarshal of third marshal failed: %v", err)
		}
		m3, ok := v3.(jsonMarshaler)
		if !ok {
			return
		}
		data4, err := m3.MarshalJSON()
		if err != nil {
			t.Fatalf("fourth marshal failed: %v", err)
		}
		assert.Equal(t, data3, data4, "JSON not stable after re-marshal (marshal 3 vs 4)")
	})
}

type seedEntry struct {
	collection string
	data       []byte
}

func cborSeeds() []seedEntry {
	var seeds []seedEntry
	add := func(collection string, v interface{ MarshalCBOR() ([]byte, error) }) {
		data, err := v.MarshalCBOR()
		if err != nil {
			return
		}
		seeds = append(seeds, seedEntry{collection, data})
	}
	add("app.bsky.feed.post", &FeedPost{Text: "hello", CreatedAt: "2024-01-01T00:00:00Z"})
	add("app.bsky.feed.like", &FeedLike{CreatedAt: "2024-01-01T00:00:00Z"})
	add("app.bsky.actor.profile", &ActorProfile{})
	return seeds
}

func jsonSeeds() []seedEntry {
	var seeds []seedEntry
	add := func(collection string, v interface{ MarshalJSON() ([]byte, error) }) {
		data, err := v.MarshalJSON()
		if err != nil {
			return
		}
		seeds = append(seeds, seedEntry{collection, data})
	}
	add("app.bsky.feed.post", &FeedPost{Text: "hello", CreatedAt: "2024-01-01T00:00:00Z"})
	add("app.bsky.feed.like", &FeedLike{CreatedAt: "2024-01-01T00:00:00Z"})
	add("app.bsky.actor.profile", &ActorProfile{})
	return seeds
}
