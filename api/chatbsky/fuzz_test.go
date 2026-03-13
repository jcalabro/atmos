package chatbsky

import "testing"

// FuzzDecodeRecord tests that all generated UnmarshalCBOR methods in the
// chat.bsky.* namespace never panic on arbitrary input.
func FuzzDecodeRecord(f *testing.F) {
	f.Add("chat.bsky.actor.declaration", []byte{0xa0})                                                                   // empty map
	f.Add("chat.bsky.actor.declaration", []byte{})                                                                       // empty
	f.Add("chat.bsky.actor.declaration", []byte{0xff})                                                                   // invalid CBOR
	f.Add("unknown.collection", []byte{0xa0})                                                                            // unknown
	f.Add("chat.bsky.actor.declaration", []byte{0xa1, 0x64, 0x74, 0x65, 0x78, 0x74, 0x65, 0x68, 0x65, 0x6c, 0x6c, 0x6f}) // {"text":"hello"}

	f.Fuzz(func(t *testing.T, collection string, data []byte) {
		_, _ = DecodeRecord(collection, data)
	})
}
