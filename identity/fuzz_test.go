package identity

import "testing"

// FuzzIdentityFromDocument tests the full pipeline: parse DID document JSON,
// then extract an Identity. Verifies no panics and that extracted DIDs are valid.
func FuzzIdentityFromDocument(f *testing.F) {
	f.Add([]byte(`{"id":"did:plc:abc123","alsoKnownAs":["at://alice.bsky.social"],"verificationMethod":[{"id":"#atproto","type":"Multikey","controller":"did:plc:abc123","publicKeyMultibase":"zDnae"}],"service":[{"id":"#atproto_pds","type":"AtprotoPersonalDataServer","serviceEndpoint":"https://pds.example.com"}]}`))
	f.Add([]byte(`{"id":"did:web:example.com","alsoKnownAs":[],"verificationMethod":[],"service":[]}`))
	f.Add([]byte(`{"id":"did:plc:test","alsoKnownAs":["at://invalid handle"]}`))
	f.Add([]byte(`{"id":"did:plc:x","verificationMethod":[{"id":"no-fragment","type":"t","controller":"c","publicKeyMultibase":"z"}]}`))
	f.Add([]byte(`{"id":"did:plc:x","service":[{"id":"#","type":"t","serviceEndpoint":"https://x.com"}]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		doc, err := ParseDIDDocument(data)
		if err != nil {
			return
		}
		id, err := IdentityFromDocument(doc)
		if err != nil {
			return
		}
		// The DID should round-trip.
		if id.DID.String() != doc.ID {
			t.Fatalf("DID mismatch: %q vs %q", id.DID.String(), doc.ID)
		}
	})
}
