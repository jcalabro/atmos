package atmos

import "testing"

func BenchmarkParseDID(b *testing.B) {
	for b.Loop() {
		_, _ = ParseDID("did:plc:z72i7hdynmk6r22z27h6tvur")
	}
}

func BenchmarkParseHandle(b *testing.B) {
	for b.Loop() {
		_, _ = ParseHandle("alice.bsky.social")
	}
}

func BenchmarkParseNSID(b *testing.B) {
	for b.Loop() {
		_, _ = ParseNSID("app.bsky.feed.post")
	}
}

func BenchmarkParseATURI(b *testing.B) {
	for b.Loop() {
		_, _ = ParseATURI("at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3jt5tsfbx2s2a")
	}
}

func BenchmarkParseTID(b *testing.B) {
	for b.Loop() {
		_, _ = ParseTID("3jt5tsfbx2s2a")
	}
}

func BenchmarkTIDClock_Next(b *testing.B) {
	clock := NewTIDClock(1)
	for b.Loop() {
		_ = clock.Next()
	}
}
