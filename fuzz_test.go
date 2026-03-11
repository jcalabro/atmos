package atmos

import "testing"

// FuzzParseDID tests that DID parsing never panics and round-trips.
func FuzzParseDID(f *testing.F) {
	f.Add("did:plc:abc123def456ghij")
	f.Add("did:web:example.com")
	f.Add("")
	f.Add("did:")
	f.Add("not-a-did")

	f.Fuzz(func(t *testing.T, s string) {
		d, err := ParseDID(s)
		if err != nil {
			return
		}
		if d.String() != s {
			t.Fatalf("round-trip mismatch: %q vs %q", d.String(), s)
		}
	})
}

// FuzzParseHandle tests that handle parsing never panics and round-trips.
func FuzzParseHandle(f *testing.F) {
	f.Add("user.bsky.social")
	f.Add("example.com")
	f.Add("")
	f.Add(".")

	f.Fuzz(func(t *testing.T, s string) {
		h, err := ParseHandle(s)
		if err != nil {
			return
		}
		if h.String() != s {
			t.Fatalf("round-trip mismatch: %q vs %q", h.String(), s)
		}
	})
}

// FuzzParseNSID tests that NSID parsing never panics and round-trips.
func FuzzParseNSID(f *testing.F) {
	f.Add("app.bsky.feed.post")
	f.Add("com.example.record")
	f.Add("")
	f.Add("a.b")

	f.Fuzz(func(t *testing.T, s string) {
		n, err := ParseNSID(s)
		if err != nil {
			return
		}
		if n.String() != s {
			t.Fatalf("round-trip mismatch: %q vs %q", n.String(), s)
		}
	})
}

// FuzzParseATURI tests that AT-URI parsing never panics and round-trips.
func FuzzParseATURI(f *testing.F) {
	f.Add("at://did:plc:abc123/app.bsky.feed.post/tid")
	f.Add("at://user.bsky.social")
	f.Add("")
	f.Add("at://")

	f.Fuzz(func(t *testing.T, s string) {
		a, err := ParseATURI(s)
		if err != nil {
			return
		}
		if a.String() != s {
			t.Fatalf("round-trip mismatch: %q vs %q", a.String(), s)
		}
	})
}

// FuzzParseTID tests that TID parsing never panics and round-trips.
func FuzzParseTID(f *testing.F) {
	f.Add("3jqfcqzm3fo2j")
	f.Add("2222222222222")
	f.Add("")
	f.Add("zzzzzzzzzzzzz")

	f.Fuzz(func(t *testing.T, s string) {
		tid, err := ParseTID(s)
		if err != nil {
			return
		}
		if tid.String() != s {
			t.Fatalf("round-trip mismatch: %q vs %q", tid.String(), s)
		}
	})
}

// FuzzParseRecordKey tests that record key parsing never panics.
func FuzzParseRecordKey(f *testing.F) {
	f.Add("3jqfcqzm3fo2j")
	f.Add("self")
	f.Add("")
	f.Add(".")
	f.Add("..")

	f.Fuzz(func(t *testing.T, s string) {
		rk, err := ParseRecordKey(s)
		if err != nil {
			return
		}
		if rk.String() != s {
			t.Fatalf("round-trip mismatch: %q vs %q", rk.String(), s)
		}
	})
}

// FuzzParseDatetime tests that datetime parsing never panics and that
// successfully parsed values can be re-parsed from their String() output.
func FuzzParseDatetime(f *testing.F) {
	f.Add("2024-01-15T12:00:00Z")
	f.Add("2024-01-15T12:00:00.000Z")
	f.Add("2024-01-15T12:00:00+05:30")
	f.Add("")
	f.Add("not-a-date")

	f.Fuzz(func(t *testing.T, s string) {
		dt, err := ParseDatetime(s)
		if err != nil {
			return
		}
		// The String() output should itself be parseable.
		_, err = ParseDatetime(dt.String())
		if err != nil {
			t.Fatalf("round-trip parse failed: %v (original=%q, string=%q)", err, s, dt.String())
		}
	})
}

// FuzzParseLanguage tests that language tag parsing never panics and round-trips.
func FuzzParseLanguage(f *testing.F) {
	f.Add("en")
	f.Add("en-US")
	f.Add("zh-Hant-TW")
	f.Add("")

	f.Fuzz(func(t *testing.T, s string) {
		lang, err := ParseLanguage(s)
		if err != nil {
			return
		}
		if lang.String() != s {
			t.Fatalf("round-trip mismatch: %q vs %q", lang.String(), s)
		}
	})
}

// FuzzParseURI tests that URI parsing never panics and round-trips.
func FuzzParseURI(f *testing.F) {
	f.Add("https://example.com")
	f.Add("http://example.com/path?query=1#fragment")
	f.Add("")
	f.Add(":")

	f.Fuzz(func(t *testing.T, s string) {
		u, err := ParseURI(s)
		if err != nil {
			return
		}
		if u.String() != s {
			t.Fatalf("round-trip mismatch: %q vs %q", u.String(), s)
		}
	})
}

// FuzzParseAtIdentifier tests that AtIdentifier parsing never panics, round-trips,
// and maintains DID/Handle type consistency.
func FuzzParseAtIdentifier(f *testing.F) {
	f.Add("did:plc:abc123def456ghij")
	f.Add("did:web:example.com")
	f.Add("user.bsky.social")
	f.Add("example.com")
	f.Add("")
	f.Add("did:")

	f.Fuzz(func(t *testing.T, s string) {
		id, err := ParseAtIdentifier(s)
		if err != nil {
			return
		}
		if id.String() != s {
			t.Fatalf("round-trip mismatch: %q vs %q", id.String(), s)
		}
		// Type consistency: exactly one of IsDID/IsHandle must be true.
		if id.IsDID() == id.IsHandle() {
			t.Fatalf("IsDID=%v IsHandle=%v for %q — expected exactly one", id.IsDID(), id.IsHandle(), s)
		}
	})
}

// FuzzParseRepoPath tests that repo path parsing never panics and round-trips.
func FuzzParseRepoPath(f *testing.F) {
	f.Add("app.bsky.feed.post/3jqfcqzm3fo2j")
	f.Add("com.example.record/self")
	f.Add("")
	f.Add("/")
	f.Add("a/b/c")
	f.Add("app.bsky.feed.post/")

	f.Fuzz(func(t *testing.T, s string) {
		nsid, rkey, err := ParseRepoPath(s)
		if err != nil {
			return
		}
		reconstructed := nsid.String() + "/" + rkey.String()
		if reconstructed != s {
			t.Fatalf("round-trip mismatch: %q vs %q", reconstructed, s)
		}
	})
}
