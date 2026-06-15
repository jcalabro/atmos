package atmos

import (
	"sync"
	"time"
)

// Base32SortAlphabet is the base32 alphabet used for TID encoding.
const Base32SortAlphabet = "234567abcdefghijklmnopqrstuvwxyz"

// TID represents a Timestamp Identifier (13 base32-sort characters).
type TID string

// ParseTID validates and returns a TID.
func ParseTID(raw string) (TID, error) {
	if len(raw) != 13 {
		return "", syntaxErr("TID", raw, "must be exactly 13 characters")
	}
	// First character restricted to [234567abcdefghij] (high bit cannot be set).
	if !isTIDFirstChar(raw[0]) {
		return "", syntaxErr("TID", raw, "invalid first character")
	}
	for i := 1; i < 13; i++ {
		if !isTIDChar(raw[i]) {
			return "", syntaxErr("TID", raw, "invalid character")
		}
	}
	return TID(raw), nil
}

// maxTIDMicros is the largest timestamp (microseconds since the Unix epoch)
// representable in a TID. A TID stores the timestamp in 53 bits — the high bit
// of the 64-bit value is always clear and the low 10 bits hold the clock ID.
const maxTIDMicros = int64(1)<<53 - 1

// maxTIDClockID is the largest representable clock ID (10 bits).
const maxTIDClockID = uint(1)<<10 - 1

// NewTID creates a TID from a microsecond timestamp and clock ID.
//
// It panics if unixMicros is negative or exceeds the 53-bit timestamp range, or
// if clockID exceeds 10 bits. These represent programmer errors: silently
// masking them would produce a syntactically valid but semantically corrupt TID
// (e.g. a negative timestamp wrapping to a far-future date), which is worse than
// a loud failure. Untrusted inputs arrive as strings via [ParseTID], which
// returns an error rather than panicking.
func NewTID(unixMicros int64, clockID uint) TID {
	if unixMicros < 0 || unixMicros > maxTIDMicros {
		panic("atmos: NewTID timestamp out of range [0, 2^53)")
	}
	if clockID > maxTIDClockID {
		panic("atmos: NewTID clockID out of range [0, 2^10)")
	}
	v := uint64(unixMicros)<<10 | uint64(clockID)
	return NewTIDFromInteger(v)
}

// NewTIDNow creates a TID from the current time.
func NewTIDNow(clockID uint) TID {
	return NewTID(time.Now().UnixMicro(), clockID)
}

// NewTIDFromTime creates a TID from a time.Time and clock ID.
func NewTIDFromTime(ts time.Time, clockID uint) TID {
	return NewTID(ts.UnixMicro(), clockID)
}

// NewTIDFromInteger creates a TID from a raw 64-bit value.
func NewTIDFromInteger(v uint64) TID {
	v &^= 1 << 63 // Clear high bit.
	var buf [13]byte
	for i := 12; i >= 0; i-- {
		buf[i] = Base32SortAlphabet[v&0x1F]
		v >>= 5
	}
	return TID(buf[:])
}

// Integer returns the raw 64-bit representation.
// Returns 0 for a zero-value (empty) TID.
//
// The result is only meaningful for a syntactically valid TID (one produced by
// [ParseTID] or the New* constructors). For a 13-character string cast that
// contains characters outside the base32-sort alphabet, the result is
// unspecified; call [TID.Validate] first if the value's provenance is untrusted.
func (t TID) Integer() uint64 {
	if len(t) != 13 {
		return 0
	}
	var v uint64
	for i := range 13 {
		v = (v << 5) | uint64(base32SortDecode(t[i]))
	}
	return v
}

// Time extracts the timestamp as a time.Time.
// Returns the zero time for a zero-value (empty) TID.
func (t TID) Time() time.Time {
	if len(t) != 13 {
		return time.Time{}
	}
	return time.UnixMicro(int64(t.Integer() >> 10))
}

// ClockID extracts the 10-bit clock ID.
// Returns 0 for a zero-value (empty) TID.
func (t TID) ClockID() uint {
	if len(t) != 13 {
		return 0
	}
	return uint(t.Integer() & 0x3FF)
}

// Validate reports whether t satisfies the TID syntax. Useful for
// values constructed by string casts (rather than [ParseTID]) that
// need a post-hoc syntax check. Returns nil on success or a typed
// syntax error that names the offending input.
func (t TID) Validate() error {
	_, err := ParseTID(string(t))
	return err
}

func (t TID) String() string {
	return string(t)
}

func (t TID) MarshalText() ([]byte, error) {
	return []byte(t), nil
}

func (t *TID) UnmarshalText(b []byte) error {
	parsed, err := ParseTID(string(b))
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}

// TIDClock generates monotonically increasing TIDs.
type TIDClock struct {
	clockID       uint
	mu            sync.Mutex
	lastUnixMicro int64
}

// ClockID returns the clock ID.
func (c *TIDClock) ClockID() uint {
	return c.clockID
}

// NewTIDClock creates a new TIDClock with the given clock ID.
func NewTIDClock(clockID uint) *TIDClock {
	return &TIDClock{clockID: clockID}
}

// ClockFromTID reconstructs a TIDClock from an existing TID.
func ClockFromTID(t TID) *TIDClock {
	v := t.Integer()
	return &TIDClock{
		clockID:       uint(v & 0x3FF),
		lastUnixMicro: int64(v >> 10),
	}
}

// Next generates the next TID, guaranteed greater than the previous.
func (c *TIDClock) Next() TID {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().UnixMicro()
	if now <= c.lastUnixMicro {
		now = c.lastUnixMicro + 1
	}
	c.lastUnixMicro = now
	return NewTID(now, c.clockID)
}

// isTIDFirstChar checks if the character is valid as the first TID character.
// Only [234567abcdefghij] — the lower half of the alphabet (high bit clear).
func isTIDFirstChar(c byte) bool {
	return (c >= '2' && c <= '7') || (c >= 'a' && c <= 'j')
}

// isTIDChar checks if the character is valid in a TID (any base32-sort char).
func isTIDChar(c byte) bool {
	return (c >= '2' && c <= '7') || (c >= 'a' && c <= 'z')
}

func base32SortDecode(c byte) byte {
	if c >= '2' && c <= '7' {
		return c - '2'
	}
	return c - 'a' + 6
}
