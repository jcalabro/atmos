package streaming

// inflightSeqs is a small sorted set of int64 seq numbers used by the
// readLoop to track which firehose seqs have been dispatched to the
// scheduler but not yet collected. Used to compute the watermark
// cursor: min(inflight) - 1 is the highest seq we can safely persist.
//
// Bounded by Workers + total per-key queue depth across all active
// keys. Concrete numbers under default Options.Parallelism = 32 and
// keyQueueCap = 64: a single hot DID maxes ~65; ~50 simultaneously
// active DIDs (one in-flight per worker plus a partial queue per key)
// can push toward 1-2k. Linear inserts/removes remain cheap at this
// size and avoid the overhead (and pointer chasing) of container/heap.
type inflightSeqs struct {
	xs []int64 // sorted ascending
}

// Add inserts seq into the set. O(n) worst case.
func (s *inflightSeqs) Add(seq int64) {
	// Find the first index i with xs[i] >= seq, insert there.
	i := 0
	for i < len(s.xs) && s.xs[i] < seq {
		i++
	}
	s.xs = append(s.xs, 0)
	copy(s.xs[i+1:], s.xs[i:])
	s.xs[i] = seq
}

// Remove deletes the first occurrence of seq. No-op if absent. O(n).
func (s *inflightSeqs) Remove(seq int64) {
	for i, v := range s.xs {
		if v == seq {
			s.xs = append(s.xs[:i], s.xs[i+1:]...)
			return
		}
	}
}

// Min returns the smallest seq, or 0 if empty.
func (s *inflightSeqs) Min() int64 {
	if len(s.xs) == 0 {
		return 0
	}
	return s.xs[0]
}

// Len returns the number of in-flight seqs.
func (s *inflightSeqs) Len() int { return len(s.xs) }
