package lexicon

import "testing"

// FuzzParse tests that lexicon schema parsing never panics on arbitrary input.
func FuzzParse(f *testing.F) {
	f.Add([]byte(`{"lexicon":1,"id":"app.bsky.feed.post","defs":{"main":{"type":"record","key":"tid","record":{"type":"object","required":["text","createdAt"],"properties":{"text":{"type":"string"},"createdAt":{"type":"string","format":"datetime"}}}}}}`))
	f.Add([]byte(`{"lexicon":1,"id":"app.bsky.actor.getProfile","defs":{"main":{"type":"query","parameters":{"type":"params","required":["actor"],"properties":{"actor":{"type":"string"}}},"output":{"encoding":"application/json","schema":{"type":"ref","ref":"app.bsky.actor.defs#profileViewDetailed"}}}}}`))
	f.Add([]byte(`{"lexicon":1,"id":"com.atproto.repo.createRecord","defs":{"main":{"type":"procedure","input":{"encoding":"application/json","schema":{"type":"object","required":["repo","collection","record"],"properties":{"repo":{"type":"string"},"collection":{"type":"string"},"record":{"type":"unknown"}}}}}}}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"lexicon":2}`))
	f.Add([]byte(`not json`))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = Parse(data)
	})
}
