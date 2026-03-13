package lexicon

import "testing"

// FuzzParse tests that lexicon schema parsing never panics on arbitrary input.
func FuzzParse(f *testing.F) {
	f.Add([]byte(`{"lexicon":1,"id":"app.bsky.feed.post","defs":{"main":{"type":"record","key":"tid","record":{"type":"object","required":["text","createdAt"],"properties":{"text":{"type":"string"},"createdAt":{"type":"string","format":"datetime"}}}}}}`))
	f.Add([]byte(`{"lexicon":1,"id":"app.bsky.actor.getProfile","defs":{"main":{"type":"query","parameters":{"type":"params","required":["actor"],"properties":{"actor":{"type":"string"}}},"output":{"encoding":"application/json","schema":{"type":"ref","ref":"app.bsky.actor.defs#profileViewDetailed"}}}}}`))
	f.Add([]byte(`{"lexicon":1,"id":"com.atproto.repo.createRecord","defs":{"main":{"type":"procedure","input":{"encoding":"application/json","schema":{"type":"object","required":["repo","collection","record"],"properties":{"repo":{"type":"string"},"collection":{"type":"string"},"record":{"type":"unknown"}}}}}}}`))
	// String def with constraints.
	f.Add([]byte(`{"lexicon":1,"id":"test.string","defs":{"main":{"type":"string","format":"did","maxLength":2048,"minLength":7,"enum":["did:plc:abc"],"const":"did:plc:abc"}}}`))
	// Integer def with bounds.
	f.Add([]byte(`{"lexicon":1,"id":"test.int","defs":{"main":{"type":"object","properties":{"n":{"type":"integer","minimum":0,"maximum":100}}}}}`))
	// Union with closed flag.
	f.Add([]byte(`{"lexicon":1,"id":"test.union","defs":{"main":{"type":"object","properties":{"val":{"type":"union","refs":["#a"],"closed":true}}},"a":{"type":"object","properties":{"x":{"type":"string"}}}}}`))
	// Ref to external schema.
	f.Add([]byte(`{"lexicon":1,"id":"test.ref","defs":{"main":{"type":"object","properties":{"r":{"type":"ref","ref":"com.example.target"}}}}}`))
	// Subscription with message union.
	f.Add([]byte(`{"lexicon":1,"id":"test.sub","defs":{"main":{"type":"subscription","message":{"schema":{"type":"union","refs":["#evt"]}},"errors":[{"name":"FutureCursor"}]},"evt":{"type":"object","properties":{"seq":{"type":"integer"}}}}}`))
	// Token def.
	f.Add([]byte(`{"lexicon":1,"id":"test.token","defs":{"myToken":{"type":"token","description":"a token"}}}`))
	// Blob field with accept and maxSize.
	f.Add([]byte(`{"lexicon":1,"id":"test.blob","defs":{"main":{"type":"object","properties":{"file":{"type":"blob","accept":["image/*","*/*"],"maxSize":5000000}}}}}`))
	// Bytes field.
	f.Add([]byte(`{"lexicon":1,"id":"test.bytes","defs":{"main":{"type":"object","properties":{"data":{"type":"bytes","minLength":1,"maxLength":1000000}}}}}`))
	// Edge cases.
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"lexicon":2}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(`{"lexicon":1,"id":"","defs":{}}`))
	f.Add([]byte(`{"lexicon":0,"id":"test","defs":{"main":{"type":"record"}}}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = Parse(data)
	})
}
