//go:build !js && !wasip1

package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeJetstreamCommit(t *testing.T) {
	t.Parallel()

	frame := []byte(`{
		"did": "did:plc:eygmaihciaxprqvxpfvl6flk",
		"time_us": 1725911162329308,
		"kind": "commit",
		"commit": {
			"rev": "3l3qo2vutsw2b",
			"operation": "create",
			"collection": "app.bsky.feed.like",
			"rkey": "3l3qo2vuowo2b",
			"record": {"$type": "app.bsky.feed.like", "subject": {"uri": "at://did:plc:test/app.bsky.feed.post/abc"}},
			"cid": "bafyreidwaivazkwu67xztlmuobx35hs2lnfh3kolmgfmucldvhd3sgzcqi"
		}
	}`)

	evt, err := decodeJetstreamFrame(frame)
	require.NoError(t, err)
	require.NotNil(t, evt.Jetstream)

	js := evt.Jetstream
	assert.Equal(t, "did:plc:eygmaihciaxprqvxpfvl6flk", js.DID)
	assert.Equal(t, int64(1725911162329308), js.TimeUS)
	assert.Equal(t, JetstreamKindCommit, js.Kind)
	require.NotNil(t, js.Commit)
	assert.Equal(t, "3l3qo2vutsw2b", js.Commit.Rev)
	assert.Equal(t, JetstreamOpCreate, js.Commit.Operation)
	assert.Equal(t, "app.bsky.feed.like", js.Commit.Collection)
	assert.Equal(t, "3l3qo2vuowo2b", js.Commit.RKey)
	assert.Equal(t, "bafyreidwaivazkwu67xztlmuobx35hs2lnfh3kolmgfmucldvhd3sgzcqi", js.Commit.CID)
	assert.NotNil(t, js.Commit.Record)

	// Verify record is valid JSON.
	var rec map[string]any
	require.NoError(t, json.Unmarshal(js.Commit.Record, &rec))
	assert.Equal(t, "app.bsky.feed.like", rec["$type"])

	// Commit events should not populate Account/Identity.
	assert.Nil(t, evt.Account)
	assert.Nil(t, evt.Identity)
}

func TestDecodeJetstreamAccount(t *testing.T) {
	t.Parallel()

	frame := []byte(`{
		"did": "did:plc:ugzroc5it3v5eij3lpk56cyg",
		"time_us": 1774185739577803,
		"kind": "account",
		"account": {
			"active": true,
			"did": "did:plc:ugzroc5it3v5eij3lpk56cyg",
			"seq": 28438003333,
			"time": "2026-03-22T13:22:18.959Z"
		}
	}`)

	evt, err := decodeJetstreamFrame(frame)
	require.NoError(t, err)
	require.NotNil(t, evt.Jetstream)
	require.NotNil(t, evt.Account)

	assert.Equal(t, "did:plc:ugzroc5it3v5eij3lpk56cyg", evt.Jetstream.DID)
	assert.Equal(t, int64(1774185739577803), evt.Jetstream.TimeUS)
	assert.Equal(t, JetstreamKindAccount, evt.Jetstream.Kind)
	assert.True(t, evt.Account.Active)
	assert.Equal(t, "did:plc:ugzroc5it3v5eij3lpk56cyg", evt.Account.DID)
	assert.Equal(t, int64(28438003333), evt.Account.Seq)

	// Account events populate both Jetstream and Account fields.
	assert.NotNil(t, evt.Jetstream.Account)
	assert.Equal(t, int64(28438003333), evt.Seq)
}

func TestDecodeJetstreamIdentity(t *testing.T) {
	t.Parallel()

	frame := []byte(`{
		"did": "did:plc:abc123",
		"time_us": 1725911162329308,
		"kind": "identity",
		"identity": {
			"did": "did:plc:abc123",
			"seq": 100,
			"time": "2024-09-09T19:46:02.102Z",
			"handle": "alice.bsky.social"
		}
	}`)

	evt, err := decodeJetstreamFrame(frame)
	require.NoError(t, err)
	require.NotNil(t, evt.Jetstream)
	require.NotNil(t, evt.Identity)

	assert.Equal(t, JetstreamKindIdentity, evt.Jetstream.Kind)
	assert.Equal(t, "did:plc:abc123", evt.Identity.DID)
	assert.Equal(t, int64(100), evt.Seq)
}

func TestDecodeJetstreamError(t *testing.T) {
	t.Parallel()

	frame := []byte(`{
		"error": "cursor_too_old",
		"message": "Use HTTP segment download for backfill beyond 7 days"
	}`)

	_, err := decodeJetstreamFrame(frame)
	require.Error(t, err)

	var jsErr *JetstreamError
	require.ErrorAs(t, err, &jsErr)
	assert.Equal(t, "cursor_too_old", jsErr.ErrorType)
	assert.Equal(t, "Use HTTP segment download for backfill beyond 7 days", jsErr.Message)
	assert.Contains(t, jsErr.Error(), "cursor_too_old")
}

func TestDecodeJetstreamUnknownKind(t *testing.T) {
	t.Parallel()

	frame := []byte(`{"did": "did:plc:test", "time_us": 123, "kind": "future_type"}`)
	_, err := decodeJetstreamFrame(frame)
	assert.ErrorIs(t, err, errUnknownType)
}

func TestDecodeJetstreamMalformed(t *testing.T) {
	t.Parallel()

	_, err := decodeJetstreamFrame([]byte(`not json`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode jetstream frame")
}

func TestDecodeJetstreamMissingKind(t *testing.T) {
	t.Parallel()

	frame := []byte(`{"did": "did:plc:test", "time_us": 123}`)
	_, err := decodeJetstreamFrame(frame)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing kind")
}

func TestDecodeJetstreamCommitDelete(t *testing.T) {
	t.Parallel()

	frame := []byte(`{
		"did": "did:plc:test",
		"time_us": 1725911162329308,
		"kind": "commit",
		"commit": {
			"rev": "3abc",
			"operation": "delete",
			"collection": "app.bsky.feed.post",
			"rkey": "xyz"
		}
	}`)

	evt, err := decodeJetstreamFrame(frame)
	require.NoError(t, err)
	require.NotNil(t, evt.Jetstream)
	require.NotNil(t, evt.Jetstream.Commit)

	assert.Equal(t, JetstreamOpDelete, evt.Jetstream.Commit.Operation)
	assert.Empty(t, evt.Jetstream.Commit.CID)
	assert.Nil(t, evt.Jetstream.Commit.Record)
}

func TestDecodeJetstreamCommitUpdate(t *testing.T) {
	t.Parallel()

	frame := []byte(`{
		"did": "did:plc:test",
		"time_us": 1725911162329308,
		"kind": "commit",
		"commit": {
			"rev": "3abc",
			"operation": "update",
			"collection": "app.bsky.feed.post",
			"rkey": "xyz",
			"record": {"text": "edited"},
			"cid": "bafytest"
		}
	}`)

	evt, err := decodeJetstreamFrame(frame)
	require.NoError(t, err)
	assert.Equal(t, JetstreamOpUpdate, evt.Jetstream.Commit.Operation)
	assert.Equal(t, "bafytest", evt.Jetstream.Commit.CID)
	assert.NotNil(t, evt.Jetstream.Commit.Record)
}

func TestIsJetstreamURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		url  string
		want bool
	}{
		{"wss://jetstream.bsky.network/subscribe", true},
		{"ws://localhost:4600/subscribe", true},
		{"wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos", false},
		{"wss://mod.bsky.app/xrpc/com.atproto.label.subscribeLabels", false},
		{"http://example.com/subscribe", true},
		{"wss://example.com/api/v1/subscribe", true},
		{"wss://example.com/subscribes", false},
		{"", false},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.want, isJetstreamURL(tt.url), "url=%q", tt.url)
	}
}

// --- Integration tests with mock Jetstream server ---

// startMockJetstream creates a test Jetstream server that sends JSON text frames.
func startMockJetstream(t *testing.T, handler func(conn *websocket.Conn, r *http.Request)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Logf("accept error: %v", err)
			return
		}
		handler(conn, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// jetstreamURL builds a Jetstream-style URL (ending in /subscribe) from an httptest server.
func jetstreamURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/subscribe"
}

// writeJetstreamFrames sends JSON text frames and closes the connection.
func writeJetstreamFrames(conn *websocket.Conn, frames ...[]byte) {
	ctx := context.Background()
	for _, f := range frames {
		_ = conn.Write(ctx, websocket.MessageText, f)
	}
	_ = conn.Close(websocket.StatusNormalClosure, "done")
}

func TestJetstream_Integration_MixedEvents(t *testing.T) {
	t.Parallel()

	commitFrame := []byte(`{
		"did": "did:plc:alice",
		"time_us": 1000001,
		"kind": "commit",
		"commit": {
			"rev": "rev1",
			"operation": "create",
			"collection": "app.bsky.feed.post",
			"rkey": "abc",
			"record": {"text": "hello"},
			"cid": "bafytest1"
		}
	}`)

	accountFrame := []byte(`{
		"did": "did:plc:bob",
		"time_us": 1000002,
		"kind": "account",
		"account": {
			"active": true,
			"did": "did:plc:bob",
			"seq": 42,
			"time": "2024-01-01T00:00:00Z"
		}
	}`)

	identityFrame := []byte(`{
		"did": "did:plc:carol",
		"time_us": 1000003,
		"kind": "identity",
		"identity": {
			"did": "did:plc:carol",
			"seq": 43,
			"time": "2024-01-01T00:00:00Z",
			"handle": "carol.bsky.social"
		}
	}`)

	srv := startMockJetstream(t, func(conn *websocket.Conn, _ *http.Request) {
		writeJetstreamFrames(conn, commitFrame, accountFrame, identityFrame)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{URL: jetstreamURL(srv)})

	var events []Event
	for evt, err := range client.Events(ctx) {
		require.NoError(t, err)
		events = append(events, evt)
		if len(events) == 3 {
			cancel()
		}
	}

	require.Len(t, events, 3)

	// Commit event.
	assert.NotNil(t, events[0].Jetstream)
	assert.NotNil(t, events[0].Jetstream.Commit)
	assert.Equal(t, "did:plc:alice", events[0].Jetstream.DID)
	assert.Equal(t, "app.bsky.feed.post", events[0].Jetstream.Commit.Collection)
	assert.Nil(t, events[0].Account)
	assert.Nil(t, events[0].Identity)

	// Account event — both Jetstream and Account populated.
	assert.NotNil(t, events[1].Jetstream)
	assert.NotNil(t, events[1].Account)
	assert.Equal(t, "did:plc:bob", events[1].Account.DID)
	assert.True(t, events[1].Account.Active)

	// Identity event — both Jetstream and Identity populated.
	assert.NotNil(t, events[2].Jetstream)
	assert.NotNil(t, events[2].Identity)
	assert.Equal(t, "did:plc:carol", events[2].Identity.DID)
}

func TestJetstream_Integration_ReconnectWithCursor(t *testing.T) {
	t.Parallel()

	conns := make(chan string, 10)

	srv := startMockJetstream(t, func(conn *websocket.Conn, r *http.Request) {
		conns <- r.URL.RawQuery
		ctx := context.Background()

		cursor := r.URL.Query().Get("cursor")
		if cursor == "" {
			// First connection: send 2 commit events with time_us for cursor.
			for _, ts := range []int64{1000001, 1000002} {
				frame := []byte(fmt.Sprintf(`{
					"did": "did:plc:test",
					"time_us": %d,
					"kind": "commit",
					"commit": {"rev": "r1", "operation": "create", "collection": "a.b.c", "rkey": "k", "record": {}}
				}`, ts))
				_ = conn.Write(ctx, websocket.MessageText, frame)
			}
			_ = conn.CloseNow()
			return
		}

		// Reconnection: send 1 more event.
		frame := []byte(`{
			"did": "did:plc:test",
			"time_us": 1000003,
			"kind": "commit",
			"commit": {"rev": "r2", "operation": "create", "collection": "a.b.c", "rkey": "k2", "record": {}}
		}`)
		_ = conn.Write(ctx, websocket.MessageText, frame)
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL: jetstreamURL(srv),
		Backoff: gt.Some(BackoffPolicy{
			InitialDelay: gt.Some(10 * time.Millisecond),
			MaxDelay:     gt.Some(50 * time.Millisecond),
			Jitter:       gt.Some(false),
		}),
	})

	var events []Event
	for evt, err := range client.Events(ctx) {
		if err != nil {
			continue
		}
		events = append(events, evt)
		if len(events) == 3 {
			cancel()
		}
	}

	require.Len(t, events, 3)

	// Verify reconnection included cursor param.
	<-conns // first connection
	q2 := <-conns
	assert.Contains(t, q2, "cursor=")
}

func TestJetstream_Integration_QueryParams(t *testing.T) {
	t.Parallel()

	queries := make(chan string, 5)

	srv := startMockJetstream(t, func(conn *websocket.Conn, r *http.Request) {
		queries <- r.URL.RawQuery
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{
		URL:         jetstreamURL(srv),
		Collections: gt.Some([]string{"app.bsky.feed.like", "app.bsky.feed.post"}),
		DIDs:        gt.Some([]string{"did:plc:alice"}),
	})

	for range client.Events(ctx) {
		// Server closes immediately; loop exits.
	}

	q := <-queries
	assert.Contains(t, q, "collections=")
	assert.Contains(t, q, "app.bsky.feed.like")
	assert.Contains(t, q, "app.bsky.feed.post")
	assert.Contains(t, q, "dids=did")
}

func TestJetstream_Integration_ErrorFrame(t *testing.T) {
	t.Parallel()

	srv := startMockJetstream(t, func(conn *websocket.Conn, _ *http.Request) {
		ctx := context.Background()

		// Send one good event, then an error frame.
		good := []byte(`{
			"did": "did:plc:test",
			"time_us": 1000001,
			"kind": "commit",
			"commit": {"rev": "r1", "operation": "create", "collection": "a.b.c", "rkey": "k1", "record": {}}
		}`)
		errFrame := []byte(`{"error": "cursor_too_old", "message": "too old"}`)

		_ = conn.Write(ctx, websocket.MessageText, good)
		_ = conn.Write(ctx, websocket.MessageText, errFrame)
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{URL: jetstreamURL(srv)})

	var events []Event
	var errs []error
	for evt, err := range client.Events(ctx) {
		if err != nil {
			errs = append(errs, err)
			cancel()
			continue
		}
		events = append(events, evt)
	}

	require.Len(t, events, 1)
	require.Len(t, errs, 1)

	// The error is wrapped in DecodeError by readLoop.
	var decErr *DecodeError
	require.ErrorAs(t, errs[0], &decErr)

	var jsErr *JetstreamError
	require.ErrorAs(t, decErr.Err, &jsErr)
	assert.Equal(t, "cursor_too_old", jsErr.ErrorType)
}

func TestJetstream_FirehoseURLNotDetected(t *testing.T) {
	t.Parallel()

	assert.False(t, isJetstreamURL("wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"))
	assert.False(t, isJetstreamURL("wss://mod.bsky.app/xrpc/com.atproto.label.subscribeLabels"))

	// Firehose client should not be created as Jetstream.
	client := mustNewClient(t, Options{
		URL: "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos",
	})
	assert.False(t, client.isJetstream)
}

func TestJetstream_Integration_CursorTracking(t *testing.T) {
	t.Parallel()

	srv := startMockJetstream(t, func(conn *websocket.Conn, _ *http.Request) {
		writeJetstreamFrames(conn,
			[]byte(`{"did":"did:plc:a","time_us":100,"kind":"commit","commit":{"rev":"r","operation":"create","collection":"x","rkey":"1","record":{}}}`),
			[]byte(`{"did":"did:plc:b","time_us":200,"kind":"commit","commit":{"rev":"r","operation":"create","collection":"x","rkey":"2","record":{}}}`),
			[]byte(`{"did":"did:plc:c","time_us":300,"kind":"account","account":{"active":true,"did":"did:plc:c","seq":999,"time":"2024-01-01T00:00:00Z"}}`),
		)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{URL: jetstreamURL(srv)})

	var events []Event
	for evt, err := range client.Events(ctx) {
		require.NoError(t, err)
		events = append(events, evt)
		if len(events) == 3 {
			cancel()
		}
	}

	require.Len(t, events, 3)

	// Commit events use time_us as cursor, account uses seq.
	// The last event is an account with seq=999, so cursor should be 999.
	assert.Equal(t, int64(999), client.Cursor())
}

func TestJetstream_Integration_UnknownKindSkipped(t *testing.T) {
	t.Parallel()

	srv := startMockJetstream(t, func(conn *websocket.Conn, _ *http.Request) {
		writeJetstreamFrames(conn,
			// Unknown kind — should be silently skipped.
			[]byte(`{"did":"did:plc:a","time_us":100,"kind":"future_event_type"}`),
			// Valid commit — should be received.
			[]byte(`{"did":"did:plc:b","time_us":200,"kind":"commit","commit":{"rev":"r","operation":"create","collection":"x","rkey":"1","record":{}}}`),
		)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mustNewClient(t, Options{URL: jetstreamURL(srv)})

	var events []Event
	for evt, err := range client.Events(ctx) {
		require.NoError(t, err)
		events = append(events, evt)
		if len(events) == 1 {
			cancel()
		}
	}

	require.Len(t, events, 1)
	assert.Equal(t, "did:plc:b", events[0].Jetstream.DID)
}
