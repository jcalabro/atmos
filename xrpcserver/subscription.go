package xrpcserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

// defaultWriteTimeout bounds how long a single WebSocket frame write can
// take. A wedged client surfaces as a Send error so the handler exits
// and the connection is torn down, rather than pinning a goroutine.
const defaultWriteTimeout = 5 * time.Second

// Message is a subscription message the server can frame on the wire.
// Both encodings are required so one handler serves every negotiated
// subprotocol; all lexgen-generated message types (and message unions)
// implement both.
//
// The JSON encoding must carry the message's full "<nsid>#<fragment>"
// in a "$type" field: pass the generated *_Message union (which stamps
// it automatically) or a generated struct with LexiconTypeID set.
// Stream.Send rejects payloads without one.
type Message interface {
	AppendCBOR(buf []byte) ([]byte, error)
	AppendJSON(buf []byte) ([]byte, error)
}

// SubscriptionHandler serves one subscription connection. It is called
// after the WebSocket upgrade with the negotiated Stream and blocks for
// the connection's lifetime: send events with stream.Send until done or
// ctx is cancelled (the context is derived from the request and is
// cancelled when the client disconnects).
//
// Return nil for a normal close. Returning an error closes the
// connection with websocket.StatusInternalError; no error detail is
// leaked to the client. To hand the client a typed stream error (e.g.
// FutureCursor), call stream.SendError first and then return nil.
type SubscriptionHandler func(ctx context.Context, p Params, stream *Stream) error

// SubscriptionConfig declares a subscription endpoint's wire contract.
type SubscriptionConfig struct {
	// Subprotocol is the stream's lexicon-declared default subprotocol:
	// what an unnegotiated connection (no Sec-WebSocket-Protocol offer,
	// or no recognized token) receives. None means xrpc.v0.cbor, the
	// protocol-wide default that preserves the behavior of every
	// existing subscription.
	Subprotocol gt.Option[xrpc.Subprotocol]

	// Subprotocols is the full set the endpoint supports and will
	// negotiate via Sec-WebSocket-Protocol. None means only the default.
	// The set must include the default: the spec requires servers to
	// support at minimum the lexicon-declared subprotocol.
	Subprotocols gt.Option[[]xrpc.Subprotocol]

	// Compression is the permessage-deflate mode offered to clients.
	// None means disabled. Context takeover compresses JSON streams
	// well (repeated field names compress against prior frames) but
	// costs a 32 KB sliding window per connection — opt in per the
	// endpoint's connection-count envelope.
	Compression gt.Option[websocket.CompressionMode]

	// WriteTimeout bounds each frame write. None means 5s. A write that
	// exceeds it fails the Send, signalling a wedged client.
	WriteTimeout gt.Option[time.Duration]

	// Validate, when set, runs before the WebSocket upgrade. An error
	// return (typically InvalidRequest) is written as a normal XRPC
	// HTTP error envelope and no upgrade happens. This is the only
	// chance to reject a request with an HTTP status; after upgrade,
	// errors travel as stream error frames.
	Validate gt.Option[func(ctx context.Context, p Params) error]
}

// subscriptionEntry is the resolved registration stored on the server.
type subscriptionEntry struct {
	defaultSub   xrpc.Subprotocol
	supported    []xrpc.Subprotocol
	compression  websocket.CompressionMode
	writeTimeout time.Duration
	validate     func(ctx context.Context, p Params) error
	handler      SubscriptionHandler
}

// HandleSubscription registers a WebSocket subscription (XRPC
// "subscription" type) endpoint. Unlike HandleQuery/HandleProcedure it
// returns an error because the config carries a wire contract that can
// be inconsistent.
//
// Like the other registration methods, it is not safe for concurrent
// use with ServeHTTP.
func (s *Server) HandleSubscription(nsid string, cfg SubscriptionConfig, fn SubscriptionHandler) error {
	if fn == nil {
		return errors.New("subscription handler must not be nil")
	}

	def := cfg.Subprotocol.ValOr(xrpc.SubprotocolV0CBOR)
	if !def.Valid() {
		return fmt.Errorf("unsupported default subprotocol: %q", def)
	}

	supported := cfg.Subprotocols.ValOr([]xrpc.Subprotocol{def})
	hasDefault := false
	for _, sub := range supported {
		if !sub.Valid() {
			return fmt.Errorf("unsupported subprotocol: %q", sub)
		}
		if sub == def {
			hasDefault = true
		}
	}
	if !hasDefault {
		// The spec requires servers to support at minimum the
		// lexicon-declared subprotocol.
		return fmt.Errorf("supported subprotocols %v must include the default %q", supported, def)
	}

	timeout := cfg.WriteTimeout.ValOr(defaultWriteTimeout)
	if timeout <= 0 {
		return errors.New("WriteTimeout must be > 0")
	}

	if s.handlers == nil {
		s.handlers = make(map[string]entry)
	}
	s.handlers[nsid] = entry{
		method: "GET",
		sub: &subscriptionEntry{
			defaultSub:   def,
			supported:    supported,
			compression:  cfg.Compression.ValOr(websocket.CompressionDisabled),
			writeTimeout: timeout,
			validate:     cfg.Validate.ValOr(nil),
			handler:      fn,
		},
	}
	return nil
}

// serveSubscription upgrades the request and runs the handler.
func (s *Server) serveSubscription(w http.ResponseWriter, r *http.Request, sub *subscriptionEntry, req *Request) {
	if sub.validate != nil {
		if err := sub.validate(r.Context(), req.Params); err != nil {
			writeError(w, err)
			return
		}
	}

	subs := make([]string, len(sub.supported))
	for i, sp := range sub.supported {
		subs[i] = string(sp)
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:    subs,
		CompressionMode: sub.compression,
	})
	if err != nil {
		// Accept has already written an HTTP error response.
		return
	}

	// Negotiation fallback (proposal 0015): when the client offered
	// nothing we recognize, Accept echoes no subprotocol and the
	// connection proceeds on the lexicon-declared default.
	negotiated := xrpc.Subprotocol(conn.Subprotocol())
	if negotiated == "" {
		negotiated = sub.defaultSub
	}

	// Subscriptions are server-push: the client sends nothing after the
	// handshake. CloseRead starts a background reader that keeps
	// control-frame handling alive (ping/pong, close) and cancels the
	// returned context when the client disconnects.
	ctx := conn.CloseRead(r.Context())

	stream := &Stream{
		conn:         conn,
		subprotocol:  negotiated,
		writeTimeout: sub.writeTimeout,
	}

	if err := sub.handler(ctx, req.Params, stream); err != nil {
		// Handler errors carry no wire representation post-upgrade; the
		// close code is all the client sees. Details stay server-side.
		_ = conn.Close(websocket.StatusInternalError, "internal error")
		return
	}
	_ = conn.Close(websocket.StatusNormalClosure, "")
}

// Stream is one subscription connection. It frames messages per the
// negotiated subprotocol:
//
//   - xrpc.v0.cbor: binary frames of two concatenated DASL-CBOR objects,
//     a {op, t} header followed by the message body.
//   - xrpc.v1.json: text frames of one self-describing JSON object,
//     {"$type":"message","payload":{...}}.
//
// Methods are safe for concurrent use; concurrent Sends serialize on
// the stream in unspecified order, and no frame is ever written after
// an error frame.
type Stream struct {
	conn         *websocket.Conn
	subprotocol  xrpc.Subprotocol
	writeTimeout time.Duration

	// mu serializes writes and makes the terminal check-and-write
	// atomic: without it a Send racing a SendError could put a message
	// frame on the wire after the error frame, which the event-stream
	// spec forbids (a stream closes immediately after an error frame).
	mu sync.Mutex

	// terminal is set once an error frame has been sent; the stream
	// must go quiet afterwards. Guarded by mu.
	terminal bool
}

// ErrStreamTerminal is returned by Send/SendError after an error frame
// has been sent on the stream.
var ErrStreamTerminal = errors.New("stream is terminal: an error frame was already sent")

// Subprotocol returns the subprotocol this connection negotiated (or
// fell back to).
func (st *Stream) Subprotocol() xrpc.Subprotocol {
	return st.subprotocol
}

// Send frames and writes one subscription message.
//
// fragment is the lexicon message fragment including the leading '#',
// e.g. "#commit" — it becomes the v0 header's "t" value. msg must JSON-
// encode with a "$type" of the full "<nsid>#<fragment>" (see Message);
// the fragment and the message's type must agree, which passing the
// generated *_Message union guarantees.
func (st *Stream) Send(ctx context.Context, fragment string, msg Message) error {
	if !strings.HasPrefix(fragment, "#") {
		return fmt.Errorf("fragment %q must start with '#'", fragment)
	}

	var frame []byte
	var msgType websocket.MessageType
	var err error
	switch st.subprotocol {
	case xrpc.SubprotocolV1JSON:
		msgType = websocket.MessageText
		frame, err = appendV1JSONMessageFrame(nil, msg)
	default: // xrpc.v0.cbor
		msgType = websocket.MessageBinary
		frame, err = appendV0MessageFrame(nil, fragment, msg)
	}
	if err != nil {
		return fmt.Errorf("encode %s frame: %w", fragment, err)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.terminal {
		return ErrStreamTerminal
	}
	return st.write(ctx, msgType, frame)
}

// SendError sends a stream error frame (e.g. "FutureCursor") and marks
// the stream terminal: all subsequent Send/SendError calls fail with
// ErrStreamTerminal. Per the event-stream spec the connection closes
// immediately after an error frame — return from the handler right
// after calling this. message may be empty.
func (st *Stream) SendError(ctx context.Context, code, message string) error {
	if code == "" {
		return errors.New("error code must not be empty")
	}

	var frame []byte
	var msgType websocket.MessageType
	switch st.subprotocol {
	case xrpc.SubprotocolV1JSON:
		msgType = websocket.MessageText
		frame = appendV1JSONErrorFrame(nil, code, message)
	default: // xrpc.v0.cbor
		msgType = websocket.MessageBinary
		frame = appendV0ErrorFrame(nil, code, message)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.terminal {
		return ErrStreamTerminal
	}
	st.terminal = true
	return st.write(ctx, msgType, frame)
}

func (st *Stream) write(ctx context.Context, msgType websocket.MessageType, frame []byte) error {
	ctx, cancel := context.WithTimeout(ctx, st.writeTimeout)
	defer cancel()
	return st.conn.Write(ctx, msgType, frame)
}

// appendV0MessageFrame appends a v0 message frame: the CBOR header
// {op: 1, t: fragment} followed by the message's CBOR encoding.
func appendV0MessageFrame(buf []byte, fragment string, msg Message) ([]byte, error) {
	buf = cbor.AppendMapHeader(buf, 2)
	buf = cbor.AppendTextKey(buf, "op")
	buf = cbor.AppendInt(buf, 1)
	buf = cbor.AppendTextKey(buf, "t")
	buf = cbor.AppendText(buf, fragment)
	return msg.AppendCBOR(buf)
}

// appendV0ErrorFrame appends a v0 error frame: the CBOR header
// {op: -1} followed by the {error, message} body.
func appendV0ErrorFrame(buf []byte, code, message string) []byte {
	buf = cbor.AppendMapHeader(buf, 1)
	buf = cbor.AppendTextKey(buf, "op")
	buf = cbor.AppendInt(buf, -1)

	fields := uint64(1)
	if message != "" {
		fields = 2
	}
	buf = cbor.AppendMapHeader(buf, fields)
	buf = cbor.AppendTextKey(buf, "error")
	buf = cbor.AppendText(buf, code)
	if message != "" {
		buf = cbor.AppendTextKey(buf, "message")
		buf = cbor.AppendText(buf, message)
	}
	return buf
}

// appendV1JSONMessageFrame appends a v1 message frame:
// {"$type":"message","payload":<msg JSON>}. The payload must carry a
// "$type" — a payload without one is not lexicon-decodable and is
// rejected here rather than shipped to every consumer.
func appendV1JSONMessageFrame(buf []byte, msg Message) ([]byte, error) {
	buf = append(buf, `{"$type":"message","payload":`...)
	payloadStart := len(buf)
	buf, err := msg.AppendJSON(buf)
	if err != nil {
		return nil, err
	}
	typ, err := cbor.PeekJSONType(buf[payloadStart:])
	if err != nil || typ == "" {
		return nil, errors.New("payload has no $type: pass the generated message union or set LexiconTypeID")
	}
	return append(buf, '}'), nil
}

// appendV1JSONErrorFrame appends a v1 error frame:
// {"$type":"error","error":code,"message":message}.
func appendV1JSONErrorFrame(buf []byte, code, message string) []byte {
	buf = append(buf, `{"$type":"error","error":`...)
	buf = cbor.AppendJSONString(buf, code)
	if message != "" {
		buf = append(buf, `,"message":`...)
		buf = cbor.AppendJSONString(buf, message)
	}
	return append(buf, '}')
}
