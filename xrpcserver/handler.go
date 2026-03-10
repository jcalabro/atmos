package xrpcserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// Handler handles an XRPC request. If the handler returns a non-nil error,
// the server writes an XRPC error envelope. Handlers that write directly to
// http.ResponseWriter (via RawQuery/RawProcedure) must not return an error
// after calling w.Write or w.WriteHeader.
type Handler interface {
	// ServeXRPC handles a single XRPC request and returns an error or nil.
	ServeXRPC(ctx context.Context, w http.ResponseWriter, r *Request) error
}

// HandlerFunc adapts a function to the Handler interface.
type HandlerFunc func(ctx context.Context, w http.ResponseWriter, r *Request) error

func (f HandlerFunc) ServeXRPC(ctx context.Context, w http.ResponseWriter, r *Request) error {
	return f(ctx, w, r)
}

// Request is the XRPC request passed to handlers.
type Request struct {
	NSID    string
	Params  Params
	HTTPReq *http.Request
}

// Query returns a Handler that decodes no body and JSON-encodes the output.
func Query[Out any](fn func(ctx context.Context, p Params) (*Out, error)) Handler {
	return HandlerFunc(func(ctx context.Context, w http.ResponseWriter, r *Request) error {
		out, err := fn(ctx, r.Params)
		if err != nil {
			return err
		}
		return writeJSON(w, out)
	})
}

// QueryEmpty returns a Handler for queries with no response body.
func QueryEmpty(fn func(ctx context.Context, p Params) error) Handler {
	return HandlerFunc(func(ctx context.Context, _ http.ResponseWriter, r *Request) error {
		return fn(ctx, r.Params)
	})
}

// Procedure returns a Handler that JSON-decodes input and JSON-encodes output.
// The callback also receives Params for procedures that accept query parameters.
func Procedure[In, Out any](fn func(ctx context.Context, p Params, input *In) (*Out, error)) Handler {
	return HandlerFunc(func(ctx context.Context, w http.ResponseWriter, r *Request) error {
		in, err := decodeBody[In](r)
		if err != nil {
			return err
		}
		out, err := fn(ctx, r.Params, in)
		if err != nil {
			return err
		}
		return writeJSON(w, out)
	})
}

// ProcedureEmpty returns a Handler that JSON-decodes input with no response body.
func ProcedureEmpty[In any](fn func(ctx context.Context, input *In) error) Handler {
	return HandlerFunc(func(ctx context.Context, _ http.ResponseWriter, r *Request) error {
		in, err := decodeBody[In](r)
		if err != nil {
			return err
		}
		return fn(ctx, in)
	})
}

// ProcedureVoid returns a Handler for procedures with no input or output.
func ProcedureVoid(fn func(ctx context.Context) error) Handler {
	return HandlerFunc(func(ctx context.Context, _ http.ResponseWriter, _ *Request) error {
		return fn(ctx)
	})
}

// RawQuery returns a Handler for queries that write binary output directly.
// The handler is responsible for setting Content-Type and writing the response.
func RawQuery(fn func(ctx context.Context, p Params, w http.ResponseWriter) error) Handler {
	return HandlerFunc(func(ctx context.Context, w http.ResponseWriter, r *Request) error {
		return fn(ctx, r.Params, w)
	})
}

// RawProcedure returns a Handler for procedures with binary input.
// The handler receives the Content-Type header and request body directly.
func RawProcedure(fn func(ctx context.Context, contentType string, body io.Reader, w http.ResponseWriter) error) Handler {
	return HandlerFunc(func(ctx context.Context, w http.ResponseWriter, r *Request) error {
		return fn(ctx, r.HTTPReq.Header.Get("Content-Type"), r.HTTPReq.Body, w)
	})
}

// decodeBody JSON-decodes the request body into a new value of type In.
// Returns a TooLarge error for MaxBytesReader violations, or InvalidRequest
// for other decode failures.
func decodeBody[In any](r *Request) (*In, error) {
	var in In
	if err := json.NewDecoder(r.HTTPReq.Body).Decode(&in); err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			return nil, TooLarge("request body too large")
		}
		return nil, InvalidRequest("invalid request body")
	}
	return &in, nil
}

func writeJSON(w http.ResponseWriter, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return InternalError("failed to encode response")
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
	return nil
}
