package xrpcserver

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

const defaultMaxRequestBody = 2 << 20 // 2 MB

// Server is an XRPC HTTP server that routes /xrpc/{nsid} requests to handlers.
// It implements http.Handler and composes with any standard middleware.
//
// Register all handlers before calling ServeHTTP or passing to http.ListenAndServe.
// Registration methods (HandleQuery, HandleProcedure) are not safe for concurrent
// use with ServeHTTP.
type Server struct {
	// MaxRequestBody is the maximum size of a procedure request body.
	// None uses the default of 2 MB.
	MaxRequestBody gt.Option[int64]

	handlers map[string]entry
}

type entry struct {
	method  string // "GET" or "POST"
	handler Handler
}

// HandleQuery registers a handler for a query (GET) endpoint.
func (s *Server) HandleQuery(nsid string, h Handler) {
	s.register(nsid, "GET", h)
}

// HandleProcedure registers a handler for a procedure (POST) endpoint.
func (s *Server) HandleProcedure(nsid string, h Handler) {
	s.register(nsid, "POST", h)
}

func (s *Server) register(nsid, method string, h Handler) {
	if s.handlers == nil {
		s.handlers = make(map[string]entry)
	}
	s.handlers[nsid] = entry{method: method, handler: h}
}

func (s *Server) maxBody() int64 {
	return s.MaxRequestBody.ValOr(defaultMaxRequestBody)
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	const prefix = "/xrpc/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		http.NotFound(w, r)
		return
	}
	nsid := r.URL.Path[len(prefix):]
	if nsid == "" {
		writeError(w, InvalidRequest("missing XRPC method"))
		return
	}

	e, ok := s.handlers[nsid]
	if !ok {
		writeError(w, &xrpc.Error{StatusCode: 400, Name: "MethodNotImplemented", Message: "method not implemented: " + nsid})
		return
	}

	if r.Method != e.method {
		writeError(w, MethodNotAllowed("expected "+e.method))
		return
	}

	if e.method == "POST" {
		r.Body = http.MaxBytesReader(w, r.Body, s.maxBody())
	}

	req := &Request{
		NSID:    nsid,
		Params:  Params{vals: r.URL.Query()},
		HTTPReq: r,
	}

	if err := e.handler.ServeXRPC(r.Context(), w, req); err != nil {
		writeError(w, err)
	}
}

// headerWritten checks if response headers have already been sent by
// inspecting the Content-Type or status. This is a best-effort check:
// once WriteHeader or Write has been called, we cannot send an error envelope.
func headerWritten(w http.ResponseWriter) bool {
	// ResponseWriter does not expose "headers sent" directly, but if
	// Content-Type was set by a raw handler, that's a strong signal.
	return w.Header().Get("Content-Type") != ""
}

func writeError(w http.ResponseWriter, err error) {
	// If the handler already started writing (e.g. RawQuery/RawProcedure),
	// we cannot override the response — the error is lost. This matches
	// the behavior of http.Error when headers have been flushed.
	if headerWritten(w) {
		return
	}

	xe, ok := errors.AsType[*xrpc.Error](err)
	if !ok {
		xe = &xrpc.Error{StatusCode: 500, Name: "InternalServerError", Message: "internal server error"}
	}

	// Detect MaxBytesError and convert to 413.
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		xe = &xrpc.Error{StatusCode: 413, Name: "TooLarge", Message: "request body too large"}
	}

	body, _ := json.Marshal(errorBody{
		Error:   xe.Name,
		Message: xe.Message,
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(xe.StatusCode)
	_, _ = w.Write(body)
}

// errorBody is the JSON envelope for XRPC error responses.
type errorBody struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// Error constructors.

// AuthRequired returns a 401 AuthRequired XRPC error.
func AuthRequired(msg string) *xrpc.Error {
	return &xrpc.Error{StatusCode: 401, Name: "AuthRequired", Message: msg}
}

// Forbidden returns a 403 Forbidden XRPC error.
func Forbidden(msg string) *xrpc.Error {
	return &xrpc.Error{StatusCode: 403, Name: "Forbidden", Message: msg}
}

// NotFound returns a 404 NotFound XRPC error.
func NotFound(msg string) *xrpc.Error {
	return &xrpc.Error{StatusCode: 404, Name: "NotFound", Message: msg}
}

// MethodNotAllowed returns a 405 MethodNotAllowed XRPC error.
func MethodNotAllowed(msg string) *xrpc.Error {
	return &xrpc.Error{StatusCode: 405, Name: "MethodNotAllowed", Message: msg}
}

// TooLarge returns a 413 TooLarge XRPC error.
func TooLarge(msg string) *xrpc.Error {
	return &xrpc.Error{StatusCode: 413, Name: "TooLarge", Message: msg}
}

// RateLimited returns a 429 RateLimited XRPC error.
func RateLimited(msg string) *xrpc.Error {
	return &xrpc.Error{StatusCode: 429, Name: "RateLimited", Message: msg}
}

// InternalError returns a 500 InternalServerError XRPC error.
func InternalError(msg string) *xrpc.Error {
	return &xrpc.Error{StatusCode: 500, Name: "InternalServerError", Message: msg}
}
