package xrpcserver_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/atmos/xrpcserver"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type errorEnvelope struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// testEnv bundles a test server with its own HTTP client to avoid sharing
// http.DefaultClient across parallel tests. httptest.Server.Close calls
// CloseIdleConnections on the default transport, which races with other
// tests' in-flight requests when using http.DefaultClient.
type testEnv struct {
	URL    string
	client *http.Client
}

func newTestServer(t *testing.T, s *xrpcserver.Server) testEnv {
	t.Helper()
	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)
	return testEnv{URL: ts.URL, client: ts.Client()}
}

func get(t *testing.T, env testEnv, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), "GET", url, nil)
	require.NoError(t, err)
	resp, err := env.client.Do(req)
	require.NoError(t, err)
	return resp
}

func post(t *testing.T, env testEnv, url, contentType string, body io.Reader) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), "POST", url, body)
	require.NoError(t, err)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := env.client.Do(req)
	require.NoError(t, err)
	return resp
}

func decodeError(t *testing.T, resp *http.Response) errorEnvelope {
	t.Helper()
	var env errorEnvelope
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
	return env
}

func TestRouting(t *testing.T) {
	t.Parallel()

	s := &xrpcserver.Server{}
	s.HandleQuery("com.example.ping", xrpcserver.QueryEmpty(func(_ context.Context, _ xrpcserver.Params) error {
		return nil
	}))

	ts := newTestServer(t, s)

	// Known NSID works.
	resp := get(t, ts, ts.URL+"/xrpc/com.example.ping")
	assert.Equal(t, 200, resp.StatusCode)
	_ = resp.Body.Close()

	// Unknown NSID → 400 MethodNotImplemented.
	resp = get(t, ts, ts.URL+"/xrpc/com.example.unknown")
	assert.Equal(t, 400, resp.StatusCode)
	env := decodeError(t, resp)
	assert.Equal(t, "MethodNotImplemented", env.Error)
	_ = resp.Body.Close()

	// Non-XRPC path → 404.
	resp = get(t, ts, ts.URL+"/other")
	assert.Equal(t, 404, resp.StatusCode)
	_ = resp.Body.Close()

	// Empty NSID → 400.
	resp = get(t, ts, ts.URL+"/xrpc/")
	assert.Equal(t, 400, resp.StatusCode)
	env = decodeError(t, resp)
	assert.Equal(t, "InvalidRequest", env.Error)
	_ = resp.Body.Close()
}

func TestMethodCheck(t *testing.T) {
	t.Parallel()

	s := &xrpcserver.Server{}
	s.HandleQuery("com.example.query", xrpcserver.QueryEmpty(func(_ context.Context, _ xrpcserver.Params) error {
		return nil
	}))
	s.HandleProcedure("com.example.proc", xrpcserver.ProcedureVoid(func(_ context.Context) error {
		return nil
	}))

	ts := newTestServer(t, s)

	// POST on query → 405.
	resp := post(t, ts, ts.URL+"/xrpc/com.example.query", "", nil)
	assert.Equal(t, 405, resp.StatusCode)
	env := decodeError(t, resp)
	assert.Equal(t, "MethodNotAllowed", env.Error)
	_ = resp.Body.Close()

	// GET on procedure → 405.
	resp = get(t, ts, ts.URL+"/xrpc/com.example.proc")
	assert.Equal(t, 405, resp.StatusCode)
	_ = resp.Body.Close()
}

func TestQueryJSON(t *testing.T) {
	t.Parallel()

	type output struct {
		Greeting string `json:"greeting"`
	}

	s := &xrpcserver.Server{}
	s.HandleQuery("com.example.hello", xrpcserver.Query(func(_ context.Context, p xrpcserver.Params) (*output, error) {
		name, err := p.String("name")
		if err != nil {
			return nil, err
		}
		return &output{Greeting: "hello " + name}, nil
	}))

	ts := newTestServer(t, s)

	resp := get(t, ts, ts.URL+"/xrpc/com.example.hello?name=world")
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var out output
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, "hello world", out.Greeting)
}

func TestQueryEmpty(t *testing.T) {
	t.Parallel()

	s := &xrpcserver.Server{}
	s.HandleQuery("com.example.noop", xrpcserver.QueryEmpty(func(_ context.Context, _ xrpcserver.Params) error {
		return nil
	}))

	ts := newTestServer(t, s)
	resp := get(t, ts, ts.URL+"/xrpc/com.example.noop")
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
	assert.Empty(t, body)
}

func TestProcedureJSON(t *testing.T) {
	t.Parallel()

	type input struct {
		X int `json:"x"`
		Y int `json:"y"`
	}
	type output struct {
		Sum int `json:"sum"`
	}

	s := &xrpcserver.Server{}
	s.HandleProcedure("com.example.add", xrpcserver.Procedure(func(_ context.Context, _ xrpcserver.Params, in *input) (*output, error) {
		return &output{Sum: in.X + in.Y}, nil
	}))

	ts := newTestServer(t, s)

	body, _ := json.Marshal(input{X: 3, Y: 4})
	resp := post(t, ts, ts.URL+"/xrpc/com.example.add", "application/json", bytes.NewReader(body))
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, 200, resp.StatusCode)

	var out output
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, 7, out.Sum)
}

func TestProcedureWithParams(t *testing.T) {
	t.Parallel()

	type input struct {
		Value string `json:"value"`
	}
	type output struct {
		Value string `json:"value"`
		Mode  string `json:"mode"`
	}

	s := &xrpcserver.Server{}
	s.HandleProcedure("com.example.proc", xrpcserver.Procedure(func(_ context.Context, p xrpcserver.Params, in *input) (*output, error) {
		return &output{
			Value: in.Value,
			Mode:  p.StringOr("mode", "default"),
		}, nil
	}))

	ts := newTestServer(t, s)

	body, _ := json.Marshal(input{Value: "test"})
	resp := post(t, ts, ts.URL+"/xrpc/com.example.proc?mode=fast", "application/json", bytes.NewReader(body))
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, 200, resp.StatusCode)

	var out output
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, "test", out.Value)
	assert.Equal(t, "fast", out.Mode)
}

func TestProcedureEmpty(t *testing.T) {
	t.Parallel()

	type input struct {
		Value string `json:"value"`
	}

	var received string
	s := &xrpcserver.Server{}
	s.HandleProcedure("com.example.store", xrpcserver.ProcedureEmpty(func(_ context.Context, in *input) error {
		received = in.Value
		return nil
	}))

	ts := newTestServer(t, s)

	body, _ := json.Marshal(input{Value: "test"})
	resp := post(t, ts, ts.URL+"/xrpc/com.example.store", "application/json", bytes.NewReader(body))
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
	assert.Empty(t, respBody)
	assert.Equal(t, "test", received)
}

func TestProcedureVoid(t *testing.T) {
	t.Parallel()

	var called bool
	s := &xrpcserver.Server{}
	s.HandleProcedure("com.example.void", xrpcserver.ProcedureVoid(func(_ context.Context) error {
		called = true
		return nil
	}))

	ts := newTestServer(t, s)

	resp := post(t, ts, ts.URL+"/xrpc/com.example.void", "", nil)
	_ = resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
	assert.True(t, called)
}

func TestError_XRPC(t *testing.T) {
	t.Parallel()

	s := &xrpcserver.Server{}
	s.HandleQuery("com.example.fail", xrpcserver.QueryEmpty(func(_ context.Context, _ xrpcserver.Params) error {
		return xrpcserver.NotFound("thing not found")
	}))

	ts := newTestServer(t, s)

	resp := get(t, ts, ts.URL+"/xrpc/com.example.fail")
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, 404, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	env := decodeError(t, resp)
	assert.Equal(t, "NotFound", env.Error)
	assert.Equal(t, "thing not found", env.Message)
}

func TestError_Plain(t *testing.T) {
	t.Parallel()

	s := &xrpcserver.Server{}
	s.HandleQuery("com.example.panic", xrpcserver.QueryEmpty(func(_ context.Context, _ xrpcserver.Params) error {
		return errors.New("secret internal detail")
	}))

	ts := newTestServer(t, s)

	resp := get(t, ts, ts.URL+"/xrpc/com.example.panic")
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, 500, resp.StatusCode)

	env := decodeError(t, resp)
	assert.Equal(t, "InternalServerError", env.Error)
	assert.NotContains(t, env.Message, "secret")
}

func TestParams_Required(t *testing.T) {
	t.Parallel()

	s := &xrpcserver.Server{}
	s.HandleQuery("com.example.req", xrpcserver.Query(func(_ context.Context, p xrpcserver.Params) (*struct{}, error) {
		_, err := p.String("name")
		if err != nil {
			return nil, err
		}
		return &struct{}{}, nil
	}))

	ts := newTestServer(t, s)

	resp := get(t, ts, ts.URL+"/xrpc/com.example.req")
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, 400, resp.StatusCode)

	env := decodeError(t, resp)
	assert.Equal(t, "InvalidRequest", env.Error)
}

func TestParams_Types(t *testing.T) {
	t.Parallel()

	type output struct {
		S  string   `json:"s"`
		N  int64    `json:"n"`
		B  bool     `json:"b"`
		SS []string `json:"ss"`
	}

	s := &xrpcserver.Server{}
	s.HandleQuery("com.example.types", xrpcserver.Query(func(_ context.Context, p xrpcserver.Params) (*output, error) {
		str, err := p.String("s")
		if err != nil {
			return nil, err
		}
		n, err := p.Int64("n")
		if err != nil {
			return nil, err
		}
		b, err := p.Bool("b")
		if err != nil {
			return nil, err
		}
		return &output{S: str, N: n, B: b, SS: p.Strings("ss")}, nil
	}))

	ts := newTestServer(t, s)

	resp := get(t, ts, ts.URL+"/xrpc/com.example.types?s=hello&n=42&b=true&ss=a&ss=b")
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, 200, resp.StatusCode)

	var out output
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, "hello", out.S)
	assert.Equal(t, int64(42), out.N)
	assert.True(t, out.B)
	assert.Equal(t, []string{"a", "b"}, out.SS)

	// Invalid int.
	resp2 := get(t, ts, ts.URL+"/xrpc/com.example.types?s=x&n=notint&b=true")
	_ = resp2.Body.Close()
	assert.Equal(t, 400, resp2.StatusCode)

	// Invalid bool.
	resp3 := get(t, ts, ts.URL+"/xrpc/com.example.types?s=x&n=1&b=notbool")
	_ = resp3.Body.Close()
	assert.Equal(t, 400, resp3.StatusCode)
}

func TestParams_Optional(t *testing.T) {
	t.Parallel()

	type output struct {
		S string `json:"s"`
		N int64  `json:"n"`
		B bool   `json:"b"`
		O bool   `json:"o"` // whether optional was present
	}

	s := &xrpcserver.Server{}
	s.HandleQuery("com.example.opt", xrpcserver.Query(func(_ context.Context, p xrpcserver.Params) (*output, error) {
		opt := p.StringOptional("cursor")
		return &output{
			S: p.StringOr("s", "default"),
			N: p.Int64Or("n", 99),
			B: p.BoolOr("b", true),
			O: opt.HasVal(),
		}, nil
	}))

	ts := newTestServer(t, s)

	// Defaults.
	resp := get(t, ts, ts.URL+"/xrpc/com.example.opt")
	defer func() { _ = resp.Body.Close() }()
	var out output
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, "default", out.S)
	assert.Equal(t, int64(99), out.N)
	assert.True(t, out.B)
	assert.False(t, out.O)

	// Overrides.
	resp2 := get(t, ts, ts.URL+"/xrpc/com.example.opt?s=x&n=1&b=false&cursor=abc")
	defer func() { _ = resp2.Body.Close() }()
	var out2 output
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&out2))
	assert.Equal(t, "x", out2.S)
	assert.Equal(t, int64(1), out2.N)
	assert.False(t, out2.B)
	assert.True(t, out2.O)
}

func TestParams_Has(t *testing.T) {
	t.Parallel()

	type output struct {
		HasA bool   `json:"hasA"`
		HasB bool   `json:"hasB"`
		A    string `json:"a"`
	}

	s := &xrpcserver.Server{}
	s.HandleQuery("com.example.has", xrpcserver.Query(func(_ context.Context, p xrpcserver.Params) (*output, error) {
		var a string
		if p.Has("a") {
			a, _ = p.String("a")
		}
		return &output{HasA: p.Has("a"), HasB: p.Has("b"), A: a}, nil
	}))

	ts := newTestServer(t, s)

	resp := get(t, ts, ts.URL+"/xrpc/com.example.has?a=val")
	defer func() { _ = resp.Body.Close() }()
	var out output
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.True(t, out.HasA)
	assert.False(t, out.HasB)
	assert.Equal(t, "val", out.A)
}

func TestParams_EmptyString(t *testing.T) {
	t.Parallel()

	s := &xrpcserver.Server{}
	s.HandleQuery("com.example.empty", xrpcserver.Query(func(_ context.Context, p xrpcserver.Params) (*struct {
		Val string `json:"val"`
		Has bool   `json:"has"`
	}, error) {
		v, err := p.String("key")
		if err != nil {
			return nil, err
		}
		return &struct {
			Val string `json:"val"`
			Has bool   `json:"has"`
		}{Val: v, Has: p.Has("key")}, nil
	}))

	ts := newTestServer(t, s)

	// ?key= is present with empty value — should succeed, not error.
	resp := get(t, ts, ts.URL+"/xrpc/com.example.empty?key=")
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, 200, resp.StatusCode)

	var out struct {
		Val string `json:"val"`
		Has bool   `json:"has"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, "", out.Val)
	assert.True(t, out.Has)
}

func TestRawQuery(t *testing.T) {
	t.Parallel()

	s := &xrpcserver.Server{}
	s.HandleQuery("com.example.blob", xrpcserver.RawQuery(func(_ context.Context, _ xrpcserver.Params, w http.ResponseWriter) error {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte{0xDE, 0xAD})
		return nil
	}))

	ts := newTestServer(t, s)

	resp := get(t, ts, ts.URL+"/xrpc/com.example.blob")
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, []byte{0xDE, 0xAD}, body)
}

func TestRawProcedure(t *testing.T) {
	t.Parallel()

	s := &xrpcserver.Server{}
	s.HandleProcedure("com.example.upload", xrpcserver.RawProcedure(func(_ context.Context, ct string, body io.Reader, w http.ResponseWriter) error {
		data, _ := io.ReadAll(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"size":%d,"ct":"%s"}`, len(data), ct)
		return nil
	}))

	ts := newTestServer(t, s)

	resp := post(t, ts, ts.URL+"/xrpc/com.example.upload", "image/png", bytes.NewReader([]byte("pngdata")))
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, 200, resp.StatusCode)

	var result struct {
		Size int    `json:"size"`
		CT   string `json:"ct"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, 7, result.Size)
	assert.Equal(t, "image/png", result.CT)
}

func TestRawQuery_ErrorAfterWrite(t *testing.T) {
	t.Parallel()

	s := &xrpcserver.Server{}
	s.HandleQuery("com.example.partial", xrpcserver.RawQuery(func(_ context.Context, _ xrpcserver.Params, w http.ResponseWriter) error {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte("partial"))
		return errors.New("oops after write")
	}))

	ts := newTestServer(t, s)

	// Should get the partial response, not a corrupted error envelope.
	resp := get(t, ts, ts.URL+"/xrpc/com.example.partial")
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, []byte("partial"), body)
}

func TestMaxRequestBody(t *testing.T) {
	t.Parallel()

	s := &xrpcserver.Server{MaxRequestBody: gt.Some(int64(10))}
	s.HandleProcedure("com.example.small", xrpcserver.ProcedureEmpty(func(_ context.Context, in *json.RawMessage) error {
		return nil
	}))

	ts := newTestServer(t, s)

	// Valid JSON that exceeds the 10-byte limit.
	bigBody := []byte(`{"data":"` + string(bytes.Repeat([]byte("a"), 100)) + `"}`)
	resp := post(t, ts, ts.URL+"/xrpc/com.example.small", "application/json", bytes.NewReader(bigBody))
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, 413, resp.StatusCode)
	env := decodeError(t, resp)
	assert.Equal(t, "TooLarge", env.Error)
}

func TestMiddleware(t *testing.T) {
	t.Parallel()

	s := &xrpcserver.Server{}
	s.HandleQuery("com.example.auth", xrpcserver.Query(func(ctx context.Context, _ xrpcserver.Params) (*struct {
		User string `json:"user"`
	}, error) {
		user, ok := ctx.Value(testUserKey{}).(string)
		if !ok {
			return nil, xrpcserver.AuthRequired("no user")
		}
		return &struct {
			User string `json:"user"`
		}{User: user}, nil
	}))

	// Wrap with a simple test middleware that sets a context value.
	handler := testAuthMiddleware(s)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	ts := testEnv{URL: srv.URL, client: srv.Client()}

	// Without auth header → 401.
	resp := get(t, ts, ts.URL+"/xrpc/com.example.auth")
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, 401, resp.StatusCode)

	// With auth header → 200.
	req, err := http.NewRequestWithContext(context.Background(), "GET", ts.URL+"/xrpc/com.example.auth", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer testuser")
	resp2, err := ts.client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp2.Body.Close() }()
	assert.Equal(t, 200, resp2.StatusCode)

	var out struct {
		User string `json:"user"`
	}
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&out))
	assert.Equal(t, "testuser", out.User)
}

type testUserKey struct{}

func testAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "" && len(auth) > 7 {
			ctx := context.WithValue(r.Context(), testUserKey{}, auth[7:])
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(w, r)
	})
}

func TestConcurrency(t *testing.T) {
	t.Parallel()

	type output struct {
		N int `json:"n"`
	}

	s := &xrpcserver.Server{}
	s.HandleQuery("com.example.echo", xrpcserver.Query(func(_ context.Context, p xrpcserver.Params) (*output, error) {
		n := p.Int64Or("n", 0)
		return &output{N: int(n)}, nil
	}))

	ts := newTestServer(t, s)

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(context.Background(), "GET", ts.URL+"/xrpc/com.example.echo?n="+strconv.Itoa(n%10), nil)
			if err != nil {
				return
			}
			resp, err := ts.client.Do(req)
			if err != nil {
				return
			}
			_ = resp.Body.Close()
		}(i)
	}
	wg.Wait()
}

func TestErrorConstructors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn     func(string) *xrpc.Error
		status int
		name   string
	}{
		{xrpcserver.InvalidRequest, 400, "InvalidRequest"},
		{xrpcserver.AuthRequired, 401, "AuthRequired"},
		{xrpcserver.Forbidden, 403, "Forbidden"},
		{xrpcserver.NotFound, 404, "NotFound"},
		{xrpcserver.MethodNotAllowed, 405, "MethodNotAllowed"},
		{xrpcserver.TooLarge, 413, "TooLarge"},
		{xrpcserver.RateLimited, 429, "RateLimited"},
		{xrpcserver.InternalError, 500, "InternalServerError"},
	}

	for _, tt := range tests {
		e := tt.fn("msg")
		assert.Equal(t, tt.status, e.StatusCode)
		assert.Equal(t, tt.name, e.Name)
		assert.Equal(t, "msg", e.Message)
	}
}
