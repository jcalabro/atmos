package xrpc

import (
	"net/http"
	"testing"
	"time"
)

func TestNewTransport(t *testing.T) {
	t.Parallel()
	tr := NewTransport()

	if tr.MaxIdleConns != 100 {
		t.Errorf("MaxIdleConns = %d, want 100", tr.MaxIdleConns)
	}
	if tr.MaxIdleConnsPerHost != 50 {
		t.Errorf("MaxIdleConnsPerHost = %d, want 50", tr.MaxIdleConnsPerHost)
	}
	if tr.MaxConnsPerHost != 100 {
		t.Errorf("MaxConnsPerHost = %d, want 100", tr.MaxConnsPerHost)
	}
	if tr.IdleConnTimeout != 90*time.Second {
		t.Errorf("IdleConnTimeout = %v, want 90s", tr.IdleConnTimeout)
	}
	if tr.TLSHandshakeTimeout != 5*time.Second {
		t.Errorf("TLSHandshakeTimeout = %v, want 5s", tr.TLSHandshakeTimeout)
	}
	if tr.ResponseHeaderTimeout != 15*time.Second {
		t.Errorf("ResponseHeaderTimeout = %v, want 15s", tr.ResponseHeaderTimeout)
	}
	if tr.ExpectContinueTimeout != 1*time.Second {
		t.Errorf("ExpectContinueTimeout = %v, want 1s", tr.ExpectContinueTimeout)
	}
	if !tr.ForceAttemptHTTP2 {
		t.Error("ForceAttemptHTTP2 should be true")
	}
	if tr.TLSClientConfig == nil || tr.TLSClientConfig.MinVersion != 0x0303 {
		t.Error("TLS MinVersion should be TLS 1.2 (0x0303)")
	}
	if tr.Proxy == nil {
		t.Error("Proxy should be set (ProxyFromEnvironment)")
	}
}

func TestNewTransport_IndependentInstances(t *testing.T) {
	t.Parallel()
	tr1 := NewTransport()
	tr2 := NewTransport()
	if tr1 == tr2 {
		t.Error("NewTransport should return distinct instances")
	}
}

func TestNewHTTPClient(t *testing.T) {
	t.Parallel()
	c := NewHTTPClient(5 * time.Second)

	if c.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v, want 5s", c.Timeout)
	}
	if c.Transport == nil {
		t.Fatal("Transport should not be nil")
	}
	if c.Transport == http.DefaultTransport {
		t.Error("Transport should not be http.DefaultTransport")
	}
}
