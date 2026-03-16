package plc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

const (
	maxResponseBody     = 1 << 20 // 1 MB
	defaultDirectoryURL = "https://plc.directory"
	defaultUserAgent    = "atmos/v0.1"
)

// ClientConfig holds optional configuration for a PLC client.
type ClientConfig struct {
	DirectoryURL gt.Option[string]
	HTTPClient   gt.Option[*http.Client]
	UserAgent    gt.Option[string]
}

// Client is an HTTP client for the PLC directory. Safe for concurrent use.
type Client struct {
	directoryURL string
	httpClient   *http.Client
	userAgent    string
}

// NewClient creates a new PLC client. Zero-value ClientConfig uses sensible defaults.
func NewClient(cfg ClientConfig) *Client {
	return &Client{
		directoryURL: cfg.DirectoryURL.ValOr(defaultDirectoryURL),
		httpClient:   cfg.HTTPClient.ValOr(xrpc.NewHTTPClient(30 * time.Second)),
		userAgent:    cfg.UserAgent.ValOr(defaultUserAgent),
	}
}

// Resolve fetches the current DID document.
func (c *Client) Resolve(ctx context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	body, err := c.get(ctx, "/"+string(did))
	if err != nil {
		return nil, err
	}
	return identity.ParseDIDDocument(body)
}

// OpLog returns active (non-nullified) operations as raw JSON.
func (c *Client) OpLog(ctx context.Context, did atmos.DID) ([]json.RawMessage, error) {
	body, err := c.get(ctx, "/"+string(did)+"/log")
	if err != nil {
		return nil, err
	}
	var ops []json.RawMessage
	if err := json.Unmarshal(body, &ops); err != nil {
		return nil, fmt.Errorf("plc: decode op log: %w", err)
	}
	return ops, nil
}

// AuditLog returns all operations including nullified, with metadata.
func (c *Client) AuditLog(ctx context.Context, did atmos.DID) ([]LogEntry, error) {
	body, err := c.get(ctx, "/"+string(did)+"/log/audit")
	if err != nil {
		return nil, err
	}
	var entries []LogEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		return nil, fmt.Errorf("plc: decode audit log: %w", err)
	}
	return entries, nil
}

// Submit sends a signed operation (Operation or TombstoneOp) to the directory.
func (c *Client) Submit(ctx context.Context, did atmos.DID, op any) error {
	data, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("plc: marshal operation: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.directoryURL+"/"+string(did), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("plc: submit: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
		return fmt.Errorf("plc: submit: HTTP %d: %s", resp.StatusCode, body)
	}
	return nil
}

func (c *Client) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.directoryURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("plc: request %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if err != nil {
		return nil, fmt.Errorf("plc: read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plc: HTTP %d: %s", resp.StatusCode, body)
	}

	return body, nil
}
