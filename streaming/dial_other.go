//go:build !js

package streaming

import (
	"context"
	"net/http"
	"runtime/debug"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/xrpc"
)

const atmosModulePath = "github.com/jcalabro/atmos"

var atmosUserAgent = "atmos/" + atmosVersion()

func dial(ctx context.Context, u string, cfg DialConfig) (Conn, *http.Response, error) {
	opts := &websocket.DialOptions{
		HTTPHeader: http.Header{
			"User-Agent": []string{atmosUserAgent},
		},
		Subprotocols:    subprotocolStrings(cfg.Subprotocols),
		CompressionMode: cfg.Compression,
	}
	return websocket.Dial(ctx, u, opts)
}

// subprotocolStrings converts typed subprotocol tokens to the []string
// that websocket.DialOptions/AcceptOptions expect. nil in, nil out (no
// Sec-WebSocket-Protocol header is sent for an empty offer).
func subprotocolStrings(subs []xrpc.Subprotocol) []string {
	if len(subs) == 0 {
		return nil
	}
	out := make([]string, len(subs))
	for i, s := range subs {
		out[i] = string(s)
	}
	return out
}

func atmosVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	return atmosVersionFromBuildInfo(info)
}

func atmosVersionFromBuildInfo(info *debug.BuildInfo) string {
	if info == nil {
		return "unknown"
	}

	if info.Main.Path == atmosModulePath {
		return moduleVersion(info.Main)
	}

	for _, dep := range info.Deps {
		if dep.Path == atmosModulePath {
			return moduleVersion(*dep)
		}
	}

	return "unknown"
}

func moduleVersion(mod debug.Module) string {
	if mod.Replace != nil {
		if version := cleanModuleVersion(mod.Replace.Version); version != "" {
			return version
		}
		return "devel"
	}

	if version := cleanModuleVersion(mod.Version); version != "" {
		return version
	}
	return "devel"
}

func cleanModuleVersion(version string) string {
	if version == "" || version == "(devel)" {
		return ""
	}
	return version
}
