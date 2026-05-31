//go:build !js

package streaming

import (
	"context"
	"net/http"
	"runtime/debug"

	"github.com/coder/websocket"
)

const atmosModulePath = "github.com/jcalabro/atmos"

var atmosUserAgent = "atmos/" + atmosVersion()

func dial(ctx context.Context, u string) (*websocket.Conn, *http.Response, error) {
	return websocket.Dial(ctx, u, &websocket.DialOptions{
		HTTPHeader: http.Header{
			"User-Agent": []string{atmosUserAgent},
		},
	})
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
