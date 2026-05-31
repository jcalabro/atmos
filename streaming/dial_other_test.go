//go:build !js

package streaming

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAtmosVersionFromBuildInfo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		info *debug.BuildInfo
		want string
	}{
		{
			name: "dependency version",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app", Version: "(devel)"},
				Deps: []*debug.Module{
					{Path: atmosModulePath, Version: "v1.2.3"},
				},
			},
			want: "v1.2.3",
		},
		{
			name: "replacement module version",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app", Version: "(devel)"},
				Deps: []*debug.Module{
					{
						Path:    atmosModulePath,
						Version: "v1.2.3",
						Replace: &debug.Module{
							Path:    "example.com/fork/atmos",
							Version: "v1.2.4",
						},
					},
				},
			},
			want: "v1.2.4",
		},
		{
			name: "local replacement",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app", Version: "(devel)"},
				Deps: []*debug.Module{
					{
						Path:    atmosModulePath,
						Version: "v1.2.3",
						Replace: &debug.Module{
							Path: "../atmos",
						},
					},
				},
			},
			want: "devel",
		},
		{
			name: "main module",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: atmosModulePath, Version: "v1.2.3"},
			},
			want: "v1.2.3",
		},
		{
			name: "main module devel",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: atmosModulePath, Version: "(devel)"},
			},
			want: "devel",
		},
		{
			name: "missing",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app", Version: "(devel)"},
			},
			want: "unknown",
		},
		{
			name: "nil",
			info: nil,
			want: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, atmosVersionFromBuildInfo(tt.info))
		})
	}
}
