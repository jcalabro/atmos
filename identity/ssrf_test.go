package identity

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsNonRoutableIP(t *testing.T) {
	t.Parallel()

	nonRoutable := []struct {
		name string
		ip   string
	}{
		{"loopback4", "127.0.0.1"},
		{"loopback4_other", "127.0.0.2"},
		{"loopback6", "::1"},
		{"private_10", "10.0.0.1"},
		{"private_172", "172.16.0.1"},
		{"private_192", "192.168.1.1"},
		{"link_local4", "169.254.1.1"},
		{"link_local6", "fe80::1"},
		{"unspecified4", "0.0.0.0"},
		{"unspecified6", "::"},
		{"private6_ula", "fd00::1"},
	}

	for _, tt := range nonRoutable {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ip := net.ParseIP(tt.ip)
			assert.True(t, isNonRoutableIP(ip), "%s should be non-routable", tt.ip)
		})
	}

	routable := []struct {
		name string
		ip   string
	}{
		{"public4", "8.8.8.8"},
		{"public4_other", "1.1.1.1"},
		{"public6", "2001:4860:4860::8888"},
		{"public4_93", "93.184.216.34"},
	}

	for _, tt := range routable {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ip := net.ParseIP(tt.ip)
			assert.False(t, isNonRoutableIP(ip), "%s should be routable", tt.ip)
		})
	}
}
