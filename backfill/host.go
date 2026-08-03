package backfill

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"unicode"

	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
)

// ValidateHostname validates an untrusted listHosts hostname for use by the
// default direct-PDS client builder. It intentionally accepts DNS hostnames
// only: no URL syntax, ports, IP literals, local names, or private naming
// suffixes.
func ValidateHostname(hostname string) error {
	if hostname == "" || len(hostname) > 253 {
		return fmt.Errorf("invalid hostname length")
	}
	if hostname != strings.TrimSpace(hostname) || strings.HasSuffix(hostname, ".") {
		return fmt.Errorf("hostname must be canonical DNS text")
	}
	lower := strings.ToLower(hostname)
	if net.ParseIP(lower) != nil {
		return fmt.Errorf("IP literals are not allowed")
	}
	if lower == "localhost" || !strings.Contains(lower, ".") {
		return fmt.Errorf("local or single-label hostname is not allowed")
	}
	for _, suffix := range []string{".local", ".internal", ".lan"} {
		if strings.HasSuffix(lower, suffix) {
			return fmt.Errorf("private hostname suffix %q is not allowed", suffix)
		}
	}
	for _, label := range strings.Split(lower, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return fmt.Errorf("invalid DNS label")
		}
		for _, r := range label {
			if r > unicode.MaxASCII {
				return fmt.Errorf("invalid DNS label character")
			}
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
				return fmt.Errorf("invalid DNS label character")
			}
		}
	}
	return nil
}

func defaultHostClientBuilder() func(string) (*atmossync.Client, error) {
	// One transport/pool is shared by the entire fleet. The engine owns all
	// retries; the HTTP and XRPC layers each perform one attempt.
	httpClient := jttp.New(xrpc.BulkDownloadOpts()...)
	return func(hostname string) (*atmossync.Client, error) {
		if err := ValidateHostname(hostname); err != nil {
			return nil, err
		}
		xc := &xrpc.Client{
			Host:       "https://" + strings.ToLower(hostname),
			HTTPClient: gt.Some[*http.Client](httpClient),
			Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
		}
		return atmossync.NewClient(atmossync.Options{Client: xc}), nil
	}
}
