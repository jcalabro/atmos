package backfill

import (
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
)

// Options configures the multi-host backfill engine.
type Options struct {
	// Relay is the listHosts source. Required.
	Relay *sync.Client

	// NewHostClient constructs a direct client for a validated listHosts
	// hostname. None uses HTTPS, one fleet-wide connection pool, and disabled
	// XRPC retries. An injected builder is an explicit trust boundary and may
	// support test-only host syntax such as ports.
	NewHostClient gt.Option[func(hostname string) (*sync.Client, error)]

	Store   Store
	Handler Handler

	// GlobalDownloads bounds aggregate in-flight getRepo attempts. None=256.
	GlobalDownloads gt.Option[int]
	// HostWorkers caps workers per host. The actual count is
	// clamp(relayAccountCount/10_000, 1, HostWorkers). None=32.
	HostWorkers gt.Option[int]
	// MaxActiveHosts bounds simultaneous listRepos producer loops. None=512.
	MaxActiveHosts gt.Option[int]
	// MaxHosts bounds the untrusted relay roster. None=50,000.
	MaxHosts gt.Option[int]
	// MaxPagesPerHost bounds listRepos responses consumed from one PDS across
	// all retries in a Run. None=10,000 (up to 10 million listed repos).
	MaxPagesPerHost gt.Option[int]

	HostBackoffBase gt.Option[time.Duration]
	HostBackoffMax  gt.Option[time.Duration]
	HostMaxAttempts gt.Option[int]

	// DiscoverOnly enumerates hosts and repos — firing OnHost, OnDiscover,
	// and OnUpdate — without downloading anything. Host cursors and drained
	// state advance exactly as in a downloading run, so a later downloading
	// Run against the same Store will NOT revisit these hosts; downloading
	// rows recorded during a DiscoverOnly run is the consumer's job (e.g., a
	// retry pass driven by Store state). None=false.
	DiscoverOnly       gt.Option[bool]
	IncludeBannedHosts gt.Option[bool]

	OnError            gt.Option[func(did atmos.DID, err error)]
	OnProgress         gt.Option[func(stats Stats)]
	OnHostState        gt.Option[func(host HostInfo, state HostState, attempts int, err error)]
	OnEntryError       gt.Option[func(err error)]
	OnHostnameRejected gt.Option[func(hostname string, err error)]
	OnRosterCapped     gt.Option[func(limit int)]
	OnDownloadSlotWait gt.Option[func(time.Duration)]

	// MaxRetries bounds ordinary transient retries. Together with
	// RetryRateLimitMaxAttempts, the larger value also bounds total retries
	// across both failure classes. None=1.
	MaxRetries     gt.Option[int]
	RetryBaseDelay gt.Option[time.Duration]
	RetryMaxDelay  gt.Option[time.Duration]
	// RetryRateLimitMaxAttempts bounds rate-limit retries. None=1.
	RetryRateLimitMaxAttempts gt.Option[int]
	DownloadTimeout           gt.Option[time.Duration]
	// MaxRepoBytes bounds the decoded CAR stream retained for one getRepo.
	// None=2 GiB.
	MaxRepoBytes  gt.Option[int64]
	Directory     gt.Option[*identity.Directory]
	VerifyCommits gt.Option[bool]
	BatchSize     gt.Option[int]
}

// Stats is a point-in-time snapshot delivered after lifecycle changes.
type Stats struct {
	Completed       int64
	Failed          int64
	ReposEnumerated int64
	ActiveHosts     int64
	HostsDrained    int64
	HostsExhausted  int64
}
