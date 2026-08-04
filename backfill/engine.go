package backfill

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jcalabro/atmos"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
)

var errOnCompleteRecorded = errors.New("backfill: OnComplete recording failed; handler already ran")
var errDownloadTimeout = errors.New("backfill: repo download exceeded DownloadTimeout")
var ErrEngineAlreadyRan = errors.New("backfill: Engine.Run already invoked; engines are single-shot")

const (
	listPageLimit                    = 1000
	defaultBatchSize                 = 5000
	defaultGlobalDownloads           = 256
	defaultHostWorkers               = 32
	defaultMaxActiveHosts            = 512
	defaultMaxHosts                  = 50_000
	defaultHostBackoffBase           = time.Minute
	defaultHostBackoffMax            = time.Hour
	defaultHostMaxAttempts           = 8
	DefaultMaxRetries                = 3
	DefaultRetryRateLimitMaxAttempts = 20
	defaultRetryBaseDelay            = time.Second
	defaultRetryMaxDelay             = 30 * time.Second
	DefaultDownloadTimeout           = 5 * time.Minute
	retryRateLimitCeiling            = 330 * time.Second
	didLockShards                    = 256
)

type retrySleeper interface {
	Sleep(context.Context, time.Duration) error
}

type timerRetrySleeper struct{}

func (timerRetrySleeper) Sleep(ctx context.Context, delay time.Duration) error {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Engine enumerates the relay's host roster and runs independent direct-PDS
// listRepos/download pipelines behind fleet-wide concurrency controls.
type Engine struct {
	opts Options

	retrySleeper retrySleeper
	started      atomic.Bool

	completed      atomic.Int64
	failed         atomic.Int64
	enumerated     atomic.Int64
	activeHosts    atomic.Int64
	hostsDrained   atomic.Int64
	hostsExhausted atomic.Int64
	progressMu     sync.Mutex

	globalSlots chan struct{}
	builder     func(string) (*atmossync.Client, error)

	claimMu sync.Mutex
	claims  map[atmos.DID]*didClaim
	didMu   [didLockShards]sync.Mutex
}

// didClaim is the in-Run ownership record for a DID that appears on more
// than one host (migration windows). The claiming host's worker resolves it
// after the DID's terminal Store transition; other hosts listing the same
// DID must not checkpoint a cursor past it until then. terminal is published
// before done closes, so waiters may read it after <-done without locking.
type didClaim struct {
	done     chan struct{}
	terminal bool
}

// errClaimNotTerminal marks a batch barrier that observed a cross-host DID
// claim resolve without a terminal Store transition (the owning host's
// producer aborted before dispatch, or the run is unwinding). The waiting
// host's attempt fails retryably — its cursor is not saved past the DID —
// rather than fatally.
var errClaimNotTerminal = errors.New("backfill: cross-host claim resolved without a terminal state")

func NewEngine(opts Options) *Engine { return &Engine{opts: opts} }

type hostCandidate struct {
	info HostInfo
}

// Run is single-shot. A nil return means every eligible host observed by a
// final listHosts pass is either drained or explicitly exhausted, and every
// repo dispatched by a drained host reached a terminal Store transition.
func (e *Engine) Run(ctx context.Context) error {
	if !e.started.CompareAndSwap(false, true) {
		return ErrEngineAlreadyRan
	}
	if err := e.validate(); err != nil {
		return err
	}
	e.globalSlots = make(chan struct{}, e.globalDownloadCount())
	e.claims = make(map[atmos.DID]*didClaim)
	if e.opts.NewHostClient.HasVal() {
		e.builder = e.opts.NewHostClient.Val()
	} else {
		e.builder = defaultHostClientBuilder()
	}

	terminal := make(map[string]HostState)
	for {
		hosts, err := e.enumerateHostsWithRetry(ctx, terminal)
		if err != nil {
			return err
		}
		if len(hosts) == 0 {
			return nil
		}
		if err := e.runFleet(ctx, hosts, terminal); err != nil {
			return err
		}
		// Re-list unconditionally. This is both the new-host race closure and
		// the terminal proof for the fleet observed above.
	}
}

func (e *Engine) enumerateHostsWithRetry(ctx context.Context, terminal map[string]HostState) ([]hostCandidate, error) {
	var lastErr error
	for attempt := 1; attempt <= e.hostMaxAttempts(); attempt++ {
		hosts, err := e.enumerateHosts(ctx, terminal)
		if err == nil {
			return hosts, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !xrpc.IsTransient(err) || attempt == e.hostMaxAttempts() {
			return nil, err
		}
		delay := backoffDelay(e.hostBackoffBase(), e.hostBackoffMax(), attempt-1)
		if xrpc.IsRateLimited(err) {
			delay = rateLimitDelay(err, e.hostBackoffBase(), attempt)
		}
		if err := e.sleep(ctx, delay); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func (e *Engine) validate() error {
	if e.opts.Relay == nil {
		return fmt.Errorf("backfill: Relay is required")
	}
	if e.opts.Store == nil {
		return fmt.Errorf("backfill: Store is required")
	}
	if e.opts.Handler == nil {
		return fmt.Errorf("backfill: Handler is required")
	}
	if e.opts.VerifyCommits.ValOr(false) && !e.opts.Directory.HasVal() {
		return fmt.Errorf("backfill: Directory is required when VerifyCommits is set")
	}
	for name, value := range map[string]int{
		"GlobalDownloads": e.globalDownloadCount(), "HostWorkers": e.hostWorkerLimit(),
		"MaxActiveHosts": e.maxActiveHosts(), "MaxHosts": e.maxHosts(),
		"HostMaxAttempts": e.hostMaxAttempts(),
	} {
		if value <= 0 {
			return fmt.Errorf("backfill: %s must be positive", name)
		}
	}
	if e.batchSize() <= 0 {
		return fmt.Errorf("backfill: BatchSize must be positive")
	}
	for name, value := range map[string]int{
		"MaxRetries":                e.opts.MaxRetries.ValOr(DefaultMaxRetries),
		"RetryRateLimitMaxAttempts": e.opts.RetryRateLimitMaxAttempts.ValOr(DefaultRetryRateLimitMaxAttempts),
	} {
		if value < 0 {
			return fmt.Errorf("backfill: %s must be non-negative", name)
		}
	}
	for name, value := range map[string]time.Duration{
		"HostBackoffBase": e.hostBackoffBase(), "HostBackoffMax": e.hostBackoffMax(),
		"RetryBaseDelay":  e.opts.RetryBaseDelay.ValOr(defaultRetryBaseDelay),
		"RetryMaxDelay":   e.opts.RetryMaxDelay.ValOr(defaultRetryMaxDelay),
		"DownloadTimeout": e.downloadTimeout(),
	} {
		if value < 0 {
			return fmt.Errorf("backfill: %s must be non-negative", name)
		}
	}
	return nil
}

func (e *Engine) enumerateHosts(ctx context.Context, terminal map[string]HostState) ([]hostCandidate, error) {
	seen := make(map[string]struct{})
	hosts := make([]hostCandidate, 0)
	examined := 0
pages:
	for page, err := range e.opts.Relay.ListHosts(ctx, listPageLimit, "") {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			var entryErr *atmossync.ListEntryError
			if errors.As(err, &entryErr) {
				e.notifyEntryError(err)
				continue
			}
			return nil, fmt.Errorf("backfill: listHosts: %w", err)
		}
		for _, entry := range page.Entries {
			// The cap counts every examined entry — before dedup and
			// validation — so a hostile relay cannot bypass it with
			// duplicates or junk names; it bounds total work, not just
			// retained roster size.
			if examined >= e.maxHosts() {
				if cb := e.opts.OnRosterCapped; cb.HasVal() {
					cb.Val()(e.maxHosts())
				}
				break pages
			}
			examined++
			hostname := strings.ToLower(entry.Hostname)
			if _, ok := seen[hostname]; ok {
				continue
			}
			if !e.opts.NewHostClient.HasVal() {
				if err := ValidateHostname(hostname); err != nil {
					if cb := e.opts.OnHostnameRejected; cb.HasVal() {
						cb.Val()(entry.Hostname, err)
					}
					continue
				}
			}
			seen[hostname] = struct{}{}
			info := HostInfo{Hostname: hostname, RelayStatus: entry.Status, RelayAccounts: entry.AccountCount, Seq: entry.Seq}
			if err := e.opts.Store.OnHost(ctx, info); err != nil {
				return nil, fmt.Errorf("backfill: store on_host %s: %w", hostname, err)
			}
			if _, ok := terminal[hostname]; ok {
				continue
			}
			_, drained, err := e.opts.Store.HostCursor(ctx, hostname)
			if err != nil {
				return nil, fmt.Errorf("backfill: store host_cursor %s: %w", hostname, err)
			}
			if drained {
				terminal[hostname] = HostStateDrained
				continue
			}
			if entry.Status == "banned" && !e.opts.IncludeBannedHosts.ValOr(false) {
				continue
			}
			hosts = append(hosts, hostCandidate{info: info})
		}
	}
	// The ListHosts iterator also returns silently on cancellation; without
	// this check a cancel between pages could look like an empty roster and
	// let Run return nil as if the fleet had converged.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sort.Slice(hosts, func(i, j int) bool {
		if hosts[i].info.RelayAccounts == hosts[j].info.RelayAccounts {
			return hosts[i].info.Hostname < hosts[j].info.Hostname
		}
		return hosts[i].info.RelayAccounts > hosts[j].info.RelayAccounts
	})
	return hosts, nil
}

func (e *Engine) runFleet(ctx context.Context, hosts []hostCandidate, terminal map[string]HostState) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	active := make(chan struct{}, e.maxActiveHosts())
	errs := make(chan error, len(hosts))
	var wg sync.WaitGroup
	var terminalMu sync.Mutex
	for _, host := range hosts {
		wg.Go(func() {
			state, err := e.runHostManager(runCtx, active, host.info)
			if err != nil {
				errs <- err
				cancel()
				return
			}
			terminalMu.Lock()
			terminal[host.info.Hostname] = state
			terminalMu.Unlock()
		})
	}
	wg.Wait()
	close(errs)
	var cancellationErr error
	for err := range errs {
		if err == nil {
			continue
		}
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if cancellationErr == nil {
			cancellationErr = err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return cancellationErr
}

func (e *Engine) runHostManager(ctx context.Context, active chan struct{}, host HostInfo) (HostState, error) {
	var client *atmossync.Client
	var lastErr error
	for attempt := 1; attempt <= e.hostMaxAttempts(); attempt++ {
		e.notifyHostState(host, HostStatePending, attempt, nil)
		if client == nil {
			var err error
			client, err = e.builder(host.Hostname)
			if err != nil {
				lastErr = fmt.Errorf("backfill: build host client %s: %w", host.Hostname, err)
			} else if client == nil {
				lastErr = fmt.Errorf("backfill: build host client %s: returned nil client", host.Hostname)
				client = nil
			}
		}
		if client != nil {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case active <- struct{}{}:
			}
			e.activeHosts.Add(1)
			e.notifyHostState(host, HostStateRunning, attempt, nil)
			lastNonEmpty, runErr := e.runHostAttempt(ctx, host, client)
			<-active
			e.activeHosts.Add(-1)
			if runErr == nil {
				if err := e.opts.Store.OnHostDrained(ctx, host.Hostname, lastNonEmpty); err != nil {
					return "", fmt.Errorf("backfill: store on_host_drained %s: %w", host.Hostname, err)
				}
				e.hostsDrained.Add(1)
				e.notifyHostState(host, HostStateDrained, attempt, nil)
				return HostStateDrained, nil
			}
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			var fatal *fatalStoreError
			if errors.As(runErr, &fatal) {
				return "", fatal.err
			}
			lastErr = runErr
		}
		if attempt == e.hostMaxAttempts() {
			break
		}
		e.notifyHostState(host, HostStateBackoff, attempt, lastErr)
		if err := e.sleep(ctx, backoffDelay(e.hostBackoffBase(), e.hostBackoffMax(), attempt-1)); err != nil {
			return "", err
		}
	}
	return e.exhaustHost(ctx, host, lastErr, e.hostMaxAttempts())
}

func (e *Engine) exhaustHost(ctx context.Context, host HostInfo, cause error, attempts int) (HostState, error) {
	if err := e.opts.Store.OnHostExhausted(ctx, host.Hostname, cause, attempts); err != nil {
		return "", fmt.Errorf("backfill: store on_host_exhausted %s: %w", host.Hostname, err)
	}
	e.hostsExhausted.Add(1)
	e.notifyHostState(host, HostStateExhausted, attempts, cause)
	return HostStateExhausted, nil
}

type fatalStoreError struct{ err error }

func (e *fatalStoreError) Error() string { return e.err.Error() }
func (e *fatalStoreError) Unwrap() error { return e.err }

type repoJob struct {
	host   string
	client *atmossync.Client
	entry  atmossync.ListReposEntry
	done   chan<- error

	// claim is set on the job that owns the DID for this Run; the worker
	// resolves it after the DID's terminal Store transition.
	claim *didClaim
	// wait is set instead of claim when another host's pipeline already owns
	// the DID. The worker blocks until the owner resolves, so this host's
	// batch barrier — and therefore its cursor — cannot advance past a DID
	// that never reached a terminal state.
	wait *didClaim
}

func (e *Engine) runHostAttempt(ctx context.Context, host HostInfo, client *atmossync.Client) (string, error) {
	startCursor, _, err := e.opts.Store.HostCursor(ctx, host.Hostname)
	if err != nil {
		return "", &fatalStoreError{fmt.Errorf("backfill: store host_cursor %s: %w", host.Hostname, err)}
	}
	workers := e.workerCount(host.RelayAccounts)
	jobs := make(chan repoJob, workers*2)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() { e.workerLoop(ctx, jobs) })
	}
	defer func() { close(jobs); wg.Wait() }()

	batch := make([]repoJob, 0, e.batchSize())
	batchEntries := 0
	batchCursor := ""
	lastNonEmpty := startCursor
	for page, listErr := range client.ListRepos(ctx, listPageLimit, startCursor) {
		if ctx.Err() != nil {
			e.releaseClaims(batch)
			return "", ctx.Err()
		}
		if listErr != nil {
			var entryErr *atmossync.ListEntryError
			if errors.As(listErr, &entryErr) {
				e.notifyEntryError(listErr)
				continue
			}
			e.releaseClaims(batch)
			return "", fmt.Errorf("backfill: host %s listRepos: %w", host.Hostname, listErr)
		}
		batchEntries += len(page.Entries)
		batchCursor = page.NextCursor
		if page.NextCursor != "" {
			lastNonEmpty = page.NextCursor
		}
		for _, entry := range page.Entries {
			e.enumerated.Add(1)
			job, dispatch, recErr := e.reconcile(ctx, host.Hostname, client, entry)
			if recErr != nil {
				e.releaseClaims(batch)
				return "", &fatalStoreError{recErr}
			}
			if dispatch {
				batch = append(batch, job)
			}
		}
		if batchEntries >= e.batchSize() {
			if err := e.finishBatch(ctx, jobs, batch, host.Hostname, batchCursor); err != nil {
				return "", classifyBatchErr(err)
			}
			batch = batch[:0]
			batchEntries = 0
			batchCursor = ""
		}
	}
	// ListRepos' iterator returns silently when ctx is cancelled; without
	// this check a cancellation landing between pages would fall through and
	// mark a half-enumerated host drained.
	if err := ctx.Err(); err != nil {
		e.releaseClaims(batch)
		return "", err
	}
	if batchEntries > 0 {
		if err := e.finishBatch(ctx, jobs, batch, host.Hostname, batchCursor); err != nil {
			return "", classifyBatchErr(err)
		}
	}
	return lastNonEmpty, nil
}

// classifyBatchErr separates retryable barrier outcomes from fatal Store
// failures. A cross-host claim that resolved without a terminal state means
// the owning host's attempt aborted; this host retries (re-lists, re-claims)
// rather than killing the Run.
func classifyBatchErr(err error) error {
	if errors.Is(err, errClaimNotTerminal) {
		return err
	}
	return &fatalStoreError{err}
}

func (e *Engine) reconcile(ctx context.Context, host string, client *atmossync.Client, entry atmossync.ListReposEntry) (repoJob, bool, error) {
	mu := &e.didMu[didShard(entry.DID)]
	mu.Lock()
	defer mu.Unlock()
	rec, err := e.opts.Store.Lookup(ctx, entry.DID)
	if err != nil {
		return repoJob{}, false, fmt.Errorf("backfill: store lookup %s: %w", entry.DID, err)
	}
	if rec.State == StateUnknown {
		if err := e.opts.Store.OnDiscover(ctx, host, entry); err != nil {
			return repoJob{}, false, fmt.Errorf("backfill: store on_discover %s: %w", entry.DID, err)
		}
	} else if rec.Active != entry.Active {
		if err := e.opts.Store.OnUpdate(ctx, host, entry); err != nil {
			return repoJob{}, false, fmt.Errorf("backfill: store on_update %s: %w", entry.DID, err)
		}
	}
	if e.opts.DiscoverOnly.ValOr(false) || !entry.Active || rec.State == StateComplete {
		return repoJob{}, false, nil
	}
	e.claimMu.Lock()
	defer e.claimMu.Unlock()
	if owner, claimed := e.claims[entry.DID]; claimed {
		// Another host's pipeline owns this DID (migration window). Dispatch
		// a wait-only job so this host's batch barrier blocks until the owner
		// reaches a terminal state — otherwise this host could save a cursor
		// covering a DID that never landed OnComplete/OnFail.
		return repoJob{host: host, entry: entry, wait: owner}, true, nil
	}
	claim := &didClaim{done: make(chan struct{})}
	e.claims[entry.DID] = claim
	return repoJob{host: host, client: client, entry: entry, claim: claim}, true, nil
}

func didShard(did atmos.DID) uint8 {
	var h uint32 = 2166136261
	for i := range len(did) {
		h ^= uint32(did[i])
		h *= 16777619
	}
	return uint8(h)
}

func (e *Engine) finishBatch(ctx context.Context, jobs chan<- repoJob, batch []repoJob, host, cursor string) error {
	if err := e.dispatchBatch(ctx, jobs, batch); err != nil {
		return err
	}
	if err := e.opts.Store.SaveHostCursor(ctx, host, cursor); err != nil {
		return fmt.Errorf("backfill: store save_host_cursor %s: %w", host, err)
	}
	return nil
}

func (e *Engine) dispatchBatch(ctx context.Context, jobs chan<- repoJob, batch []repoJob) error {
	done := make(chan error, len(batch))
	sent := 0
	var waits []*didClaim
	for i, job := range batch {
		if job.wait != nil {
			waits = append(waits, job.wait)
			continue
		}
		job.done = done
		select {
		case <-ctx.Done():
			e.releaseClaims(batch[i:])
			for range sent {
				<-done
			}
			return ctx.Err()
		case jobs <- job:
			sent++
		}
	}
	for range sent {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-done:
			if err != nil {
				return err
			}
		}
	}
	// Cross-host claims last: the cursor this barrier gates covers DIDs owned
	// by another host's pipeline, so it must not be saved until each of them
	// reached a terminal Store transition too.
	for _, claim := range waits {
		if err := e.awaitClaim(ctx, claim); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) workerLoop(ctx context.Context, jobs <-chan repoJob) {
	for job := range jobs {
		if ctx.Err() != nil {
			e.resolveClaim(job.entry.DID, job.claim, false)
			if job.done != nil {
				job.done <- ctx.Err()
			}
			continue
		}
		err := e.processRepo(ctx, job)
		e.resolveClaim(job.entry.DID, job.claim, err == nil)
		if job.done != nil {
			job.done <- err
		}
	}
}

// awaitClaim blocks until the owning host's pipeline resolves the claim.
// Only batch barriers (producer goroutines) wait here, never workers:
// owner jobs are always handed to their host's worker pool before the
// producer blocks, and workers never wait on foreign claims, so claim
// resolution cannot form a cross-host cycle.
func (e *Engine) awaitClaim(ctx context.Context, claim *didClaim) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-claim.done:
	}
	if !claim.terminal {
		return errClaimNotTerminal
	}
	return nil
}

// resolveClaim publishes the claim outcome and releases the DID for later
// sightings (a re-listed host attempt re-claims it via reconcile).
//
// It takes the DID's shard lock (same didMu → claimMu order as reconcile) so
// deletion cannot interleave with another producer's Lookup+claim sequence:
// without it, a producer could Lookup before the owner's OnComplete commits,
// observe the claim already deleted, re-claim, and run the Handler twice in
// one Run.
func (e *Engine) resolveClaim(did atmos.DID, claim *didClaim, terminal bool) {
	if claim == nil {
		return
	}
	mu := &e.didMu[didShard(did)]
	mu.Lock()
	defer mu.Unlock()
	claim.terminal = terminal
	close(claim.done)
	e.claimMu.Lock()
	if e.claims[did] == claim {
		delete(e.claims, did)
	}
	e.claimMu.Unlock()
}

// releaseClaims resolves the claims of never-dispatched owner jobs as
// non-terminal so cross-host waiters unblock (and retry) instead of hanging.
func (e *Engine) releaseClaims(jobs []repoJob) {
	for _, job := range jobs {
		e.resolveClaim(job.entry.DID, job.claim, false)
	}
}

func (e *Engine) processRepo(ctx context.Context, job repoJob) error {
	maxRetries := e.opts.MaxRetries.ValOr(DefaultMaxRetries)
	rlMaxAttempts := e.opts.RetryRateLimitMaxAttempts.ValOr(DefaultRetryRateLimitMaxAttempts)
	baseDelay := e.opts.RetryBaseDelay.ValOr(defaultRetryBaseDelay)
	maxDelay := e.opts.RetryMaxDelay.ValOr(defaultRetryMaxDelay)
	transientAttempt, rlAttempt, attempts := 0, 0, 0
	for {
		err := e.tryRepo(ctx, job)
		attempts++
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, errOnCompleteRecorded) {
			return err
		}
		requestHost := job.host
		if errors.Is(err, errDownloadTimeout) {
			return e.recordFail(ctx, job.entry.DID, requestHost, err, attempts)
		}
		var delay time.Duration
		if xrpc.IsRateLimited(err) {
			if rlAttempt >= rlMaxAttempts {
				return e.recordFail(ctx, job.entry.DID, requestHost, fmt.Errorf("backfill: still rate limited after %d attempts: %w", rlAttempt+1, err), attempts)
			}
			rlAttempt++
			delay = rateLimitDelay(err, baseDelay, rlAttempt)
		} else {
			if !xrpc.IsTransient(err) || transientAttempt >= maxRetries {
				return e.recordFail(ctx, job.entry.DID, requestHost, err, attempts)
			}
			delay = backoffDelay(baseDelay, maxDelay, transientAttempt)
			transientAttempt++
		}
		if err := e.sleep(ctx, delay); err != nil {
			return err
		}
	}
}

func (e *Engine) tryRepo(ctx context.Context, job repoJob) error {
	started := time.Now()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case e.globalSlots <- struct{}{}:
	}
	if cb := e.opts.OnDownloadSlotWait; cb.HasVal() {
		cb.Val()(time.Since(started))
	}
	rp, commit, responseHost, err := e.download(ctx, job.client, job.entry.DID)
	<-e.globalSlots
	if err != nil {
		return err
	}
	if err := e.opts.Handler.HandleRepo(ctx, job.entry.DID, rp, commit); err != nil {
		return err
	}
	// Store attribution is the validated roster hostname used for direct
	// enumeration/routing. xrpc independently attributes rate-limit state to
	// the post-redirect response host.
	_ = responseHost
	if err := e.opts.Store.OnComplete(ctx, job.entry.DID, job.host, commit); err != nil {
		if cb := e.opts.OnError; cb.HasVal() {
			cb.Val()(job.entry.DID, fmt.Errorf("backfill: store on_complete: %w", err))
		}
		return errOnCompleteRecorded
	}
	e.notifyComplete()
	return nil
}

func (e *Engine) download(ctx context.Context, client *atmossync.Client, did atmos.DID) (*atmosrepo.Repo, *atmosrepo.Commit, string, error) {
	dlCtx := ctx
	if timeout := e.downloadTimeout(); timeout > 0 {
		var cancel context.CancelFunc
		dlCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	translate := func(err error) error {
		if err == nil || ctx.Err() != nil {
			return err
		}
		if errors.Is(dlCtx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("%w: %w", errDownloadTimeout, err)
		}
		return err
	}
	body, host, err := client.GetRepoStreamHost(dlCtx, did, "")
	if err != nil {
		return nil, nil, "", translate(err)
	}
	defer func() { _ = body.Close() }()
	rp, commit, err := atmosrepo.LoadFromCAR(bufio.NewReader(body))
	if err != nil {
		return nil, nil, host, translate(err)
	}
	// The PDS is untrusted: a CAR whose commit identifies a different DID
	// must not be handled or recorded under the requested DID. (rp.DID is
	// parsed from the commit, so one comparison covers both.)
	if rp.DID != did {
		return nil, nil, host, fmt.Errorf("backfill: getRepo DID mismatch: requested %s, CAR commit is %s", did, rp.DID)
	}
	if err := rp.CheckComplete(); err != nil {
		return nil, nil, host, translate(err)
	}
	if e.opts.VerifyCommits.ValOr(false) {
		if err := atmossync.VerifyCommitWithDirectory(dlCtx, e.opts.Directory.ValOr(nil), commit); err != nil {
			return nil, nil, host, translate(err)
		}
	}
	return rp, commit, host, nil
}

func (e *Engine) recordFail(ctx context.Context, did atmos.DID, host string, cause error, attempts int) error {
	if cb := e.opts.OnError; cb.HasVal() {
		cb.Val()(did, cause)
	}
	if err := e.opts.Store.OnFail(ctx, did, host, cause, attempts); err != nil {
		if cb := e.opts.OnError; cb.HasVal() {
			cb.Val()(did, fmt.Errorf("backfill: store on_fail: %w", err))
		}
		return fmt.Errorf("backfill: store on_fail %s: %w", did, err)
	}
	e.failed.Add(1)
	e.notifyProgress()
	return nil
}

func rateLimitDelay(err error, baseDelay time.Duration, attempt int) time.Duration {
	if reset := xrpc.RetryAfter(err); !reset.IsZero() {
		if wait := time.Until(reset); wait > 0 {
			return min(wait, retryRateLimitCeiling)
		}
	}
	return max(baseDelay, backoffDelay(baseDelay, retryRateLimitCeiling, attempt-1))
}

func backoffDelay(base, maxDelay time.Duration, attempt int) time.Duration {
	// A zero base or ceiling means "no delay", not "maximum delay" —
	// validation permits zero to disable backoff entirely.
	if base <= 0 || maxDelay <= 0 {
		return 0
	}
	delay := maxDelay
	if attempt < bits.LeadingZeros64(uint64(base)) {
		if shifted := base << attempt; shifted < maxDelay {
			delay = shifted
		}
	}
	if half := int64(delay) / 2; half > 0 {
		delay += time.Duration(rand.Int64N(half))
	}
	return min(delay, maxDelay)
}

func (e *Engine) sleep(ctx context.Context, delay time.Duration) error {
	if e.retrySleeper != nil {
		return e.retrySleeper.Sleep(ctx, delay)
	}
	return timerRetrySleeper{}.Sleep(ctx, delay)
}

func (e *Engine) notifyComplete() {
	if !e.opts.OnProgress.HasVal() {
		e.completed.Add(1)
		return
	}
	e.progressMu.Lock()
	e.completed.Add(1)
	e.opts.OnProgress.Val()(e.stats())
	e.progressMu.Unlock()
}

func (e *Engine) notifyProgress() {
	if !e.opts.OnProgress.HasVal() {
		return
	}
	e.progressMu.Lock()
	e.opts.OnProgress.Val()(e.stats())
	e.progressMu.Unlock()
}

func (e *Engine) notifyHostState(host HostInfo, state HostState, attempts int, err error) {
	if cb := e.opts.OnHostState; cb.HasVal() {
		cb.Val()(host, state, attempts, err)
	}
	e.notifyProgress()
}

func (e *Engine) notifyEntryError(err error) {
	if cb := e.opts.OnEntryError; cb.HasVal() {
		cb.Val()(err)
	}
}

func (e *Engine) stats() Stats {
	return Stats{Completed: e.completed.Load(), Failed: e.failed.Load(), ReposEnumerated: e.enumerated.Load(), ActiveHosts: e.activeHosts.Load(), HostsDrained: e.hostsDrained.Load(), HostsExhausted: e.hostsExhausted.Load()}
}

func (e *Engine) globalDownloadCount() int {
	return e.opts.GlobalDownloads.ValOr(defaultGlobalDownloads)
}
func (e *Engine) hostWorkerLimit() int {
	return e.opts.HostWorkers.ValOr(defaultHostWorkers)
}
func (e *Engine) maxActiveHosts() int  { return e.opts.MaxActiveHosts.ValOr(defaultMaxActiveHosts) }
func (e *Engine) maxHosts() int        { return e.opts.MaxHosts.ValOr(defaultMaxHosts) }
func (e *Engine) hostMaxAttempts() int { return e.opts.HostMaxAttempts.ValOr(defaultHostMaxAttempts) }
func (e *Engine) hostBackoffBase() time.Duration {
	return e.opts.HostBackoffBase.ValOr(defaultHostBackoffBase)
}
func (e *Engine) hostBackoffMax() time.Duration {
	return e.opts.HostBackoffMax.ValOr(defaultHostBackoffMax)
}
func (e *Engine) batchSize() int { return e.opts.BatchSize.ValOr(defaultBatchSize) }
func (e *Engine) downloadTimeout() time.Duration {
	return e.opts.DownloadTimeout.ValOr(DefaultDownloadTimeout)
}

func (e *Engine) workerCount(relayAccounts int64) int {
	workers := max(1, int(relayAccounts/10_000))
	return min(workers, e.hostWorkerLimit())
}
