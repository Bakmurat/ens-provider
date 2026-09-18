package main

// Active-passive high availability (v0.1.25, design review).
//
// Two replicas run the full binary; exactly ONE holds the Lease
// `ens-provider-leader` and serves. The follower is kubelet-READY (kubelet
// readiness stays Kubernetes-reachability only — a permanently-NotReady
// follower would wedge the Deployment, helm wait, and the Flux HelmRelease,
// C-14) but is excluded from CA routing three ways:
//
//   1. ROUTING — the Service selects `ens-provider.io/role: leader`, a pod
//      label only the leader carries (published on acquisition, purged on
//      startup, dropped by pod death).
//   2. ADMISSION — a fail-closed interceptor rejects every non-health RPC
//      with Unavailable unless this pod is the serving leader (C-10); the
//      handler context is merged with the leadership-term context so
//      demotion also cancels IN-FLIGHT work (C-03).
//   3. TRANSPORT — real demotion ends in os.Exit(1): CA holds ONE long-lived
//      HTTP/2 connection that per-RPC errors never close (C-05); process
//      exit closes it, forcing CA to reconnect to the labeled leader.
//
// Lease handling (C-13/C-15/C-18-impl/C-19-impl):
//   - ReleaseOnCancel is NEVER used: client-go runs its release BEFORE
//     OnStoppedLeading with its own RenewDeadline budget, widening the
//     dual-active window on unexpected loss.
//   - Graceful shutdown drains WHILE RETAINING the Lease (renewals continue),
//     then cancels the election, JOINS Run() and the acquisition callback,
//     and only then — and only if full quiescence was PROVEN — clears
//     spec.holderIdentity owner-checked for a ~2s handoff. Any doubt skips
//     the release: natural ~15s expiry is the fencing interval. "Fast
//     handoff is a reward for a clean drain, never a default."
//   - The whole OnStartedLeading callback is a tracked term (client-go
//     launches it in a goroutine Run() never joins): the term handle is
//     installed atomically under the state lock, and a callback that starts
//     after shutdown was recorded performs no publication and no work.

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	// LeaseName is a CONSTANT shared with the chart RBAC resourceNames — a
	// knob here and a hard-coded Role there could silently diverge (C-08).
	LeaseName = "ens-provider-leader"
	// RoleLabelKey/Value: the Service selector's leader half. Only the pod
	// holding the Lease carries it.
	RoleLabelKey   = "ens-provider.io/role"
	RoleLabelValue = "leader"
	// healthServicePrefix is the ONLY interceptor exemption (C-10): kubelet
	// must read readiness from a follower, and gRPC-aware clients may watch.
	healthServicePrefix = "/grpc.health.v1.Health/"
	// leaderHealthService is a NAMED health service reflecting leadership for
	// observability only — kubelet probes the default "" service, which stays
	// Kubernetes-reachability-only on both pods (C-14).
	leaderHealthService = "leader"
)

// electionRunner is the slice of *leaderelection.LeaderElector we consume —
// an interface so shutdown-path tests can script acquisition/loss.
type electionRunner interface {
	Run(ctx context.Context)
	IsLeader() bool
}

// podLabeler is the routing-publication surface (implemented by Kube).
type podLabeler interface {
	SetPodLabel(ctx context.Context, namespace, pod, key, value string) error
	RemovePodLabel(ctx context.Context, namespace, pod, key string) error
}

// leaseClearer clears spec.holderIdentity owner-checked (implemented by Kube).
type leaseClearer interface {
	ClearLeaseHolder(ctx context.Context, namespace, name, identity string) error
}

// leaderTerm tracks ONE acquisition callback from its first instruction to
// its return (C-19-impl). done is closed by endTerm; workers (the background
// loop) hang off wg.
type leaderTerm struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	wg     sync.WaitGroup
}

// leadership owns the serving state, the interceptors, the term lifecycle and
// the shutdown orchestration. With cfg.LeaderElect=false it degrades to a
// pinned-leader pass-through preserving v0.1.24 operational semantics (C-12).
type leadership struct {
	cfg    *Config
	labels podLabeler
	lease  leaseClearer
	health statusSetter
	logf   func(string, ...any)
	exit   func(int) // os.Exit, injected for tests

	// election plumbing (nil / unused when LeaderElect=false)
	elector        electionRunner
	electionCancel context.CancelFunc
	runDone        chan struct{} // closed when elector.Run returns
	cbDone         chan struct{} // closed when the OnStartedLeading callback returns

	// background is the leadership-scoped work (reconciler+janitor loop),
	// supplied by main; it must return when its context is cancelled.
	background func(context.Context)

	mu           sync.Mutex
	leader       bool // serving-leadership: term active + published
	k8sOK        bool
	shuttingDown bool
	everLed      bool
	term         *leaderTerm

	// labelRetries bounds acquisition-time publication attempts (C-16); on
	// exhaustion the leader relinquishes instead of squatting unroutable.
	labelRetries  int
	labelRetryGap time.Duration
}

func newLeadership(cfg *Config, labels podLabeler, lease leaseClearer, health statusSetter, background func(context.Context)) *leadership {
	l := &leadership{
		cfg:           cfg,
		labels:        labels,
		lease:         lease,
		health:        health,
		logf:          log.Printf,
		exit:          os.Exit,
		background:    background,
		runDone:       make(chan struct{}),
		cbDone:        make(chan struct{}),
		labelRetries:  10,
		labelRetryGap: 2 * time.Second,
	}
	if !cfg.LeaderElect {
		// pinned leader: every RPC admissible, background runs unconditionally
		// from main exactly as v0.1.24 — no Lease client, no label writes.
		l.leader = true
		l.everLed = true
		metrics.isLeader.Store(1)
		metrics.routingPublished.Store(1)
	}
	// make the named observability service exist from t0 (NOT_SERVING until
	// leadership AND k8s reachability hold; kubelet never probes it).
	l.mu.Lock()
	l.publishLocked()
	l.mu.Unlock()
	return l
}

// ---- serving state ---------------------------------------------------------

// serving is the ONE atomic condition (C-17.2): admission requires it.
func (l *leadership) servingLocked() bool {
	return l.leader && l.k8sOK && !l.shuttingDown
}

// SetK8sOK is fed by the readiness tee (readiness.go semantics untouched —
// it still writes the kubelet "" service itself; this copy only feeds the
// leadership plane and the named observability service).
func (l *leadership) SetK8sOK(ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.k8sOK == ok {
		return
	}
	l.k8sOK = ok
	l.publishLocked()
}

// publishLocked mirrors the serving state onto the NAMED health service.
func (l *leadership) publishLocked() {
	if l.health == nil {
		return
	}
	st := healthpb.HealthCheckResponse_NOT_SERVING
	if l.servingLocked() {
		st = healthpb.HealthCheckResponse_SERVING
	}
	l.health.SetServingStatus(leaderHealthService, st)
}

// AllowRPC is the admission check for surfaces that need no term capture
// (the stream interceptor and tests).
func (l *leadership) AllowRPC() bool {
	_, ok := l.admit()
	return ok
}

// admit atomically checks the serving state AND captures the term context
// under ONE lock acquisition (C-21-impl: a check-then-capture split let an
// RPC pass admission, lose leadership, observe term==nil, and run a real
// handler with an uncancellable context — reopening the C-03 hole). The
// invariant while locked: leader==true ⟹ term!=nil. A captured term whose
// context is already cancelled is rejected, not invoked.
func (l *leadership) admit() (context.Context, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.cfg.LeaderElect {
		// v0.1.24 semantics: no leadership gate; shutdown still rejects late
		// RPCs (GracefulStop refuses new work anyway).
		return nil, !l.shuttingDown
	}
	if !l.servingLocked() {
		return nil, false
	}
	if l.term == nil || l.term.ctx.Err() != nil {
		return nil, false
	}
	return l.term.ctx, true
}

// termContext exposes the current term context (observability/tests only —
// admission uses admit()).
func (l *leadership) termContext() context.Context {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.term == nil {
		return nil
	}
	return l.term.ctx
}

// ---- interceptors (C-03/C-10, fail closed) ---------------------------------

// UnaryInterceptor rejects every non-health RPC unless this pod is the
// serving leader, and merges the term context into the handler context so
// demotion cancels in-flight work. Exempt list, not a gate list: any future
// registered service is leadership-gated by default.
func (l *leadership) UnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if isHealthMethod(info.FullMethod) {
		return handler(ctx, req)
	}
	term, ok := l.admit() // atomic check-and-capture (C-21-impl)
	if !ok {
		return nil, status.Errorf(codes.Unavailable, "not leader (%s)", info.FullMethod)
	}
	if term != nil {
		mctx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(term, cancel)
		defer stop()
		defer cancel()
		return handler(mctx, req)
	}
	return handler(ctx, req)
}

// StreamInterceptor: no streaming RPC exists today (protos: Streams=[]), but
// a future one must not bypass the gate (C-10).
func (l *leadership) StreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if isHealthMethod(info.FullMethod) {
		return handler(srv, ss)
	}
	if !l.AllowRPC() {
		return status.Errorf(codes.Unavailable, "not leader (%s)", info.FullMethod)
	}
	return handler(srv, ss)
}

func isHealthMethod(fullMethod string) bool {
	return len(fullMethod) >= len(healthServicePrefix) && fullMethod[:len(healthServicePrefix)] == healthServicePrefix
}

// ---- startup (C-16 state machine, step 2) ----------------------------------

// PurgeStaleLabel removes this pod's leader label if a previous container
// incarnation left it behind (os.Exit restarts the CONTAINER in the SAME pod,
// so the label survives a demotion exit). MUST complete successfully before
// the gRPC listener opens and before election starts (C-16): a stale-labeled
// Ready follower would re-enter the Service.
func (l *leadership) PurgeStaleLabel(ctx context.Context) error {
	if !l.cfg.LeaderElect {
		return nil
	}
	var lastErr error
	for attempt := 1; attempt <= l.labelRetries; attempt++ {
		err := l.labels.RemovePodLabel(ctx, l.cfg.PodNamespace, l.cfg.PodName, RoleLabelKey)
		if err == nil {
			return nil
		}
		if isPermanentAPIError(err) {
			// C-24-impl: Forbidden/Unauthorized/definite request errors are a
			// misconfiguration — fail-closed IMMEDIATELY, don't retry 20s.
			return fmt.Errorf("startup leader-label purge failed permanently (fix RBAC/config): %w", err)
		}
		lastErr = err
		l.logf("[leader] startup label purge attempt %d/%d failed: %v", attempt, l.labelRetries, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(l.labelRetryGap):
		}
	}
	return fmt.Errorf("startup leader-label purge failed after %d attempts: %w", l.labelRetries, lastErr)
}

// ---- election wiring -------------------------------------------------------

// StartElection builds the elector and runs it on its own goroutine with a
// DEDICATED context (never the process signal context — C-01). No-op when
// election is disabled.
func (l *leadership) StartElection(client kubernetes.Interface) error {
	if !l.cfg.LeaderElect {
		close(l.runDone)
		close(l.cbDone)
		return nil
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Namespace: l.cfg.PodNamespace, Name: LeaseName},
		Client:    client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: l.cfg.PodName,
			// no EventRecorder: no Events RBAC is granted (C-08)
		},
	}
	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: l.cfg.LeaderLeaseDuration,
		RenewDeadline: l.cfg.LeaderRenewDeadline,
		RetryPeriod:   l.cfg.LeaderRetryPeriod,
		// ReleaseOnCancel=false ALWAYS (C-13): client-go's release runs before
		// OnStoppedLeading with a background ctx up to RenewDeadline, keeping
		// the old leader serving past follower acquisition on unexpected loss.
		ReleaseOnCancel: false,
		Name:            LeaseName,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: l.onStartedLeading,
			OnStoppedLeading: l.onStoppedLeading,
			OnNewLeader: func(identity string) {
				l.logf("[leader] observed leader: %s", identity)
			},
		},
	})
	if err != nil {
		return fmt.Errorf("leader elector: %w", err)
	}
	l.elector = le
	ctx, cancel := context.WithCancel(context.Background())
	l.electionCancel = cancel
	go func() {
		defer close(l.runDone)
		le.Run(ctx)
	}()
	l.logf("[leader] election started: lease %s/%s identity=%s (%s/%s/%s)",
		l.cfg.PodNamespace, LeaseName, l.cfg.PodName,
		l.cfg.LeaderLeaseDuration, l.cfg.LeaderRenewDeadline, l.cfg.LeaderRetryPeriod)
	return nil
}

// beginTerm atomically installs the term handle BEFORE any leader state is
// published (C-19-impl). Returns nil — and publishes nothing — if shutdown
// was already recorded.
func (l *leadership) beginTerm(parent context.Context) *leaderTerm {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.shuttingDown {
		l.logf("[leader] acquisition callback after shutdown was recorded - refusing the term")
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	t := &leaderTerm{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	l.term = t
	l.everLed = true
	l.leader = true
	metrics.isLeader.Store(1)
	metrics.leadershipTransitions.Add(1)
	l.publishLocked()
	l.logf("[leader] term started (identity=%s)", l.cfg.PodName)
	return t
}

func (l *leadership) endTerm(t *leaderTerm) {
	// cancel FIRST (C-21-impl): an RPC admitted a moment ago holds t.ctx —
	// cancelling before the state flips guarantees its merged handler
	// context dies no later than leadership does.
	t.cancel()
	l.mu.Lock()
	if l.term == t {
		l.term = nil
	}
	l.leader = false
	metrics.isLeader.Store(0)
	metrics.routingPublished.Store(0)
	l.publishLocked()
	l.mu.Unlock()
	close(t.done)
}

// onStartedLeading is the ENTIRE tracked leadership term (C-19-impl): label
// publication (C-16 disposition), forced fresh inventory (honest scope: it
// only removes local cache staleness, C-04), then the background loop until
// the term ends.
func (l *leadership) onStartedLeading(cbCtx context.Context) {
	defer close(l.cbDone) // at most one acquisition per process (demotion = exit)
	term := l.beginTerm(cbCtx)
	if term == nil {
		return
	}
	defer l.endTerm(term)

	if !l.publishRouting(term) {
		return
	}
	if l.background != nil {
		term.wg.Add(1)
		go func() {
			defer term.wg.Done()
			l.background(term.ctx)
		}()
		<-term.ctx.Done()
		term.wg.Wait()
	}
}

// publishRouting adds the leader label with bounded retries. On exhaustion
// the leader RELINQUISHES (C-16): endpoints stayed empty the whole time, so
// squatting on the Lease unroutable helps nobody — treat it as demotion so
// the peer can take over after expiry.
func (l *leadership) publishRouting(term *leaderTerm) bool {
	for attempt := 1; attempt <= l.labelRetries; attempt++ {
		err := l.labels.SetPodLabel(term.ctx, l.cfg.PodNamespace, l.cfg.PodName, RoleLabelKey, RoleLabelValue)
		if err == nil {
			metrics.routingPublished.Store(1)
			l.logf("[leader] routing published: %s=%s on pod %s", RoleLabelKey, RoleLabelValue, l.cfg.PodName)
			return true
		}
		if isPermanentAPIError(err) {
			// C-24-impl: retrying a Forbidden cannot make the leader routable —
			// relinquish immediately so the peer can take over after expiry.
			l.logf("[leader] routing publication failed PERMANENTLY (%v) - relinquishing leadership (exit)", err)
			go l.demote(term)
			return false
		}
		l.logf("[leader] routing publication attempt %d/%d failed (leader but UNROUTABLE): %v", attempt, l.labelRetries, err)
		select {
		case <-term.ctx.Done():
			return false // demoted or shutting down mid-retry: nothing to relinquish here
		case <-time.After(l.labelRetryGap):
		}
	}
	l.logf("[leader] routing publication EXHAUSTED after %d attempts - relinquishing leadership (exit; lease expires naturally)", l.labelRetries)
	go l.demote(term) // demote cancels the term; onStartedLeading unwinds via endTerm
	return false
}

// ---- demotion & shutdown (F-04 v6) -----------------------------------------

// onStoppedLeading fires on EVERY Run exit, including never-leaders (C-02).
// Only an unexpected loss by a process that actually led takes the demotion
// exit path.
func (l *leadership) onStoppedLeading() {
	l.mu.Lock()
	shutting := l.shuttingDown
	everLed := l.everLed
	term := l.term
	l.mu.Unlock()
	if shutting {
		l.logf("[leader] election stopped (normal shutdown)")
		return
	}
	if !everLed {
		l.logf("[leader] election stopped before ever leading")
		return
	}
	l.logf("[leader] LEADERSHIP LOST unexpectedly - demoting (exit; once per acquired term)")
	l.demote(term)
}

// demote: reject new RPCs immediately, cancel in-flight term work, bounded
// drain, exit(1). The Lease is NOT touched — with ReleaseOnCancel=false it
// simply expires (the follower's fencing interval). Transport closure by
// process death is what breaks CA's sticky connection (C-05).
func (l *leadership) demote(term *leaderTerm) {
	l.mu.Lock()
	if l.shuttingDown {
		l.mu.Unlock()
		return // a normal shutdown already owns the lifecycle
	}
	l.shuttingDown = true
	l.leader = false
	metrics.isLeader.Store(0)
	l.publishLocked()
	l.mu.Unlock()
	if term != nil {
		term.cancel()
		waitChan(term.done, 5*time.Second)
	}
	if l.electionCancel != nil {
		l.electionCancel()
	}
	l.exit(1)
}

// Shutdown is the normal-signal path (F-04.1/F-04.2). It drains while
// RETAINING the Lease (renewals continue), joins the election goroutine and
// the acquisition callback, and clears the Lease only on PROVEN quiescence.
//
// ONE deadline governs the whole orchestration (C-22-impl): every phase gets
// only the REMAINING time, so total shutdown stays inside `budget` (+ the
// small release write) and well under the pod's 30s default
// terminationGracePeriodSeconds — a per-phase budget could stack past 40s
// and end in kubelet SIGKILL mid-orchestration.
//
// stopGRPC(remaining) must block until in-flight handlers finish
// (GracefulStop) and return false if it had to give up (hard Stop at its
// deadline).
func (l *leadership) Shutdown(stopGRPC func(time.Duration) bool, budget time.Duration) {
	l.mu.Lock()
	if l.shuttingDown {
		l.mu.Unlock()
		return
	}
	l.shuttingDown = true
	term := l.term
	l.publishLocked()
	l.mu.Unlock()

	deadline := time.Now().Add(budget)
	remaining := func() time.Duration { return time.Until(deadline) }

	wasLeader := l.cfg.LeaderElect && l.elector != nil && l.elector.IsLeader()

	// pure follower: cancel the election FIRST so a terminating candidate
	// cannot acquire during its drain (C-13). A leader keeps renewing so the
	// Lease is RETAINED through the drain (C-01: the follower must not serve
	// while our in-flight work still runs).
	if !wasLeader && l.electionCancel != nil {
		l.electionCancel()
	}

	quiesced := true
	if term != nil {
		l.removeLabelBestEffort(remaining())
		term.cancel()
		if !waitChan(term.done, remaining()) {
			l.logf("[leader] term did not quiesce in time - fast release DISABLED")
			quiesced = false
		}
	}
	if !stopGRPC(remaining()) {
		l.logf("[leader] gRPC drain timed out - fast release DISABLED")
		quiesced = false
	}

	// stop the election and JOIN it (C-15: the release below must never race
	// our own renew goroutine).
	if l.electionCancel != nil {
		l.electionCancel()
	}
	if !waitChan(l.runDone, remaining()) {
		quiesced = false
	}
	// join the acquisition callback if one was (or will be) launched: after
	// acquire, client-go has already go'ed the callback even if it hasn't run
	// its first instruction yet (C-19-impl race a). A post-shutdown callback
	// refuses its term and closes cbDone immediately.
	if wasLeader {
		if !waitChan(l.cbDone, remaining()) {
			quiesced = false
		}
	}

	if wasLeader && quiesced && l.lease != nil {
		// the release write gets a short bounded window of its own — it is
		// the one step allowed to run right AT the deadline (≤3s), still far
		// inside the pod grace period.
		rctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := l.lease.ClearLeaseHolder(rctx, l.cfg.PodNamespace, LeaseName, l.cfg.PodName); err != nil {
			l.logf("[leader] owner-checked lease release skipped/failed (falling back to expiry): %v", err)
		} else {
			l.logf("[leader] lease released after clean drain - fast handoff")
		}
	} else if wasLeader {
		l.logf("[leader] quiescence NOT proven - lease left to expire (~%s fencing)", l.cfg.LeaderLeaseDuration)
	}
}

func (l *leadership) removeLabelBestEffort(limit time.Duration) {
	if !l.cfg.LeaderElect {
		return
	}
	if limit > 3*time.Second {
		limit = 3 * time.Second
	}
	if limit <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	if err := l.labels.RemovePodLabel(ctx, l.cfg.PodNamespace, l.cfg.PodName, RoleLabelKey); err != nil {
		l.logf("[leader] best-effort label removal failed (pod death will drop the endpoint): %v", err)
	}
	metrics.routingPublished.Store(0)
}

// waitChan waits up to d (an exhausted deadline still gives a non-blocking
// look — a closed channel must count as success even at d<=0).
func waitChan(ch <-chan struct{}, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
	select {
	case <-ch:
		return true
	case <-time.After(d):
		return false
	}
}

// isPermanentAPIError classifies Kubernetes API errors that retrying cannot
// fix (C-24-impl): authorization and definite-request failures fail
// IMMEDIATELY instead of burning the whole retry budget.
func isPermanentAPIError(err error) bool {
	return apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) ||
		apierrors.IsInvalid(err) || apierrors.IsBadRequest(err)
}
