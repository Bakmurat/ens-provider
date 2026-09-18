package main

// Active-passive HA tests (v0.1.25, design review F-09). Deterministic where
// the design is deterministic (state machine, interceptors, shutdown
// ordering, quiescence gate); one bounded-timing integration smoke against a
// REAL LeaderElector on the fake clientset (acquire → shutdown → owner-
// checked release → no renewal ever follows the release, C-15).

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"

	pb "ens-provider/protos"
)

// ---- fakes -----------------------------------------------------------------

type fakeLabeler struct {
	mu       sync.Mutex
	setErrs  []error // consumed per call; nil = success; empty slice = always success
	setCalls int
	removed  int
	remErr   error
	blockSet chan struct{} // non-nil: SetPodLabel blocks until closed, IGNORING ctx
}

func (f *fakeLabeler) SetPodLabel(ctx context.Context, ns, pod, key, value string) error {
	if f.blockSet != nil {
		<-f.blockSet // deliberately ctx-deaf: models a stuck uncancellable call
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setCalls++
	if len(f.setErrs) > 0 {
		err := f.setErrs[0]
		f.setErrs = f.setErrs[1:]
		return err
	}
	return nil
}

func (f *fakeLabeler) RemovePodLabel(ctx context.Context, ns, pod, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed++
	return f.remErr
}

func (f *fakeLabeler) sets() int    { f.mu.Lock(); defer f.mu.Unlock(); return f.setCalls }
func (f *fakeLabeler) removes() int { f.mu.Lock(); defer f.mu.Unlock(); return f.removed }

type fakeClearer struct {
	mu      sync.Mutex
	calls   int
	err     error
	precond func() error // asserted before recording (ordering checks)
}

func (f *fakeClearer) ClearLeaseHolder(ctx context.Context, ns, name, identity string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.precond != nil {
		if err := f.precond(); err != nil {
			return fmt.Errorf("ordering violation: %w", err)
		}
	}
	f.calls++
	return f.err
}

func (f *fakeClearer) count() int { f.mu.Lock(); defer f.mu.Unlock(); return f.calls }

type fakeElector struct{ leader atomic.Bool }

func (f *fakeElector) Run(ctx context.Context) { <-ctx.Done() }
func (f *fakeElector) IsLeader() bool          { return f.leader.Load() }

// recordingSetter records named-health-service transitions.
type recordingSetter struct {
	mu     sync.Mutex
	states map[string]healthpb.HealthCheckResponse_ServingStatus
}

func (r *recordingSetter) SetServingStatus(service string, st healthpb.HealthCheckResponse_ServingStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.states == nil {
		r.states = map[string]healthpb.HealthCheckResponse_ServingStatus{}
	}
	r.states[service] = st
}

func (r *recordingSetter) get(service string) healthpb.HealthCheckResponse_ServingStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.states[service]
}

func haConfig(t *testing.T) *Config {
	t.Helper()
	setEnv(t, map[string]string{
		"LEADER_ELECT": "true", "POD_NAME": "ens-provider-test-0", "POD_NAMESPACE": "kube-system",
	})
	cfg := loadTestConfig(t)
	if !cfg.LeaderElect {
		t.Fatal("LEADER_ELECT not parsed")
	}
	return cfg
}

// newTestLeadership wires a leadership with fakes and FAST retry timings.
// exitCode records the exit; the exit func must never be os.Exit in tests.
func newTestLeadership(t *testing.T, cfg *Config, labels podLabeler, clear leaseClearer, background func(context.Context)) (*leadership, *atomic.Int64) {
	t.Helper()
	l := newLeadership(cfg, labels, clear, &recordingSetter{}, background)
	l.logf = t.Logf
	l.labelRetryGap = time.Millisecond
	exitCode := &atomic.Int64{}
	exitCode.Store(-1)
	l.exit = func(code int) { exitCode.Store(int64(code)) }
	return l, exitCode
}

// installFakeElection wires a fake elector + a joinable fake Run goroutine so
// shutdown paths are deterministic without client-go timing.
func installFakeElection(l *leadership, isLeader bool) *fakeElector {
	fe := &fakeElector{}
	fe.leader.Store(isLeader)
	l.elector = fe
	runCtx, cancel := context.WithCancel(context.Background())
	l.electionCancel = cancel
	go func() {
		fe.Run(runCtx)
		close(l.runDone)
	}()
	return fe
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// ---- interceptors (F-09.1, fail closed, C-10/C-11) -------------------------

func TestInterceptorFailClosed(t *testing.T) {
	cfg := haConfig(t)
	l, _ := newTestLeadership(t, cfg, &fakeLabeler{}, &fakeClearer{}, nil)

	called := false
	handler := func(ctx context.Context, req any) (any, error) { called = true; return "ok", nil }

	// follower: every non-health method rejected — including the REAL Refresh
	// method (the hidden handler-level writer, C-11) and an unknown future
	// service (fail-closed, C-10).
	for _, m := range []string{
		pb.CloudProvider_Refresh_FullMethodName,
		pb.CloudProvider_NodeGroupIncreaseSize_FullMethodName,
		pb.CloudProvider_NodeGroupDeleteNodes_FullMethodName,
		"/some.future.Service/Do",
	} {
		called = false
		_, err := l.UnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: m}, handler)
		if status.Code(err) != codes.Unavailable || called {
			t.Errorf("follower %s: want Unavailable+not-called, got err=%v called=%v", m, err, called)
		}
	}
	// health service passes on a follower (kubelet must read NOT_SERVING).
	for _, m := range []string{"/grpc.health.v1.Health/Check", "/grpc.health.v1.Health/Watch"} {
		called = false
		if _, err := l.UnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: m}, handler); err != nil || !called {
			t.Errorf("follower %s: health must pass, got err=%v called=%v", m, err, called)
		}
	}
	// stream interceptor gates identically.
	if err := l.StreamInterceptor(nil, nil, &grpc.StreamServerInfo{FullMethod: "/some.future.Service/Stream"}, func(any, grpc.ServerStream) error { return nil }); status.Code(err) != codes.Unavailable {
		t.Errorf("stream follower: want Unavailable, got %v", err)
	}

	// leader + k8sOK: passes.
	term := l.beginTerm(context.Background())
	l.SetK8sOK(true)
	called = false
	if _, err := l.UnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: pb.CloudProvider_Refresh_FullMethodName}, handler); err != nil || !called {
		t.Errorf("leader: want pass, got err=%v called=%v", err, called)
	}
	// leader WITHOUT k8sOK: rejected (the one atomic serving state).
	l.SetK8sOK(false)
	if _, err := l.UnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: pb.CloudProvider_Refresh_FullMethodName}, handler); status.Code(err) != codes.Unavailable {
		t.Errorf("leader without k8s: want Unavailable, got %v", err)
	}
	l.endTerm(term)
}

// F-09.1/C-03: the handler context is merged with the term context — demotion
// cancels in-flight work.
func TestInterceptorTermContextMerge(t *testing.T) {
	cfg := haConfig(t)
	l, _ := newTestLeadership(t, cfg, &fakeLabeler{}, &fakeClearer{}, nil)
	term := l.beginTerm(context.Background())
	l.SetK8sOK(true)

	entered := make(chan struct{})
	finished := make(chan error, 1)
	handler := func(ctx context.Context, req any) (any, error) {
		close(entered)
		<-ctx.Done() // in-flight work waiting on cancellation
		return nil, ctx.Err()
	}
	go func() {
		_, err := l.UnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: pb.CloudProvider_NodeGroupIncreaseSize_FullMethodName}, handler)
		finished <- err
	}()
	<-entered
	term.cancel() // demotion
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("handler not cancelled by term cancellation")
	}
	l.endTerm(term)
}

// ---- serving state matrix (F-09.2) -----------------------------------------

func TestServingStateMatrix(t *testing.T) {
	cfg := haConfig(t)
	rec := &recordingSetter{}
	l := newLeadership(cfg, &fakeLabeler{}, &fakeClearer{}, rec, nil)
	l.logf = t.Logf

	for _, tc := range []struct {
		leader, k8s, shutdown bool
		want                  healthpb.HealthCheckResponse_ServingStatus
	}{
		{false, false, false, healthpb.HealthCheckResponse_NOT_SERVING},
		{false, false, true, healthpb.HealthCheckResponse_NOT_SERVING},
		{false, true, false, healthpb.HealthCheckResponse_NOT_SERVING},
		{false, true, true, healthpb.HealthCheckResponse_NOT_SERVING},
		{true, false, false, healthpb.HealthCheckResponse_NOT_SERVING},
		{true, false, true, healthpb.HealthCheckResponse_NOT_SERVING},
		{true, true, true, healthpb.HealthCheckResponse_NOT_SERVING},
		{true, true, false, healthpb.HealthCheckResponse_SERVING}, // the ONLY serving cell
	} {
		l.mu.Lock()
		l.leader, l.k8sOK, l.shuttingDown = tc.leader, tc.k8s, tc.shutdown
		// honor the admit() invariant (C-21-impl): leader==true ⟹ term!=nil.
		if tc.leader {
			tctx, tcancel := context.WithCancel(context.Background())
			defer tcancel()
			l.term = &leaderTerm{ctx: tctx, cancel: tcancel, done: make(chan struct{})}
		} else {
			l.term = nil
		}
		l.publishLocked()
		l.mu.Unlock()
		if got := rec.get(leaderHealthService); got != tc.want {
			t.Errorf("leader=%v k8s=%v shutdown=%v: want %v got %v", tc.leader, tc.k8s, tc.shutdown, tc.want, got)
		}
		// AllowRPC must agree with the published state.
		if got := l.AllowRPC(); got != (tc.want == healthpb.HealthCheckResponse_SERVING) {
			t.Errorf("AllowRPC disagrees with matrix at leader=%v k8s=%v shutdown=%v", tc.leader, tc.k8s, tc.shutdown)
		}
	}
	// kubelet's "" service is NEVER written by the leadership plane (C-14).
	if _, ok := rec.states[""]; ok {
		t.Error("leadership plane wrote the kubelet \"\" health service")
	}
}

// ---- shutdown paths (F-09.3, C-01/C-02/C-13/C-15/C-17-impl) ----------------

// leader-graceful: reject → label-remove → term-ctx cancel → drain →
// election-ctx cancel → Run() awaited → owner-checked release → no exit(1).
func TestShutdownLeaderGracefulOrdering(t *testing.T) {
	cfg := haConfig(t)
	labels := &fakeLabeler{}
	clear := &fakeClearer{}
	bgStarted := make(chan struct{})
	l, exitCode := newTestLeadership(t, cfg, labels, clear, func(ctx context.Context) {
		close(bgStarted)
		<-ctx.Done() // well-behaved background work
	})
	installFakeElection(l, true)
	// the release must observe: election goroutine joined (C-15) AND the term
	// fully quiesced (C-18-impl).
	clear.precond = func() error {
		select {
		case <-l.runDone:
		default:
			return errors.New("release before election goroutine joined")
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.term != nil {
			return errors.New("release while term still active")
		}
		return nil
	}

	go l.onStartedLeading(context.Background())
	<-bgStarted
	waitFor(t, "routing published", func() bool { return labels.sets() == 1 })

	grpcStopped := false
	l.Shutdown(func(time.Duration) bool { grpcStopped = true; return true }, time.Second)

	if !grpcStopped {
		t.Error("gRPC drain not invoked")
	}
	if labels.removes() < 1 {
		t.Error("leader label not removed on graceful shutdown")
	}
	if clear.count() != 1 {
		t.Errorf("owner-checked release: want exactly 1 call, got %d", clear.count())
	}
	if exitCode.Load() != -1 {
		t.Errorf("graceful shutdown must not exit(1); got exit(%d)", exitCode.Load())
	}
	if l.AllowRPC() {
		t.Error("RPCs still admitted after shutdown")
	}
}

// never-leader: election cancelled first, no label ops, no release, no exit.
func TestShutdownNeverLeader(t *testing.T) {
	cfg := haConfig(t)
	labels := &fakeLabeler{}
	clear := &fakeClearer{}
	l, exitCode := newTestLeadership(t, cfg, labels, clear, nil)
	installFakeElection(l, false)

	electionCancelledBeforeDrain := false
	l.Shutdown(func(time.Duration) bool {
		select {
		case <-l.runDone:
			electionCancelledBeforeDrain = true
		case <-time.After(time.Second):
		}
		return true
	}, time.Second)

	if !electionCancelledBeforeDrain {
		t.Error("never-leader shutdown must cancel the election before/at drain (C-13: a terminating candidate must not acquire)")
	}
	if clear.count() != 0 {
		t.Error("never-leader must not touch the lease")
	}
	if labels.removes() != 0 || labels.sets() != 0 {
		t.Error("never-leader must not touch labels")
	}
	if exitCode.Load() != -1 {
		t.Errorf("never-leader shutdown must not exit(1); got exit(%d)", exitCode.Load())
	}
}

// unexpected demotion: reject-before-exit, term cancelled, lease untouched,
// exit(1) exactly once per acquired term (C-05).
func TestUnexpectedDemotion(t *testing.T) {
	cfg := haConfig(t)
	labels := &fakeLabeler{}
	clear := &fakeClearer{}
	bgStarted := make(chan struct{})
	l, exitCode := newTestLeadership(t, cfg, labels, clear, func(ctx context.Context) {
		close(bgStarted)
		<-ctx.Done()
	})
	installFakeElection(l, true)
	go l.onStartedLeading(context.Background())
	<-bgStarted

	l.onStoppedLeading() // renewal lost while leading

	if exitCode.Load() != 1 {
		t.Fatalf("demotion must exit(1); got %d", exitCode.Load())
	}
	if l.AllowRPC() {
		t.Error("RPCs admitted after demotion")
	}
	if clear.count() != 0 {
		t.Error("demotion must NEVER touch the lease (natural expiry fences)")
	}
}

// OnStoppedLeading during a recorded normal shutdown takes NO demotion action
// (C-15: it fires while Shutdown awaits Run()).
func TestOnStoppedLeadingDuringNormalShutdown(t *testing.T) {
	cfg := haConfig(t)
	l, exitCode := newTestLeadership(t, cfg, &fakeLabeler{}, &fakeClearer{}, nil)
	installFakeElection(l, false)
	l.Shutdown(func(time.Duration) bool { return true }, 100*time.Millisecond)
	l.onStoppedLeading()
	if exitCode.Load() != -1 {
		t.Errorf("normal-shutdown OnStoppedLeading must not exit; got %d", exitCode.Load())
	}
}

// ---- quiescence gate (F-09.3 blocked-work, C-18-impl) ----------------------

// a background worker that ignores its context blocks the term forever: the
// drain times out and NO lease release may be issued.
func TestQuiescenceGateBlockedBackground(t *testing.T) {
	cfg := haConfig(t)
	labels := &fakeLabeler{}
	clear := &fakeClearer{}
	block := make(chan struct{})
	defer close(block)
	bgStarted := make(chan struct{})
	l, exitCode := newTestLeadership(t, cfg, labels, clear, func(ctx context.Context) {
		close(bgStarted)
		<-block // ignores ctx: simulates a stuck context-free janitor/cloud call
	})
	installFakeElection(l, true)
	go l.onStartedLeading(context.Background())
	<-bgStarted
	waitFor(t, "routing published", func() bool { return labels.sets() == 1 })

	l.Shutdown(func(time.Duration) bool { return true }, 50*time.Millisecond)

	if clear.count() != 0 {
		t.Fatal("lease released despite unproven quiescence (blocked background worker)")
	}
	if exitCode.Load() != -1 {
		t.Errorf("shutdown path must not exit(1); got %d", exitCode.Load())
	}
}

// a gRPC drain timeout alone must also disable the fast release.
func TestQuiescenceGateBlockedHandler(t *testing.T) {
	cfg := haConfig(t)
	labels := &fakeLabeler{}
	clear := &fakeClearer{}
	bgStarted := make(chan struct{})
	l, _ := newTestLeadership(t, cfg, labels, clear, func(ctx context.Context) {
		close(bgStarted)
		<-ctx.Done()
	})
	installFakeElection(l, true)
	go l.onStartedLeading(context.Background())
	<-bgStarted
	waitFor(t, "routing published", func() bool { return labels.sets() == 1 })

	l.Shutdown(func(time.Duration) bool { return false }, 200*time.Millisecond) // GracefulStop gave up

	if clear.count() != 0 {
		t.Fatal("lease released despite gRPC drain timeout")
	}
}

// a label patch blocked BEFORE worker registration keeps the term open
// (C-19-impl race b): the callback is stuck pre-workers, quiescence cannot be
// proven, and the fast release MUST be skipped.
func TestQuiescenceGateBlockedLabelPatch(t *testing.T) {
	cfg := haConfig(t)
	labels := &fakeLabeler{blockSet: make(chan struct{})} // ctx-deaf block
	defer close(labels.blockSet)
	clear := &fakeClearer{}
	l, exitCode := newTestLeadership(t, cfg, labels, clear, func(ctx context.Context) { <-ctx.Done() })
	installFakeElection(l, true)
	go l.onStartedLeading(context.Background())
	waitFor(t, "term begun", func() bool { return l.termContext() != nil })

	l.Shutdown(func(time.Duration) bool { return true }, 50*time.Millisecond)

	if clear.count() != 0 {
		t.Fatal("lease released while the acquisition callback was still blocked in the label patch")
	}
	if exitCode.Load() != -1 {
		t.Errorf("no exit(1) expected on the shutdown path; got %d", exitCode.Load())
	}
}

// ---- C-19-impl term lifecycle ----------------------------------------------

// (a)+(c): a callback that starts after shutdown was recorded refuses the
// term: no publication, no background, cbDone still closes.
func TestCallbackAfterShutdownRefusesTerm(t *testing.T) {
	cfg := haConfig(t)
	labels := &fakeLabeler{}
	clear := &fakeClearer{}
	bgRan := atomic.Bool{}
	l, exitCode := newTestLeadership(t, cfg, labels, clear, func(ctx context.Context) { bgRan.Store(true) })
	installFakeElection(l, true)

	done := make(chan struct{})
	go func() { l.Shutdown(func(time.Duration) bool { return true }, time.Second); close(done) }()
	// Shutdown (wasLeader=true) blocks awaiting cbDone — deliver the late
	// callback NOW, after shuttingDown was recorded.
	waitFor(t, "shutdown recorded", func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.shuttingDown
	})
	l.onStartedLeading(context.Background()) // the delayed callback (C-19-impl race a)
	<-done

	if labels.sets() != 0 {
		t.Error("late callback published the leader label after shutdown")
	}
	if bgRan.Load() {
		t.Error("late callback started background work after shutdown")
	}
	// lease WAS held with zero work started: fast release is safe and expected.
	if clear.count() != 1 {
		t.Errorf("refused-term shutdown should still release the held lease (quiescence trivially proven); got %d", clear.count())
	}
	if exitCode.Load() != -1 {
		t.Errorf("no exit(1) expected; got %d", exitCode.Load())
	}
}

// ---- routing publication disposition (F-09.7, C-16) ------------------------

func TestPublishRoutingTransientRecovery(t *testing.T) {
	cfg := haConfig(t)
	labels := &fakeLabeler{setErrs: []error{errors.New("transient"), nil}}
	clear := &fakeClearer{}
	bgStarted := make(chan struct{})
	l, exitCode := newTestLeadership(t, cfg, labels, clear, func(ctx context.Context) {
		close(bgStarted)
		<-ctx.Done()
	})
	installFakeElection(l, true)
	go l.onStartedLeading(context.Background())
	select {
	case <-bgStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("background never started after transient label failure")
	}
	if exitCode.Load() != -1 {
		t.Errorf("transient patch failure must not relinquish; got exit(%d)", exitCode.Load())
	}
	l.Shutdown(func(time.Duration) bool { return true }, time.Second)
}

// permanent failure: bounded retries exhaust → relinquish-and-exit, lease
// untouched (expiry hands over).
func TestPublishRoutingExhaustionRelinquishes(t *testing.T) {
	cfg := haConfig(t)
	labels := &fakeLabeler{setErrs: []error{errors.New("forbidden"), errors.New("forbidden"), errors.New("forbidden")}}
	clear := &fakeClearer{}
	l, exitCode := newTestLeadership(t, cfg, labels, clear, func(ctx context.Context) { <-ctx.Done() })
	l.labelRetries = 3
	installFakeElection(l, true)
	go l.onStartedLeading(context.Background())

	waitFor(t, "relinquish exit", func() bool { return exitCode.Load() == 1 })
	if clear.count() != 0 {
		t.Error("relinquish must NOT touch the lease")
	}
}

// leadership loss during label retry: the term context aborts the retry loop
// without exit(1) from the publication path itself.
func TestPublishRoutingAbortsOnTermCancel(t *testing.T) {
	cfg := haConfig(t)
	labels := &fakeLabeler{setErrs: []error{errors.New("e1"), errors.New("e2"), errors.New("e3"), errors.New("e4")}}
	l, _ := newTestLeadership(t, cfg, labels, &fakeClearer{}, nil)
	l.labelRetryGap = 50 * time.Millisecond
	term := l.beginTerm(context.Background())
	go func() { time.Sleep(10 * time.Millisecond); term.cancel() }()
	if l.publishRouting(term) {
		t.Error("publishRouting must fail once the term is cancelled")
	}
	l.endTerm(term)
}

// ---- startup purge (F-09.7, C-16 state machine) ----------------------------

func TestPurgeStaleLabel(t *testing.T) {
	cfg := haConfig(t)
	// success after a transient failure
	labels := &fakeLabeler{remErr: nil}
	l, _ := newTestLeadership(t, cfg, labels, &fakeClearer{}, nil)
	if err := l.PurgeStaleLabel(context.Background()); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if labels.removes() != 1 {
		t.Errorf("want 1 removal, got %d", labels.removes())
	}
	// permanent failure exhausts and errors (caller FATALs — fail closed)
	bad := &fakeLabeler{remErr: errors.New("forbidden")}
	l2, _ := newTestLeadership(t, cfg, bad, &fakeClearer{}, nil)
	l2.labelRetries = 2
	if err := l2.PurgeStaleLabel(context.Background()); err == nil {
		t.Fatal("exhausted purge must error")
	}
}

// ---- LEADER_ELECT=false regression (F-09.6, C-12) --------------------------

func TestLeaderElectDisabledSemantics(t *testing.T) {
	cfg := loadTestConfig(t) // no LEADER_ELECT env → default false
	if cfg.LeaderElect {
		t.Fatal("default must be disabled")
	}
	labels := &fakeLabeler{}
	clear := &fakeClearer{}
	l, exitCode := newTestLeadership(t, cfg, labels, clear, nil)

	// every CloudProvider RPC callable (pinned leader; k8s gating is the
	// kubelet probe's job exactly as v0.1.24 — AllowRPC must not add a gate).
	called := false
	handler := func(ctx context.Context, req any) (any, error) { called = true; return nil, nil }
	if _, err := l.UnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: pb.CloudProvider_Refresh_FullMethodName}, handler); err != nil || !called {
		t.Errorf("disabled mode must pass RPCs, got err=%v", err)
	}
	// no election machinery, no label writes, purge is a no-op.
	if err := l.StartElection(nil); err != nil {
		t.Fatalf("disabled StartElection: %v", err)
	}
	if err := l.PurgeStaleLabel(context.Background()); err != nil {
		t.Fatalf("disabled purge: %v", err)
	}
	if labels.removes() != 0 || labels.sets() != 0 {
		t.Error("disabled mode touched pod labels")
	}
	// shutdown drains gRPC, touches neither lease nor labels, no exit(1).
	stopped := false
	l.Shutdown(func(time.Duration) bool { stopped = true; return true }, time.Second)
	if !stopped || clear.count() != 0 || labels.removes() != 0 || exitCode.Load() != -1 {
		t.Errorf("disabled shutdown wrong: stopped=%v clears=%d removes=%d exit=%d", stopped, clear.count(), labels.removes(), exitCode.Load())
	}
}

// ---- config validation (F-09.8) --------------------------------------------

func TestLeaderConfigValidation(t *testing.T) {
	base := map[string]string{
		"ENS_REGION": "ens-region-1", "CLUSTER_ID": "c-test",
		"NETWORK_ID": "n-x", "VSWITCH_ID": "vsw-x", "SECURITY_GROUP_ID": "sg-default",
		"NODE_GROUPS": groupsJSON,
	}
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"missing pod identity", map[string]string{"LEADER_ELECT": "true"}, "POD_NAME"},
		{"lease <= renew", map[string]string{"LEADER_ELECT": "true", "POD_NAME": "p", "POD_NAMESPACE": "ns",
			"LEADER_LEASE_DURATION": "10s", "LEADER_RENEW_DEADLINE": "10s"}, "LEADER_LEASE_DURATION"},
		{"renew <= 1.2*retry", map[string]string{"LEADER_ELECT": "true", "POD_NAME": "p", "POD_NAMESPACE": "ns",
			"LEADER_RENEW_DEADLINE": "2s", "LEADER_RETRY_PERIOD": "2s"}, "LEADER_RENEW_DEADLINE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{}
			for k, v := range base {
				env[k] = v
			}
			for k, v := range tc.env {
				env[k] = v
			}
			setEnv(t, env)
			if _, err := LoadConfig(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

// ---- Kube label + lease surfaces (fake clientset) --------------------------

func TestKubePodLabelLifecycle(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "ens-provider-0", Namespace: "kube-system",
		Labels: map[string]string{"app": "ens-provider", RoleLabelKey: RoleLabelValue}, // stale from a previous incarnation
	}})
	k := &Kube{cfg: &Config{}, client: client}
	ctx := context.Background()

	if err := k.RemovePodLabel(ctx, "kube-system", "ens-provider-0", RoleLabelKey); err != nil {
		t.Fatalf("remove: %v", err)
	}
	pod, _ := client.CoreV1().Pods("kube-system").Get(ctx, "ens-provider-0", metav1.GetOptions{})
	if _, present := pod.Labels[RoleLabelKey]; present {
		t.Fatal("stale leader label not removed")
	}
	if pod.Labels["app"] != "ens-provider" {
		t.Fatal("unrelated label clobbered")
	}
	if err := k.SetPodLabel(ctx, "kube-system", "ens-provider-0", RoleLabelKey, RoleLabelValue); err != nil {
		t.Fatalf("set: %v", err)
	}
	pod, _ = client.CoreV1().Pods("kube-system").Get(ctx, "ens-provider-0", metav1.GetOptions{})
	if pod.Labels[RoleLabelKey] != RoleLabelValue {
		t.Fatal("leader label not set")
	}
	// removing from a missing pod is idempotent success (pod death already
	// dropped the endpoint).
	if err := k.RemovePodLabel(ctx, "kube-system", "no-such-pod", RoleLabelKey); err != nil {
		t.Fatalf("remove on missing pod must succeed, got %v", err)
	}
}

func leaseFixture(holder string) *coordinationv1.Lease {
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: LeaseName, Namespace: "kube-system"},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder},
	}
}

func TestClearLeaseHolder(t *testing.T) {
	ctx := context.Background()

	// held by us → cleared
	client := fake.NewSimpleClientset(leaseFixture("me"))
	k := &Kube{cfg: &Config{}, client: client}
	if err := k.ClearLeaseHolder(ctx, "kube-system", LeaseName, "me"); err != nil {
		t.Fatalf("clear: %v", err)
	}
	lease, _ := client.CoordinationV1().Leases("kube-system").Get(ctx, LeaseName, metav1.GetOptions{})
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "" {
		t.Fatalf("holder not cleared: %v", lease.Spec.HolderIdentity)
	}

	// held by the peer → refuse, no write
	client2 := fake.NewSimpleClientset(leaseFixture("peer"))
	k2 := &Kube{cfg: &Config{}, client: client2}
	if err := k2.ClearLeaseHolder(ctx, "kube-system", LeaseName, "me"); err == nil {
		t.Fatal("clearing a peer-held lease must refuse")
	}
	lease2, _ := client2.CoordinationV1().Leases("kube-system").Get(ctx, LeaseName, metav1.GetOptions{})
	if *lease2.Spec.HolderIdentity != "peer" {
		t.Fatal("peer-held lease was modified")
	}

	// blocked API → bounded error (caller falls back to expiry)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := k.ClearLeaseHolder(cctx, "kube-system", LeaseName, "me"); err == nil {
		t.Fatal("cancelled context must surface an error")
	}
}

// ---- integration smoke: REAL elector, no renewal after release (C-15) ------

func TestRealElectorReleaseNoRenewalFollows(t *testing.T) {
	cfg := haConfig(t)
	cfg.LeaderLeaseDuration = 300 * time.Millisecond
	cfg.LeaderRenewDeadline = 200 * time.Millisecond
	cfg.LeaderRetryPeriod = 100 * time.Millisecond
	cfg.PodName = "ens-provider-real-0"

	client := fake.NewSimpleClientset()
	k := &Kube{cfg: cfg, client: client}
	bgStarted := make(chan struct{})
	l, exitCode := newTestLeadership(t, cfg, k, k, func(ctx context.Context) {
		close(bgStarted)
		<-ctx.Done()
	})
	// pre-create OUR pod so the label patch has a target
	if _, err := client.CoreV1().Pods("kube-system").Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: cfg.PodName, Namespace: "kube-system"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := l.StartElection(client); err != nil {
		t.Fatalf("start election: %v", err)
	}
	select {
	case <-bgStarted: // acquired, published, background running
	case <-time.After(5 * time.Second):
		t.Fatal("real elector never acquired on the fake clientset")
	}

	l.Shutdown(func(time.Duration) bool { return true }, 2*time.Second)

	lease, err := client.CoordinationV1().Leases("kube-system").Get(context.Background(), LeaseName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "" {
		t.Fatalf("owner-checked release did not clear the holder: %v", lease.Spec.HolderIdentity)
	}
	// C-15's load-bearing assertion: NO renewal may follow the release. The
	// election goroutine was joined before the clear; give any stray renewer
	// 3 retry periods to falsify that.
	time.Sleep(3 * cfg.LeaderRetryPeriod)
	lease, _ = client.CoordinationV1().Leases("kube-system").Get(context.Background(), LeaseName, metav1.GetOptions{})
	if *lease.Spec.HolderIdentity != "" {
		t.Fatalf("a renewal followed the release: holder=%q", *lease.Spec.HolderIdentity)
	}
	if exitCode.Load() != -1 {
		t.Errorf("graceful path exited: %d", exitCode.Load())
	}
}

// ---- round-7 regressions (C-20-impl .. C-24-impl) --------------------------

// C-21-impl: admission and term capture are ONE atomic step; endTerm cancels
// the term BEFORE flipping state, so an RPC that was admitted a moment before
// demotion always runs under a context that dies with the term.
func TestAdmitEndTermInterleaving(t *testing.T) {
	cfg := haConfig(t)
	l, _ := newTestLeadership(t, cfg, &fakeLabeler{}, &fakeClearer{}, nil)
	term := l.beginTerm(context.Background())
	l.SetK8sOK(true)

	// an RPC enters the handler while leader…
	entered := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		_, err := l.UnaryInterceptor(context.Background(), nil,
			&grpc.UnaryServerInfo{FullMethod: pb.CloudProvider_NodeGroupDeleteNodes_FullMethodName},
			func(ctx context.Context, req any) (any, error) {
				close(entered)
				<-ctx.Done() // in-flight work
				return nil, ctx.Err()
			})
		finished <- err
	}()
	<-entered
	// …then loses leadership via endTerm (NOT a bare term.cancel).
	l.endTerm(term)
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("endTerm did not cancel the in-flight admitted RPC (C-21-impl)")
	}

	// cancelled-but-not-yet-ended term (demote window): admission must reject.
	term2 := l.beginTerm(context.Background())
	term2.cancel()
	if _, ok := l.admit(); ok {
		t.Fatal("admit() accepted an RPC against an already-cancelled term")
	}
	l.endTerm(term2)
}

// C-20-impl: runServer must not return until the shutdown orchestration —
// including the owner-checked release — has completed.
func TestRunServerJoinsShutdown(t *testing.T) {
	cfg := haConfig(t)
	labels := &fakeLabeler{}
	clear := &fakeClearer{}
	bgStarted := make(chan struct{})
	l, exitCode := newTestLeadership(t, cfg, labels, clear, func(ctx context.Context) {
		close(bgStarted)
		<-ctx.Done()
	})
	installFakeElection(l, true)
	go l.onStartedLeading(context.Background())
	<-bgStarted

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(panicRecover, l.UnaryInterceptor), grpc.ChainStreamInterceptor(l.StreamInterceptor))
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)

	ctx, cancel := context.WithCancel(context.Background())
	ret := make(chan error, 1)
	go func() { ret <- runServer(ctx, srv, lis, hs, l, 2*time.Second) }()
	time.Sleep(50 * time.Millisecond) // let Serve start
	cancel()

	select {
	case err := <-ret:
		if err != nil {
			t.Fatalf("runServer: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runServer did not return")
	}
	// the load-bearing assertion: by the time runServer returned, the lease
	// release had already happened (no race with process exit).
	if clear.count() != 1 {
		t.Fatalf("owner-checked release not completed before runServer returned: %d calls", clear.count())
	}
	if exitCode.Load() != -1 {
		t.Errorf("graceful path exited: %d", exitCode.Load())
	}
}

// C-22-impl: ONE deadline governs the whole shutdown — a fully blocked drain
// must complete in ~budget, not budget×phases.
func TestShutdownSingleDeadline(t *testing.T) {
	cfg := haConfig(t)
	block := make(chan struct{})
	defer close(block)
	bgStarted := make(chan struct{})
	l, _ := newTestLeadership(t, cfg, &fakeLabeler{}, &fakeClearer{}, func(ctx context.Context) {
		close(bgStarted)
		<-block // ctx-deaf: worst case, every phase would time out
	})
	installFakeElection(l, true)
	go l.onStartedLeading(context.Background())
	<-bgStarted

	budget := 500 * time.Millisecond
	start := time.Now()
	l.Shutdown(func(remaining time.Duration) bool {
		// a real GracefulStop would also burn its remaining slice; a blocked
		// one returns false at its deadline.
		time.Sleep(remaining)
		return false
	}, budget)
	elapsed := time.Since(start)
	// per-phase stacking would take ≥4×budget (term + grpc + runDone + cbDone).
	if elapsed > 2*budget {
		t.Fatalf("shutdown exceeded the single deadline: %s (budget %s)", elapsed, budget)
	}
}

// C-23-impl: a REAL long-lived health Watch stream over bufconn observes the
// leadership transitions on the named service, from a follower connection.
func TestHealthWatchAcrossTransitions(t *testing.T) {
	cfg := haConfig(t)
	hs := health.NewServer()
	l := newLeadership(cfg, &fakeLabeler{}, &fakeClearer{}, hs, nil)
	l.logf = t.Logf

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(panicRecover, l.UnaryInterceptor), grpc.ChainStreamInterceptor(l.StreamInterceptor))
	healthpb.RegisterHealthServer(srv, hs)
	go srv.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := healthpb.NewHealthClient(conn).Watch(ctx, &healthpb.HealthCheckRequest{Service: leaderHealthService})
	if err != nil {
		t.Fatalf("watch (must pass the interceptor on a follower): %v", err)
	}
	expect := func(want healthpb.HealthCheckResponse_ServingStatus) {
		t.Helper()
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("watch recv: %v", err)
		}
		if resp.Status != want {
			t.Fatalf("watch: want %v got %v", want, resp.Status)
		}
	}
	expect(healthpb.HealthCheckResponse_NOT_SERVING) // follower
	term := l.beginTerm(context.Background())
	l.SetK8sOK(true)
	expect(healthpb.HealthCheckResponse_SERVING) // leader + k8s
	l.endTerm(term)
	expect(healthpb.HealthCheckResponse_NOT_SERVING) // demoted — the same stream lives on
}

// C-23-impl: lease loss during a REAL DeleteNodes — the uncancellable release
// completes (accepted residual), but the post-cancellation context-aware Node
// deletion fails closed and the RPC surfaces the error.
type blockingReleaseCloud struct {
	*fakeCloud
	gate     chan struct{} // release blocks here (uncancellable SDK call)
	entered  chan struct{}
	released []string
	mu       sync.Mutex
}

func (b *blockingReleaseCloud) ReleaseInstance(id string) error {
	close(b.entered)
	<-b.gate // tea-SDK call on the wire: no context
	b.mu.Lock()
	defer b.mu.Unlock()
	b.released = append(b.released, id)
	return nil
}

type ctxAwareKube struct{ *fakeKube }

func (c ctxAwareKube) DeleteNodeObject(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err // fenced: no K8s write after demotion
	}
	return c.fakeKube.DeleteNodeObject(ctx, name)
}

func TestDeleteNodesFencedByLeaseLoss(t *testing.T) {
	cfg := haConfig(t)
	l, _ := newTestLeadership(t, cfg, &fakeLabeler{}, &fakeClearer{}, nil)
	term := l.beginTerm(context.Background())
	l.SetK8sOK(true)

	bc := &blockingReleaseCloud{
		fakeCloud: &fakeCloud{instances: []Instance{{ID: "i-x1", Name: "edge-web-op01-1", GroupID: "np-a", Status: "Running"}}},
		gate:      make(chan struct{}),
		entered:   make(chan struct{}),
	}
	s := NewServer(cfg, bc, ctxAwareKube{&fakeKube{}})

	req := &pb.NodeGroupDeleteNodesRequest{Id: "np-a", Nodes: []*pb.ExternalGrpcNode{
		{Name: "node-x1", ProviderID: pid("i-x1")},
	}}
	var handlerCtx atomic.Value // the MERGED handler context, captured for the fence check
	done := make(chan error, 1)
	go func() {
		_, err := l.UnaryInterceptor(context.Background(), nil,
			&grpc.UnaryServerInfo{FullMethod: pb.CloudProvider_NodeGroupDeleteNodes_FullMethodName},
			func(ctx context.Context, _ any) (any, error) {
				handlerCtx.Store(ctx)
				return s.NodeGroupDeleteNodes(ctx, req)
			})
		done <- err
	}()
	<-bc.entered  // the release is on the wire…
	l.endTerm(term) // …when leadership is lost
	// context.AfterFunc propagates the cancellation ASYNCHRONOUSLY — wait for
	// the merged context to observe it before letting the release return, or
	// the fence assertion races the propagation (a real release call takes
	// far longer than the goroutine hop; the test must not depend on luck).
	waitFor(t, "merged ctx cancelled", func() bool {
		c, _ := handlerCtx.Load().(context.Context)
		return c != nil && c.Err() != nil
	})
	close(bc.gate) // the uncancellable call eventually returns

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("DeleteNodes must surface the fenced Node deletion")
		}
		if !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("want a context-cancellation failure, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("DeleteNodes hung")
	}
	bc.mu.Lock()
	released := len(bc.released)
	bc.mu.Unlock()
	if released != 1 {
		t.Fatalf("the in-flight release is the ACCEPTED residual and should complete; released=%d", released)
	}
}

// C-23-impl: lease loss during RunInstances — the merged context reaches the
// CloudAPI call (the ens.go chain consumes it in submitOrder/awaitOpInstances).
func TestRunInstancesReceivesTermContext(t *testing.T) {
	cfg := haConfig(t)
	l, _ := newTestLeadership(t, cfg, &fakeLabeler{}, &fakeClearer{}, nil)
	term := l.beginTerm(context.Background())
	l.SetK8sOK(true)

	fc := &ctxSensitiveCloud{fakeCloud: &fakeCloud{}, entered: make(chan struct{})}
	s := NewServer(cfg, fc, &fakeKube{})
	done := make(chan error, 1)
	go func() {
		_, err := l.UnaryInterceptor(context.Background(), nil,
			&grpc.UnaryServerInfo{FullMethod: pb.CloudProvider_NodeGroupIncreaseSize_FullMethodName},
			func(ctx context.Context, _ any) (any, error) {
				return s.NodeGroupIncreaseSize(ctx, &pb.NodeGroupIncreaseSizeRequest{Id: "np-a", Delta: 1})
			})
		done <- err
	}()
	<-fc.entered
	l.endTerm(term)
	select {
	case err := <-done:
		if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("want Internal/context-canceled from the fenced RunInstances, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("IncreaseSize hung after term cancellation")
	}
}

type ctxSensitiveCloud struct {
	*fakeCloud
	entered chan struct{}
}

func (c *ctxSensitiveCloud) RunInstances(ctx context.Context, g *Group, count int32) ([]string, error) {
	close(c.entered)
	<-ctx.Done() // models awaitOpInstances/submitOrder honoring the merged ctx
	return nil, ctx.Err()
}

// C-23-impl companion: the real awaitOpInstances aborts on a cancelled
// context BEFORE touching the ENS client (nil client would panic otherwise).
func TestAwaitOpInstancesAbortsOnCancel(t *testing.T) {
	c := &Cloud{cfg: &Config{DescribeTTLSeconds: 10}} // nil ens client: any describe would panic
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if ids := c.awaitOpInstances(ctx, "op-x", 2, 6); ids != nil {
		t.Fatalf("cancelled await must return nil, got %v", ids)
	}
	if time.Since(start) > time.Second {
		t.Fatal("cancelled await did not return promptly")
	}
}

// C-24-impl: permanent API errors fail the startup purge IMMEDIATELY and make
// the acquisition-time publication relinquish without burning retries.
func TestPermanentErrorsFailFast(t *testing.T) {
	cfg := haConfig(t)
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "ens-provider-test-0", errors.New("rbac"))

	// startup purge: one attempt, immediate error.
	labels := &fakeLabeler{remErr: forbidden}
	l, _ := newTestLeadership(t, cfg, labels, &fakeClearer{}, nil)
	l.labelRetries = 10
	start := time.Now()
	if err := l.PurgeStaleLabel(context.Background()); err == nil {
		t.Fatal("forbidden purge must error")
	}
	if labels.removes() != 1 {
		t.Fatalf("forbidden purge must not retry: %d attempts", labels.removes())
	}
	if time.Since(start) > time.Second {
		t.Fatal("forbidden purge burned the retry budget")
	}

	// acquisition publication: immediate relinquish.
	labels2 := &fakeLabeler{setErrs: []error{forbidden, forbidden, forbidden, forbidden, forbidden, forbidden, forbidden, forbidden, forbidden, forbidden}}
	l2, exitCode := newTestLeadership(t, cfg, labels2, &fakeClearer{}, func(ctx context.Context) { <-ctx.Done() })
	installFakeElection(l2, true)
	go l2.onStartedLeading(context.Background())
	waitFor(t, "permanent-error relinquish", func() bool { return exitCode.Load() == 1 })
	if labels2.sets() != 1 {
		t.Fatalf("forbidden publication must not retry: %d attempts", labels2.sets())
	}
}

// C-25-impl: a shutdown signal that BEATS Serve to the start line makes Serve
// return ErrServerStopped — runServer must still join the orchestration and
// return nil, with the lease release completed. No startup sleep: the context
// is cancelled before runServer is even called.
func TestRunServerAlreadyCancelledContext(t *testing.T) {
	cfg := haConfig(t)
	labels := &fakeLabeler{}
	clear := &fakeClearer{}
	bgStarted := make(chan struct{})
	l, exitCode := newTestLeadership(t, cfg, labels, clear, func(ctx context.Context) {
		close(bgStarted)
		<-ctx.Done()
	})
	installFakeElection(l, true)
	go l.onStartedLeading(context.Background())
	<-bgStarted

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(panicRecover, l.UnaryInterceptor), grpc.ChainStreamInterceptor(l.StreamInterceptor))
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the signal has ALREADY fired

	ret := make(chan error, 1)
	go func() { ret <- runServer(ctx, srv, lis, hs, l, 2*time.Second) }()
	select {
	case err := <-ret:
		if err != nil {
			t.Fatalf("runServer must swallow ErrServerStopped on the shutdown path, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runServer did not return")
	}
	if clear.count() != 1 {
		t.Fatalf("lease release must complete before runServer returns even when shutdown wins the race: %d calls", clear.count())
	}
	if exitCode.Load() != -1 {
		t.Errorf("graceful path exited: %d", exitCode.Load())
	}
}
