package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	healthgrpc "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// recSetter records every SetServingStatus call, order preserved.
type recSetter struct {
	mu     sync.Mutex
	states []healthpb.HealthCheckResponse_ServingStatus
}

func (r *recSetter) SetServingStatus(_ string, s healthpb.HealthCheckResponse_ServingStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.states = append(r.states, s)
}
func (r *recSetter) last() (healthpb.HealthCheckResponse_ServingStatus, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.states) == 0 {
		return 0, false
	}
	return r.states[len(r.states)-1], true
}
func (r *recSetter) snapshot() []healthpb.HealthCheckResponse_ServingStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]healthpb.HealthCheckResponse_ServingStatus(nil), r.states...)
}

const (
	up   = healthpb.HealthCheckResponse_SERVING
	down = healthpb.HealthCheckResponse_NOT_SERVING
)

// pingSeq yields the given errors in order, repeating the last once exhausted.
func pingSeq(results ...error) pingFunc {
	var mu sync.Mutex
	i := 0
	return func(context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		r := results[i]
		if i < len(results)-1 {
			i++
		}
		return r
	}
}

// drive steps readinessLoop deterministically: one immediate check, then one
// per manual tick, then cancels and waits for return. Returns the log count.
//
// A tick send succeeds only after the loop is back in select — i.e. the PREVIOUS
// check() has fully returned, including any publish. So after the real ticks we
// send ONE sentinel tick: its acceptance barriers the last real check's publish
// before we cancel (C-012 now suppresses publishes once ctx is cancelled, so the
// harness must not race that). pingSeq repeats its last result, making the
// sentinel check a guaranteed no-change (no publish), so it never perturbs
// assertions.
func drive(ping pingFunc, set statusSetter, ticks int) int {
	ctx, cancel := context.WithCancel(context.Background())
	tick := make(chan time.Time)
	var lc int
	logf := func(string, ...any) { lc++ }
	done := make(chan struct{})
	go func() { readinessLoop(ctx, ping, set, time.Second, tick, logf); close(done) }()
	for i := 0; i < ticks; i++ {
		tick <- time.Time{} // received only after the previous check returns
	}
	tick <- time.Time{} // sentinel: barriers the last real check's publish
	cancel()
	<-done
	return lc
}

// Proof 1+2a: health is NOT_SERVING before the first check; an initial failure
// stays down (the first-ever result always publishes).
func TestReadinessInitialFailureStaysDown(t *testing.T) {
	set := &recSetter{}
	drive(pingSeq(errors.New("no api")), set, 0)
	if s, ok := set.last(); !ok || s != down {
		t.Fatalf("initial failure must publish NOT_SERVING, got %v ok=%v", s, ok)
	}
}

// Proof 2b: an initial success publishes SERVING exactly once.
func TestReadinessInitialSuccessServes(t *testing.T) {
	set := &recSetter{}
	logs := drive(pingSeq(nil), set, 0)
	if s, ok := set.last(); !ok || s != up {
		t.Fatalf("initial success must publish SERVING, got %v ok=%v", s, ok)
	}
	if got := set.snapshot(); len(got) != 1 {
		t.Fatalf("exactly one publish on first success, got %d (%v)", len(got), got)
	}
	if logs != 1 {
		t.Fatalf("one transition log expected, got %d", logs)
	}
}

// Proof 3: success -> loss -> recovery; one publish + one log per change, none
// on repeats.
func TestReadinessTransitionsAndLogsOncePerChange(t *testing.T) {
	set := &recSetter{}
	// immediate ok ; ok(no change) ; fail ; fail(no change) ; ok(recover)
	logs := drive(pingSeq(nil, nil, errors.New("down"), errors.New("down"), nil), set, 4)
	want := []healthpb.HealthCheckResponse_ServingStatus{up, down, up}
	got := set.snapshot()
	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("publish %d: want %v got %v (all=%v)", i, want[i], got[i], got)
		}
	}
	if logs != 3 {
		t.Fatalf("want 3 transition logs, got %d", logs)
	}
}

// Proof 4: a check that blocks is cancelled by its own per-check timeout and
// counts as a failure.
func TestReadinessCheckTimeoutIsFailure(t *testing.T) {
	set := &recSetter{}
	ping := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	ctx, cancel := context.WithCancel(context.Background())
	tick := make(chan time.Time)
	done := make(chan struct{})
	go func() { readinessLoop(ctx, ping, set, 20*time.Millisecond, tick, func(string, ...any) {}); close(done) }()
	deadline := time.After(2 * time.Second)
	for {
		if s, ok := set.last(); ok && s == down {
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("a timed-out check never published NOT_SERVING")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

// Proof 5a (C-012): cancelled BEFORE start + a ping that would succeed must
// publish NOTHING — never a stray SERVING during shutdown — and the loop
// returns promptly.
func TestReadinessCancelledBeforeStartPublishesNothing(t *testing.T) {
	set := &recSetter{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before the loop starts
	tick := make(chan time.Time)
	done := make(chan struct{})
	// pingSeq(nil) would return success; the guard must suppress publication.
	go func() { readinessLoop(ctx, pingSeq(nil), set, time.Second, tick, func(string, ...any) {}); close(done) }()
	<-done
	if got := set.snapshot(); len(got) != 0 {
		t.Fatalf("cancelled-before-start must publish nothing, got %v", got)
	}
	select {
	case tick <- time.Time{}:
		t.Fatal("loop accepted a tick after returning")
	default:
	}
}

// Proof 5b (C-012): cancellation RACING an in-flight check whose ping then
// returns nil (success) must NOT publish SERVING — the post-ping root-context
// guard suppresses it and the loop returns.
func TestReadinessCancelRacingInFlightSuccessNoServing(t *testing.T) {
	set := &recSetter{}
	pingStarted := make(chan struct{})
	release := make(chan struct{})
	ping := func(context.Context) error {
		close(pingStarted) // the immediate check has entered ping
		<-release          // block until the test cancels + releases
		return nil         // success returned CONCURRENTLY with cancellation
	}
	ctx, cancel := context.WithCancel(context.Background())
	tick := make(chan time.Time)
	done := make(chan struct{})
	go func() { readinessLoop(ctx, ping, set, time.Second, tick, func(string, ...any) {}); close(done) }()
	<-pingStarted // ping is now in-flight inside the immediate check
	cancel()      // cancel the root context while the check is mid-ping
	close(release)
	<-done
	for _, s := range set.snapshot() {
		if s == up {
			t.Fatalf("a success racing cancellation must not publish SERVING, got %v", set.snapshot())
		}
	}
}

// C-007 mechanism: healthSrv.Shutdown() is terminal — a later SetServingStatus
// is ignored, so a racing readiness tick can never flip us back to SERVING
// during shutdown. Exercises the REAL grpc health server we depend on.
func TestHealthShutdownIsTerminal(t *testing.T) {
	h := healthgrpc.NewServer()
	h.SetServingStatus("", up)
	h.Shutdown() // publishes terminal NOT_SERVING
	h.SetServingStatus("", up) // must be ignored
	resp, err := h.Check(context.Background(), &healthpb.HealthCheckRequest{Service: ""})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.Status != down {
		t.Fatalf("after Shutdown() health must stay NOT_SERVING, got %v", resp.Status)
	}
}

// roundTripFunc adapts a func to http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Proof 6: Kube.Ping issues a nodes List with Limit:1 on the wire and
// propagates API errors. Asserting the request query proves Limit:1 without a
// generated fake; returning an error proves propagation.
func TestKubePingListsWithLimitOneAndPropagates(t *testing.T) {
	var gotQuery, gotPath string
	boom := errors.New("api unreachable")
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		return nil, boom
	})
	cs, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: "http://unit-test"}, &http.Client{Transport: rt})
	if err != nil {
		t.Fatalf("clientset: %v", err)
	}
	k := &Kube{cfg: &Config{}, client: cs}
	err = k.Ping(context.Background())
	if err == nil || !strings.Contains(err.Error(), boom.Error()) {
		t.Fatalf("Ping must propagate the API error, got %v", err)
	}
	if !strings.HasSuffix(gotPath, "/nodes") {
		t.Fatalf("Ping must list nodes, path=%q", gotPath)
	}
	if !strings.Contains(gotQuery, "limit=1") {
		t.Fatalf("Ping must send Limit:1, query=%q", gotQuery)
	}
}
