package main

// Kubernetes-gated gRPC readiness (v0.1.16, Issue 3).
//
// The provider is Ready only while it can reach the Kubernetes API with valid
// credentials/RBAC. ENS/cloud health is a metric concern and MUST NOT gate
// readiness (design record §12.5); process liveness is a separate, K8s-independent probe.
//
// The controller's ONLY dependency is a `ping(ctx) error` — the Kubernetes
// reachability check. That is structural: nothing here can see the cloud, so
// ENS can never gate readiness (C-010). The status setter is the gRPC health
// server; on shutdown the caller invokes healthSrv.Shutdown(), which publishes
// a terminal NOT_SERVING and makes every later SetServingStatus a no-op
// (grpc-go v1.81.1), so no post-shutdown recovery write is possible (C-007).

import (
	"context"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// statusSetter is the slice of *grpc/health.Server the controller needs.
type statusSetter interface {
	SetServingStatus(service string, status healthpb.HealthCheckResponse_ServingStatus)
}

// pingFunc is the Kubernetes reachability check (bounded by the caller's ctx).
type pingFunc func(context.Context) error

// runReadiness bounds each check with its own timeout, then drives the loop off
// a real ticker. The loop body is readinessLoop, which takes an injected tick
// channel so tests can step it deterministically (no wall-clock sleeps, C-010).
func runReadiness(ctx context.Context, ping pingFunc, set statusSetter, interval, timeout time.Duration, logf func(string, ...any)) {
	t := time.NewTicker(interval) // interval is a positive envDuration (C-008)
	defer t.Stop()
	readinessLoop(ctx, ping, set, timeout, t.C, logf)
}

// readinessLoop performs an immediate check, then one per tick, publishing
// SERVING/NOT_SERVING to the health server and logging ONLY on transitions.
// It returns as soon as the ROOT context is cancelled and, from that moment,
// publishes NOTHING — a check that returns success concurrently with
// cancellation must not briefly advertise SERVING during shutdown. The terminal
// NOT_SERVING is owned by healthSrv.Shutdown() in the caller, which is the
// airtight backstop for the residual instructions-level window.
func readinessLoop(ctx context.Context, ping pingFunc, set statusSetter, timeout time.Duration, tick <-chan time.Time, logf func(string, ...any)) {
	// serving is a tri-state: nil = no result yet (health starts NOT_SERVING),
	// so the FIRST result always logs its transition.
	var serving *bool

	// check runs one probe and returns true if the root context is cancelled
	// (caller must then return). It publishes NOTHING once cancellation is
	// observed — checked BOTH before pinging and after ping returns, so a
	// success that races cancellation is suppressed. A per-check TIMEOUT cancels
	// only the derived cctx, leaving ctx.Err()==nil, so it still publishes
	// NOT_SERVING (a real failure, not a shutdown).
	check := func() (cancelled bool) {
		if ctx.Err() != nil {
			return true // shutting down: do not ping, do not publish
		}
		cctx, cancel := context.WithTimeout(ctx, timeout)
		err := ping(cctx)
		cancel()
		if ctx.Err() != nil {
			return true // cancellation raced the ping: publish nothing, stop
		}
		ok := err == nil
		if serving != nil && *serving == ok {
			return false // no change: publish nothing, log nothing
		}
		serving = &ok
		if ok {
			set.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
			logf("[readiness] Kubernetes reachable -> SERVING")
		} else {
			set.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
			logf("[readiness] Kubernetes UNREACHABLE -> NOT_SERVING: %v", err)
		}
		return false
	}

	if check() { // immediate: no startup window where we advertise Ready unchecked
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			if check() {
				return
			}
		}
	}
}
