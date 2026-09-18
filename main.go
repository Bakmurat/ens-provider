// ens-provider — an upstream-Cluster-Autoscaler `externalgrpc` CloudProvider
// server for Alibaba Cloud ENS edge nodepools. MULTI-NODEGROUP. Go port of the
// live-validated Python PoC implementation, following the community pattern:
// vendored upstream gRPC stubs (protos/, from the CA tag matching the CA image),
// native k8s.io/api types, typed Alibaba SDK clients, RRSA/OIDC credentials from
// the standard env vars, single static binary.
//
// WHY externalgrpc: ENS edge nodepools have no native autoscaling (only cloud
// `ess` pools do), and neither the in-tree CA `alicloud` provider (drives
// ESS/ECS) nor Karpenter (no ENS provider) can scale them. externalgrpc lets us
// reuse the mature CA core (batching, expanders, PDB-aware draining, backoff,
// metrics) and implement ONLY the ENS-specific node-group operations here,
// without forking the autoscaler (see cloudprovider/POLICY.md upstream).
//
// Lifecycle (v0.1.12, design plan; readiness v0.1.16, Issue 3): signal-aware root
// context, ticker-driven reconciler, gRPC health service whose readiness is
// gated on KUBERNETES reachability (bounded Nodes().List(Limit:1)) — starts
// NOT_SERVING, goes SERVING only after a successful check, and returns to
// NOT_SERVING on API loss. Transient ENS health is a metric concern, NEVER a
// readiness or restart trigger (design record §12.5); process liveness (tcpSocket) is
// independent of Kubernetes. Panic-recovery interceptor; graceful stop with a
// 10s deadline; healthSrv.Shutdown() publishes a terminal NOT_SERVING.
//
// HA (v0.1.25): with LEADER_ELECT=true two replicas run active-
// passive — Lease election, leader-label Service routing, fail-closed
// leadership interceptors, leadership-scoped background work, quiescence-
// gated fast lease handoff. See leader.go. Startup ORDER is load-bearing
// (C-16): kube client → stale-label purge → gRPC listener →
// readiness → election. With LEADER_ELECT=false (the default) everything
// below degrades to the v0.1.24 flow.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	pb "ens-provider/protos"
)

// panicRecover keeps one bad RPC from killing a provider replica; the
// panic surfaces as codes.Internal so CA logs it and retries.
func panicRecover(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in %s: %v", info.FullMethod, r)
			err = status.Errorf(codes.Internal, "panic in %s: %v", info.FullMethod, r)
		}
	}()
	return handler(ctx, req)
}

// backgroundLoop is the providerID reconciler + alert-first janitor + stale-
// Node cleanup (design plan/1.8, v0.1.19). With HA it runs ONLY inside a
// leadership term (its ctx is the term context, C-17 contract item 5);
// without HA it runs for the whole process exactly as v0.1.24 did.
func backgroundLoop(ctx context.Context, cfg *Config, cloud *Cloud, kube *Kube) {
	t := time.NewTicker(time.Duration(cfg.ReconcileLoopSeconds) * time.Second)
	defer t.Stop()
	for {
		if _, err := kube.ReconcileProviderIDs(ctx, cloud); err != nil {
			log.Printf("providerID reconciler: %v", err)
		}
		if joined, err := kube.JoinedInstanceIDs(ctx); err != nil {
			log.Printf("janitor: joined ids: %v", err)
		} else if nodeKeys, err := kube.ManagedNodeKeys(ctx); err != nil {
			log.Printf("janitor: node keys: %v", err)
		} else {
			runJanitor(time.Now(), cfg, cloud, joined, nodeKeys)
		}
		// stale-Node cleanup (v0.1.19): reap Node objects whose instance
		// is confirmed gone - the at-floor T7 gap CA never covers
		if nodes, err := kube.ListManagedNodes(ctx); err != nil {
			log.Printf("janitor: node-cleanup list: %v", err)
		} else {
			runNodeCleanup(ctx, time.Now(), cfg, cloud, kube, nodes)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// teeSetter fans the readiness controller's transitions out to BOTH the
// kubelet-facing "" health service (v0.1.24 semantics, both pods stay Ready
// while Kubernetes is reachable — C-14) and the leadership plane.
type teeSetter struct {
	primary statusSetter
	lead    *leadership
}

func (t teeSetter) SetServingStatus(service string, st healthpb.HealthCheckResponse_ServingStatus) {
	t.primary.SetServingStatus(service, st)
	t.lead.SetK8sOK(st == healthpb.HealthCheckResponse_SERVING)
}

// isoUTCLogWriter prefixes each stdlib-log line with an RFC3339/ISO-8601 UTC
// timestamp at microsecond precision with a trailing Z, e.g.
// "2026-08-10T22:56:46.123456Z". Go's stdlib log can only emit the
// "2006/01/02 15:04:05" shape via flags, which the ELK Vector pipeline's grok
// timestamp patterns don't recognize (they expect the dash/T/Z ISO form), so
// it discarded our log time and stamped @timestamp with the ingest time. This
// writer makes the pipeline's existing "%Y-%m-%dT%H:%M:%S%.6fZ" branch match.
// The default logger targets os.Stderr, which this preserves. klog output
// (leader-election "I0810 ..." lines) bypasses the stdlib logger and is
// already parsed by the pipeline, so it is intentionally left untouched.
type isoUTCLogWriter struct{}

func (isoUTCLogWriter) Write(p []byte) (int, error) {
	ts := time.Now().UTC().Format("2006-01-02T15:04:05.000000Z")
	buf := make([]byte, 0, len(ts)+1+len(p))
	buf = append(buf, ts...)
	buf = append(buf, ' ')
	buf = append(buf, p...)
	if _, err := os.Stderr.Write(buf); err != nil {
		return 0, err
	}
	return len(p), nil
}

func main() {
	// ISO-8601/RFC3339 UTC timestamps (see isoUTCLogWriter) so the ELK Vector
	// pipeline parses @timestamp instead of falling back to ingest time.
	log.SetFlags(0)
	log.SetOutput(isoUTCLogWriter{})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	cloud, err := NewCloud(cfg)
	if err != nil {
		log.Fatalf("cloud: %v", err)
	}
	kube, err := NewKube(cfg)
	if err != nil {
		log.Fatalf("kube: %v", err)
	}

	// startup auth probe — fail loud and early if RRSA is misconfigured; also
	// warms the describe cache before the first CA call. Runs on BOTH pods
	// (read-only): a follower has validated RRSA before it ever wins.
	if _, err := cloud.AllOwnedInstances(true); err != nil {
		log.Printf("WARNING: initial DescribeInstances failed (auth/RRSA?): %v", err)
	} else {
		log.Printf("auth OK (RRSA/OIDC credential chain)")
	}

	healthSrv := health.NewServer()
	// health.NewServer() defaults the aggregate service ("") to SERVING; force
	// NOT_SERVING so we never advertise Ready before the first successful
	// Kubernetes check (Issue 3). The readiness controller is the sole writer.
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

	lead := newLeadership(cfg, kube, kube, healthSrv,
		func(termCtx context.Context) {
			// term-start uncached inventory (honest scope, C-04: this
			// only clears local cache staleness — it is NOT a reservation-
			// transfer mitigation; a Describe-invisible order stays invisible).
			if _, err := cloud.AllOwnedInstances(true); err != nil {
				log.Printf("[leader] term-start inventory refresh failed (janitor will retry): %v", err)
			}
			backgroundLoop(termCtx, cfg, cloud, kube)
		})

	// C-16 step 2: purge a stale leader label from a previous container
	// incarnation BEFORE the listener opens and BEFORE election starts. The
	// purge retries internally; exhaustion (incl. a definite Forbidden) is
	// fail-closed FATAL — a stale-labeled Ready follower must never serve.
	if err := lead.PurgeStaleLabel(ctx); err != nil {
		log.Fatalf("leader: %v", err)
	}

	if !cfg.LeaderElect {
		// v0.1.24 path: background work is process-scoped, no election.
		go backgroundLoop(ctx, cfg, cloud, kube)
	}

	go serveMetrics(cfg.MetricsAddr) // slim observability (V1-1)

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", cfg.ListenAddr, err)
	}
	// NOTE: plaintext, in-cluster only (Service ens-provider:8086). mTLS via
	// cert-manager is a production follow-up.
	// Interceptor order: panicRecover OUTERMOST, then the fail-closed
	// leadership gate (exempts only /grpc.health.v1.Health/*, merges the
	// term context into handler contexts — C-03/C-10).
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(panicRecover, lead.UnaryInterceptor),
		grpc.ChainStreamInterceptor(lead.StreamInterceptor),
	)
	pb.RegisterCloudProviderServer(srv, NewServer(cfg, cloud, kube))
	healthpb.RegisterHealthServer(srv, healthSrv)

	// Kubernetes-gated readiness: the kubelet-facing "" service is SERVING only
	// while the K8s API is reachable — on BOTH pods (leadership never touches
	// it, C-14); the tee also feeds the leadership serving state. ENS health
	// never gates it; liveness stays process-only (tcpSocket).
	go runReadiness(ctx, kube.Ping, teeSetter{primary: healthSrv, lead: lead},
		cfg.ReadinessCheckInterval, cfg.ReadinessCheckTimeout, log.Printf)

	// C-16 step 5: election starts LAST, on its own dedicated context.
	if err := lead.StartElection(kube.Clientset()); err != nil {
		log.Fatalf("leader: %v", err)
	}

	groups := ""
	for _, g := range cfg.Groups {
		groups += fmt.Sprintf(" %s[%d,%d]", g.ID, g.Min, g.Max)
	}
	mode := "single-replica"
	if cfg.LeaderElect {
		mode = fmt.Sprintf("leader-elect (lease %s/%s)", cfg.PodNamespace, LeaseName)
	}
	log.Printf("ENS externalgrpc provider (go) starting on %s | region=%s | %s | %d node group(s):%s",
		cfg.ListenAddr, cfg.EnsRegion, mode, len(cfg.Groups), groups)
	if err := runServer(ctx, srv, lis, healthSrv, lead, 10*time.Second); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// runServer serves gRPC and, on signal, runs the FULL F-04 v6 shutdown
// orchestration BEFORE returning (C-20-impl: GracefulStop unblocks
// Serve, so a detached shutdown goroutine would race process exit and lose
// the owner-checked lease handoff — the entrypoint must join it).
//
// Sequence on ctx cancellation: terminal NOT_SERVING → lead.Shutdown (reject
// new RPCs → cancel leadership-term work → drain handlers+workers to proven
// quiescence → join election + acquisition callback → owner-checked lease
// release ONLY if everything quiesced, all under ONE `budget` deadline,
// C-22-impl) → Serve returns → runServer returns.
func runServer(ctx context.Context, srv *grpc.Server, lis net.Listener, healthSrv interface{ Shutdown() }, lead *leadership, budget time.Duration) error {
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		log.Printf("shutdown signal - draining (%s total budget)", budget)
		// Shutdown() publishes a TERMINAL NOT_SERVING and makes every later
		// SetServingStatus a no-op, so a concurrent readiness tick can never
		// flip us back to SERVING during shutdown (grpc-go v1.81.1).
		healthSrv.Shutdown()
		lead.Shutdown(func(remaining time.Duration) bool {
			done := make(chan struct{})
			go func() { srv.GracefulStop(); close(done) }()
			if remaining <= 0 {
				srv.Stop()
				<-done
				return false
			}
			select {
			case <-done:
				return true
			case <-time.After(remaining):
				srv.Stop()
				<-done
				return false
			}
		}, budget)
	}()
	err := srv.Serve(lis)
	if ctx.Err() == nil {
		// Serve ended with NO shutdown signal: a genuine listener/server
		// failure — the shutdown goroutine will never fire, return now.
		return err
	}
	// Shutdown was signalled — ALWAYS join the orchestration so the lease
	// handoff and election joins complete before the process exits
	// (C-25-impl: if the signal beat Serve to the start line, GracefulStop
	// ran first and Serve returns ErrServerStopped — that is the EXPECTED
	// outcome of that race, not a failure).
	<-shutdownDone
	if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}
