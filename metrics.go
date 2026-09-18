package main

// Slim observability (v0.1.13, design plan): a hand-rolled Prometheus text
// endpoint - six series, zero new dependencies. The "know when it breaks"
// half of reliable: stock-outs/API failures (orders_total{outcome=failure}),
// join stalls (unjoined_instances), and mutation volume become queryable in
// the live VMSingle instead of living only in logs.

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
)

type providerMetrics struct {
	ordersSuccess   atomic.Int64
	ordersFailure   atomic.Int64
	ordersAmbiguous atomic.Int64 // ambiguous transport outcomes encountered
	released        atomic.Int64
	releaseFailures atomic.Int64
	// reservations that expired without ever becoming Describe-visible
	// (v0.1.22): each one is a loudly-logged bounded-risk headroom release.
	reservationsExpired atomic.Int64
	// HA (v0.1.25): is_leader (Lease held) and routing_published (leader
	// label confirmed on the pod) are SEPARATE gauges — 1/0 divergence is
	// the alert-worthy "leader but unroutable" state (C-16). With
	// LEADER_ELECT=false both are pinned 1.
	isLeader              atomic.Int64
	routingPublished      atomic.Int64
	leadershipTransitions atomic.Int64

	mu       sync.Mutex
	owned    int64 // gauge: instances we own (all groups)
	unjoined int64 // gauge: Running-but-unjoined right now (any age)
}

var metrics = &providerMetrics{}

func (m *providerMetrics) setGauges(owned, unjoined int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.owned, m.unjoined = int64(owned), int64(unjoined)
}

func (m *providerMetrics) render() string {
	m.mu.Lock()
	owned, unjoined := m.owned, m.unjoined
	m.mu.Unlock()
	return fmt.Sprintf(`# HELP ens_provider_orders_total RunInstances orders by outcome.
# TYPE ens_provider_orders_total counter
ens_provider_orders_total{outcome="success"} %d
ens_provider_orders_total{outcome="failure"} %d
ens_provider_orders_total{outcome="ambiguous"} %d
# HELP ens_provider_instances_released_total Successful instance releases.
# TYPE ens_provider_instances_released_total counter
ens_provider_instances_released_total %d
# HELP ens_provider_release_failures_total Failed instance releases.
# TYPE ens_provider_release_failures_total counter
ens_provider_release_failures_total %d
# HELP ens_provider_reservations_expired_total Order reservations expired without becoming visible.
# TYPE ens_provider_reservations_expired_total counter
ens_provider_reservations_expired_total %d
# HELP ens_provider_owned_instances Instances currently owned (all groups).
# TYPE ens_provider_owned_instances gauge
ens_provider_owned_instances %d
# HELP ens_provider_unjoined_instances Running-but-unjoined instances right now.
# TYPE ens_provider_unjoined_instances gauge
ens_provider_unjoined_instances %d
# HELP ens_provider_is_leader 1 while this process holds the leadership Lease.
# TYPE ens_provider_is_leader gauge
ens_provider_is_leader %d
# HELP ens_provider_routing_published 1 while the leader label is confirmed on this pod.
# TYPE ens_provider_routing_published gauge
ens_provider_routing_published %d
# HELP ens_provider_leadership_transitions_total Leadership acquisitions by this process.
# TYPE ens_provider_leadership_transitions_total counter
ens_provider_leadership_transitions_total %d
`,
		m.ordersSuccess.Load(), m.ordersFailure.Load(), m.ordersAmbiguous.Load(),
		m.released.Load(), m.releaseFailures.Load(), m.reservationsExpired.Load(),
		owned, unjoined,
		m.isLeader.Load(), m.routingPublished.Load(), m.leadershipTransitions.Load())
}

func serveMetrics(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, metrics.render())
	})
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("metrics listener %s: %v", addr, err)
	}
}
