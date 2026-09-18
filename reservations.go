package main

// Per-group order reservations (v0.1.22, backlog item 15.3 follow-up, design
// review): the v0.1.21 under-lock guard counted a fresh
// Describe, but ENS inventory is EVENTUALLY CONSISTENT - an accepted order
// can stay Describe-invisible for a window (the same window the ambiguous
// reconciler and the N-3 drill documented). Back-to-back orders inside that
// window could both count the old inventory and overshoot the ceiling.
//
// Fix: while an order is not yet Describe-visible, it is carried as an
// in-memory RESERVATION and the guard checks
//
//	effective owned = Describe-visible owned + outstanding reservations
//
// all under the per-group order lock. GUARANTEE, precisely: concurrent
// orders handled by ONE provider process cannot exceed the group max during
// the ENS visibility window. A provider restart inside that window loses the
// reservations - closing that residual gap needs durable/shared state or a
// cloud-side idempotency key, both rejected for v1 scope (design record §12.5: no
// journal, no provider HA). Failure direction is deliberately safe: a stale
// reservation causes temporary UNDER-scaling, never over-scaling.
//
// Related residual (P0-3, 2026-08 review): DeleteNodes' already-gone check
// deliberately does NOT take this lock — reservations carry no instance IDs,
// so the lock could not correlate a delete candidate to an in-flight order
// anyway. See the found=false branch in NodeGroupDeleteNodes (server.go).

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	util "github.com/alibabacloud-go/tea-utils/v2/service"
)

// reservationTTL matches CA's maxNodeProvisionTime: if an order has produced
// nothing Describe-visible for this long, CA itself has given up on the node.
const reservationTTL = 15 * time.Minute

// reservationExpiryMisses: expiry additionally requires this many CONSECUTIVE
// uncached inventories with zero visibility - one empty Describe is not proof
// (the janitor's "absence must be proven" rule, applied here).
const reservationExpiryMisses = 2

type orderReservation struct {
	expected int32     // instances this order should materialize
	created  time.Time // for TTL
	misses   int       // consecutive uncached inventories with ZERO visibility
}

// groupOrderState serializes orders for one group (successor of the bare
// v0.1.10 group mutex) and carries the group's outstanding reservations.
type groupOrderState struct {
	mu           sync.Mutex
	reservations map[string]*orderReservation // key = operation name (the InstanceName prefix)
}

func newGroupOrderState() *groupOrderState {
	return &groupOrderState{reservations: map[string]*orderReservation{}}
}

// effectiveOwned reconciles the reservations against a FRESH (uncached)
// inventory and returns the count the ceiling guard must use. Caller holds
// s.mu. Rules per reservation:
//   - fully visible -> reservation deleted (inventory now carries it);
//   - partially visible -> only the outstanding remainder stays reserved;
//   - zero visible -> full reservation held; after reservationTTL AND
//     reservationExpiryMisses consecutive empty looks it expires LOUDLY
//     (availability restored at a bounded, logged residual risk).
func (s *groupOrderState) effectiveOwned(gid string, owned []Instance, now time.Time) int {
	effective := len(owned)
	for opName, r := range s.reservations {
		visible := int32(len(instancesWithPrefix(owned, opName)))
		if visible >= r.expected {
			delete(s.reservations, opName)
			continue
		}
		if visible == 0 {
			r.misses++
			if now.Sub(r.created) >= reservationTTL && r.misses >= reservationExpiryMisses {
				log.Printf("[ens] ALERT [%s] reservation %s (expected %d) EXPIRED after %s and %d empty inventories - releasing the headroom; if the order lands later the owned math absorbs it",
					gid, opName, r.expected, now.Sub(r.created).Round(time.Second), r.misses)
				metrics.reservationsExpired.Add(1)
				delete(s.reservations, opName)
				continue
			}
		} else {
			r.misses = 0 // progress: the op is materializing
		}
		effective += int(r.expected - visible)
	}
	return effective
}

// orderOutcome is the EXPLICIT classification of a submitted order - decided
// once, next to the raw SDK error, never re-derived from wrapped error text
// (a wrapped message would not match isAmbiguousTransport's signatures and
// would silently release a reservation that must be held).
type orderOutcome int

const (
	orderRejected  orderOutcome = iota // ENS definitely did not accept - nothing to absorb
	orderAccepted                      // ENS confirmed (or reconciliation found every instance)
	orderUncertain                     // may have been accepted (ambiguous transport / partial visibility)
)

// executeGuardedOrder is the concurrency-correct core of RunInstances,
// extracted with injectable inventory/submit so the guard is testable under
// -race (the janitor's fake-boundary precedent). Sequence, all under the
// group's order lock:
//
//	fresh inventory -> reconcile reservations -> ceiling guard on the
//	effective count -> caller-budget gate (refuse-or-budget, atomic) ->
//	reserve -> submit WITH the gate's budget -> keep/release the
//	reservation by the submit outcome.
//
// The reservation is created BEFORE the ENS call: from that point no other
// order in this process can spend the same headroom, whatever the call
// returns. It is released only on orderRejected; accepted and uncertain
// orders stay reserved until a later uncached inventory shows their
// instances (or expiry).
func executeGuardedOrder(
	ctx context.Context,
	state *groupOrderState,
	gid, opName string,
	count, max int32,
	now time.Time,
	inventory func() ([]Instance, error),
	submit func(rt *util.RuntimeOptions) ([]string, orderOutcome, error),
) ([]string, error) {
	state.mu.Lock()
	defer state.mu.Unlock()

	owned, err := inventory()
	if err != nil {
		return nil, fmt.Errorf("describe before order: %s", err) // already sanitized at the SDK boundary
	}
	effective := state.effectiveOwned(gid, owned, now)
	if err := maxGuard(gid, effective, count, max); err != nil {
		return nil, err
	}
	// C-02/C-09 : ONE atomic decision — refuse dead or nearly-dead
	// callers BEFORE any reservation or submit, or fix the submit's exact
	// time budget from the same clock sample. A refusal means no RunInstances
	// order (no mutating submit) reached ENS and no reservation was created;
	// the inventory above is a read that already happened (C-10). Decided
	// against the wall clock, not the caller-supplied `now`: the token fetch
	// and the forced inventory consumed real time the caller's deadline kept
	// ticking through.
	rt, err := orderBudget(ctx, time.Now())
	if err != nil {
		log.Printf("[ens] [%s] %v (op %s: no mutating submit reached ENS)", gid, err, opName)
		return nil, err
	}
	if reserved := effective - len(owned); reserved > 0 {
		log.Printf("[ens] [%s] ceiling check: visible %d + reserved %d -> %d of max %d",
			gid, len(owned), reserved, effective, max)
	}

	state.reservations[opName] = &orderReservation{expected: count, created: now}
	ids, outcome, err := submit(rt)
	if outcome == orderRejected {
		delete(state.reservations, opName)
	}
	return ids, err
}
