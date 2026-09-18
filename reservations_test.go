package main

// Reservation-layer tests (v0.1.22): the guard property the design review
// demanded be exercised for real - concurrent orders against a deliberately
// stale inventory - plus the full reservation lifecycle table.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	util "github.com/alibabacloud-go/tea-utils/v2/service"
)

func ownedN(n int, prefix string) []Instance {
	var out []Instance
	for i := 0; i < n; i++ {
		out = append(out, Instance{
			ID:      fmt.Sprintf("i-%s-%d", prefix, i),
			Name:    fmt.Sprintf("%s-static-%d", prefix, i),
			GroupID: prefix, Status: "Running",
		})
	}
	return out
}

var t0 = time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)

// The race v0.1.21 could not close: two orders back to back while the
// inventory stays stale at 11 (ENS Describe-visibility lag). Exactly one
// may reach ENS; the second must see the first's reservation and refuse.
func TestConcurrentOrdersRespectMaxDuringDescribeLag(t *testing.T) {
	state := newGroupOrderState()
	inventory := func() ([]Instance, error) { return ownedN(11, "g"), nil } // deliberately never advances

	var submits atomic.Int32
	start := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		opName := fmt.Sprintf("g-op%02d-1", i)
		go func() {
			<-start
			_, err := executeGuardedOrder(context.Background(), state, "g", opName, 1, 12, t0, inventory,
				func(_ *util.RuntimeOptions) ([]string, orderOutcome, error) {
					submits.Add(1)
					return []string{"i-new"}, orderAccepted, nil
				})
			results <- err
		}()
	}
	close(start)
	err1, err2 := <-results, <-results

	if submits.Load() != 1 {
		t.Fatalf("want exactly one ENS order, got %d", submits.Load())
	}
	ok := (err1 == nil && errors.Is(err2, ErrExceedsMax)) ||
		(err2 == nil && errors.Is(err1, ErrExceedsMax))
	if !ok {
		t.Fatalf("want one success + one ErrExceedsMax, got %v / %v", err1, err2)
	}
	// the winner's reservation holds the headroom: effective must be 12
	state.mu.Lock()
	defer state.mu.Unlock()
	if eff := state.effectiveOwned("g", ownedN(11, "g"), t0); eff != 12 {
		t.Fatalf("effective owned after the race: want 12, got %d", eff)
	}
}

func TestReservationClearedWhenFullyVisible(t *testing.T) {
	state := newGroupOrderState()
	state.reservations["g-opaa-1"] = &orderReservation{expected: 2, created: t0}
	owned := append(ownedN(10, "g"),
		Instance{ID: "i-a", Name: "g-opaa-1", Status: "Running"},
		Instance{ID: "i-b", Name: "g-opaa-1-uniq2", Status: "Running"}) // ENS UniqueSuffix sibling
	if eff := state.effectiveOwned("g", owned, t0); eff != 12 {
		t.Fatalf("fully visible op must not double-count: want 12, got %d", eff)
	}
	if len(state.reservations) != 0 {
		t.Fatalf("fully visible reservation must be deleted")
	}
}

func TestPartialVisibilityReservesOnlyRemainder(t *testing.T) {
	state := newGroupOrderState()
	r := &orderReservation{expected: 3, created: t0, misses: 1}
	state.reservations["g-opbb-1"] = r
	owned := append(ownedN(5, "g"), Instance{ID: "i-a", Name: "g-opbb-1", Status: "Running"})
	if eff := state.effectiveOwned("g", owned, t0); eff != 8 { // 6 visible + 2 outstanding
		t.Fatalf("want 6 visible + 2 outstanding = 8, got %d", eff)
	}
	if r.misses != 0 {
		t.Fatalf("partial visibility is progress - misses must reset, got %d", r.misses)
	}
}

func TestDefiniteRejectionReleasesReservation(t *testing.T) {
	state := newGroupOrderState()
	boom := errors.New("RunInstances: InvalidParameter")
	_, err := executeGuardedOrder(context.Background(), state, "g", "g-opcc-1", 1, 12, t0,
		func() ([]Instance, error) { return ownedN(11, "g"), nil },
		func(_ *util.RuntimeOptions) ([]string, orderOutcome, error) { return nil, orderRejected, boom })
	if !errors.Is(err, boom) {
		t.Fatalf("submit error must propagate, got %v", err)
	}
	if len(state.reservations) != 0 {
		t.Fatalf("rejected order must not hold headroom")
	}
	// the freed headroom is immediately usable
	if _, err := executeGuardedOrder(context.Background(), state, "g", "g-opdd-1", 1, 12, t0,
		func() ([]Instance, error) { return ownedN(11, "g"), nil },
		func(_ *util.RuntimeOptions) ([]string, orderOutcome, error) {
			return []string{"i-new"}, orderAccepted, nil
		},
	); err != nil {
		t.Fatalf("headroom freed by rejection must be orderable: %v", err)
	}
}

func TestUncertainOutcomeRetainsReservation(t *testing.T) {
	state := newGroupOrderState()
	timeout := errors.New("RunInstances ambiguous, nothing appeared for op g-opee-1 yet: context deadline exceeded")
	_, err := executeGuardedOrder(context.Background(), state, "g", "g-opee-1", 1, 12, t0,
		func() ([]Instance, error) { return ownedN(11, "g"), nil },
		func(_ *util.RuntimeOptions) ([]string, orderOutcome, error) { return nil, orderUncertain, timeout })
	if err == nil {
		t.Fatalf("uncertain submit must still report its error to CA")
	}
	if len(state.reservations) != 1 {
		t.Fatalf("uncertain order MUST hold its reservation (may have landed)")
	}
	// the held reservation blocks the next order at the ceiling:
	// under-scaling is the accepted failure direction, never over-scaling.
	_, err = executeGuardedOrder(context.Background(), state, "g", "g-opff-1", 1, 12, t0,
		func() ([]Instance, error) { return ownedN(11, "g"), nil },
		func(_ *util.RuntimeOptions) ([]string, orderOutcome, error) {
			return []string{"i-new"}, orderAccepted, nil
		})
	if !errors.Is(err, ErrExceedsMax) {
		t.Fatalf("headroom of an uncertain order must stay spent, got %v", err)
	}
}

func TestReservationExpiryNeedsTTLAndRepeatedAbsence(t *testing.T) {
	state := newGroupOrderState()
	state.reservations["g-opgg-1"] = &orderReservation{expected: 1, created: t0}
	owned := ownedN(11, "g")
	late := t0.Add(reservationTTL + time.Minute)

	// TTL alone is not enough: first empty look (misses 1 of 2) still holds
	if eff := state.effectiveOwned("g", owned, late); eff != 12 {
		t.Fatalf("first empty look past TTL must still reserve: want 12, got %d", eff)
	}
	// second consecutive empty look past TTL -> expired, headroom restored
	if eff := state.effectiveOwned("g", owned, late); eff != 11 {
		t.Fatalf("second empty look past TTL must expire: want 11, got %d", eff)
	}
	if len(state.reservations) != 0 {
		t.Fatalf("expired reservation must be removed")
	}

	// age without absence never expires: young reservation, many empty looks
	state.reservations["g-ophh-1"] = &orderReservation{expected: 1, created: t0}
	for i := 0; i < 5; i++ {
		if eff := state.effectiveOwned("g", owned, t0.Add(time.Minute)); eff != 12 {
			t.Fatalf("young reservation must survive empty looks: want 12, got %d", eff)
		}
	}
}

func TestGroupsDoNotBlockEachOther(t *testing.T) {
	a, b := newGroupOrderState(), newGroupOrderState()
	// group A saturated by a reservation
	a.reservations["a-opii-1"] = &orderReservation{expected: 1, created: t0}
	if _, err := executeGuardedOrder(context.Background(), a, "a", "a-opjj-1", 1, 12, t0,
		func() ([]Instance, error) { return ownedN(11, "a"), nil },
		func(_ *util.RuntimeOptions) ([]string, orderOutcome, error) { return nil, orderAccepted, nil },
	); !errors.Is(err, ErrExceedsMax) {
		t.Fatalf("group a must be at ceiling, got %v", err)
	}
	// group B unaffected
	if _, err := executeGuardedOrder(context.Background(), b, "b", "b-opkk-1", 1, 12, t0,
		func() ([]Instance, error) { return ownedN(11, "b"), nil },
		func(_ *util.RuntimeOptions) ([]string, orderOutcome, error) {
			return []string{"i-new"}, orderAccepted, nil
		},
	); err != nil {
		t.Fatalf("group b must be independent: %v", err)
	}
}

// C-02 : a canceled or expired caller must never reach ENS — submit
// is not invoked, no reservation remains, and the headroom stays free for a
// live caller.
func TestGuardedOrderRefusesDeadCaller(t *testing.T) {
	state := newGroupOrderState()
	dead, cancel := context.WithCancel(context.Background())
	cancel() // already abandoned before the order
	submitted := false
	_, err := executeGuardedOrder(dead, state, "g", "g-opzz-1", 1, 12, t0,
		func() ([]Instance, error) { return ownedN(3, "g"), nil },
		func(_ *util.RuntimeOptions) ([]string, orderOutcome, error) {
			submitted = true
			return []string{"i-new"}, orderAccepted, nil
		})
	if err == nil {
		t.Fatalf("dead caller must be refused")
	}
	if !strings.Contains(err.Error(), "not submitted") {
		t.Fatalf("refusal must say the order was never submitted, got: %v", err)
	}
	if submitted {
		t.Fatalf("submit must NOT be invoked for a dead caller")
	}
	if len(state.reservations) != 0 {
		t.Fatalf("no reservation may remain after a pre-submit refusal, got %d", len(state.reservations))
	}
	// the untouched headroom is immediately usable by a live caller
	if _, err := executeGuardedOrder(context.Background(), state, "g", "g-opzy-1", 1, 12, t0,
		func() ([]Instance, error) { return ownedN(3, "g"), nil },
		func(_ *util.RuntimeOptions) ([]string, orderOutcome, error) {
			return []string{"i-new"}, orderAccepted, nil
		},
	); err != nil {
		t.Fatalf("live caller after a refused dead one must order normally: %v", err)
	}
}

// C-09: the NEAR-EXPIRY variant through the full guarded-order path — a
// deadline that is still in the FUTURE but inside the ≤500ms unusable tail.
// The 400ms budget only shrinks while the test runs, so whichever branch of
// orderBudget fires (≤500ms refusal, or ctx.Err() if scheduling ate the
// rest), the outcome is deterministic: refused, submit never invoked, no
// reservation.
func TestGuardedOrderRefusesNearExpiredDeadline(t *testing.T) {
	state := newGroupOrderState()
	near, cancel := context.WithDeadline(context.Background(), time.Now().Add(400*time.Millisecond))
	defer cancel()
	submitted := false
	_, err := executeGuardedOrder(near, state, "g", "g-opzw-1", 1, 12, t0,
		func() ([]Instance, error) { return ownedN(3, "g"), nil },
		func(_ *util.RuntimeOptions) ([]string, orderOutcome, error) {
			submitted = true
			return nil, orderAccepted, nil
		})
	if err == nil || submitted || len(state.reservations) != 0 {
		t.Fatalf("near-expired deadline: err=%v submitted=%v reservations=%d — want refusal, no submit, no reservation",
			err, submitted, len(state.reservations))
	}
}

// The expired-deadline variant of the same guarantee (deadline in the past).
func TestGuardedOrderRefusesExpiredDeadline(t *testing.T) {
	state := newGroupOrderState()
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	submitted := false
	_, err := executeGuardedOrder(expired, state, "g", "g-opzx-1", 1, 12, t0,
		func() ([]Instance, error) { return ownedN(3, "g"), nil },
		func(_ *util.RuntimeOptions) ([]string, orderOutcome, error) {
			submitted = true
			return nil, orderAccepted, nil
		})
	if err == nil || submitted || len(state.reservations) != 0 {
		t.Fatalf("expired deadline: err=%v submitted=%v reservations=%d — want refusal, no submit, no reservation",
			err, submitted, len(state.reservations))
	}
}

func TestExactFitWithReservationStillAllowed(t *testing.T) {
	state := newGroupOrderState()
	state.reservations["g-opll-1"] = &orderReservation{expected: 1, created: t0}
	// 10 visible + 1 reserved = 11; +1 fits max 12 exactly
	if _, err := executeGuardedOrder(context.Background(), state, "g", "g-opmm-1", 1, 12, t0,
		func() ([]Instance, error) { return ownedN(10, "g"), nil },
		func(_ *util.RuntimeOptions) ([]string, orderOutcome, error) {
			return []string{"i-new"}, orderAccepted, nil
		},
	); err != nil {
		t.Fatalf("exact fit including reservations must be allowed: %v", err)
	}
	// but one more is refused
	if _, err := executeGuardedOrder(context.Background(), state, "g", "g-opnn-1", 1, 12, t0,
		func() ([]Instance, error) { return ownedN(10, "g"), nil },
		func(_ *util.RuntimeOptions) ([]string, orderOutcome, error) { return nil, orderAccepted, nil },
	); !errors.Is(err, ErrExceedsMax) {
		t.Fatalf("one past exact fit must refuse, got %v", err)
	}
}
