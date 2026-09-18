package main

// Alert-first failed-join janitor (v0.1.9, design plan; design record §12.5).
//
// Every reconcile tick it scans owned instances for two problem classes:
//   - Running but UNJOINED past JOIN_TIMEOUT  (join stall - the class that
//     bills forever; met once already via the imageRepoType flag)
//   - dead states (Stopped/Expired)           (PAYG disk still billing)
//
// It ALWAYS alerts (loud, greppable log lines - phase 2 turns them into
// metrics). It releases ONLY when every gate holds:
//   owned  +  past deadline (or dead state)  +  NO managed Node correlates
//   (by instance id, private IP, or name)    +  JANITOR_RELEASE=true.
// Age alone never releases (review finding, adopted): default is
// alert-only, and a node-in-progress (IP already registered, providerID not
// yet patched) is never touched.
//
// The scan itself is a PURE function over its inputs so the whole decision
// table is unit-testable without a cloud.

import (
	"context"
	"log"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

type janitorAction struct {
	InstanceID string
	Reason     string
	Release    bool // true only when all release gates held
}

func deadStatus(s string) bool { return s == "Stopped" || s == "Expired" }

// janitorScan: owned = all instances we own; joined = instance ids with a
// providerID-correlated Node; nodeKeys = every identity a managed Node exposes
// (internal/external IPs + node names) - a match means "a Node exists for
// this instance", which blocks release even with the flag on.
func janitorScan(now time.Time, cfg *Config, owned []Instance, joined map[string]struct{}, nodeKeys map[string]bool) []janitorAction {
	var out []janitorAction
	for _, inst := range owned {
		if _, ok := joined[inst.ID]; ok {
			continue // healthy correlated capacity
		}
		nodeExists := nodeKeys[inst.ID] || nodeKeys[inst.Name] ||
			(inst.PrivateIP != "" && nodeKeys[inst.PrivateIP]) ||
			(inst.PublicIP != "" && nodeKeys[inst.PublicIP])

		switch {
		case deadStatus(inst.Status):
			out = append(out, janitorAction{
				InstanceID: inst.ID,
				Reason:     "dead state " + inst.Status,
				Release:    cfg.JanitorRelease && !nodeExists,
			})
		case inst.Status == "Running":
			if inst.CreationTime == "" {
				continue // cannot age it - DescribeInstances gave no CreationTime
			}
			created, err := time.Parse(time.RFC3339, inst.CreationTime)
			if err != nil {
				continue
			}
			age := now.Sub(created)
			if age <= cfg.JoinTimeout {
				continue // still inside the join budget
			}
			out = append(out, janitorAction{
				InstanceID: inst.ID,
				Reason:     "running-but-unjoined for " + age.Round(time.Second).String(),
				Release:    cfg.JanitorRelease && !nodeExists,
			})
		}
	}
	return out
}

// ── stale Node cleanup (v0.1.19, backlog item 15.1 / acceptance-T7 finding) ─────
//
// The gap: an edge instance dying while its group sits AT floor leaves a
// NotReady Node object that nobody reaps - CA skips at-floor nodes every scan
// ("min size reached"), ECPM never acts, and above-floor unready cleanup
// (scaleDownUnreadyTime) doesn't apply. Proven live in T7: 23 minutes of
// waiting, then a manual `kubectl delete node`.
//
// Deleting a Node OBJECT is reversible (a live kubelet re-registers within a
// heartbeat), unlike releasing an instance - hence default ON where the
// release path is default OFF. Fail-closed gates, all required:
//   1. node belongs to a managed group  (ListManagedNodes label filter)
//   2. NotReady/Unknown for >= NodeCleanupGrace (line-flap debounce)
//   3. an instance id is derivable      (providerID first, name fallback)
//   4. the instance is CONFIRMED absent via an UNCACHED per-id describe -
//      a flapped-but-alive instance exists and is skipped; a describe ERROR
//      skips too (absence must be proven, never inferred from failure)
//   5. at most maxNodeCleanupPerTick deletions per tick (blast-radius cap;
//      the backlog drains across ticks)

const maxNodeCleanupPerTick = 2

type staleNode struct {
	NodeName   string
	InstanceID string
	NotReadyFor time.Duration
}

// staleNodeCandidates is the PURE decision half: which managed nodes are
// NotReady past grace and carry a derivable instance id. Existence
// confirmation (the cloud call) deliberately stays out of it.
func staleNodeCandidates(now time.Time, cfg *Config, nodes []corev1.Node) []staleNode {
	var out []staleNode
	for _, n := range nodes {
		var ready *corev1.NodeCondition
		for i := range n.Status.Conditions {
			if n.Status.Conditions[i].Type == corev1.NodeReady {
				ready = &n.Status.Conditions[i]
			}
		}
		if ready == nil || ready.Status == corev1.ConditionTrue {
			continue // healthy, or too young to even have a Ready condition
		}
		notReadyFor := now.Sub(ready.LastTransitionTime.Time)
		if notReadyFor < cfg.NodeCleanupGrace {
			continue
		}
		id := InstanceIDFromProviderID(n.Spec.ProviderID)
		if id == "" && strings.HasPrefix(n.Name, "i-") {
			id = n.Name // ENS edge nodes register with name == instance id
		}
		if id == "" {
			continue // nothing to confirm against - leave it to a human
		}
		out = append(out, staleNode{NodeName: n.Name, InstanceID: id, NotReadyFor: notReadyFor})
	}
	return out
}

// narrow interfaces so the wiring is testable without a cluster or a cloud
type instanceGetter interface {
	InstanceByID(instanceID string) (Instance, bool, error)
}
type nodeObjectDeleter interface {
	DeleteNodeObject(ctx context.Context, name string) error
}

// runNodeCleanup confirms each candidate against the cloud (uncached) and
// deletes the Node objects of confirmed-gone instances, capped per tick.
func runNodeCleanup(ctx context.Context, now time.Time, cfg *Config, cloud instanceGetter, kube nodeObjectDeleter, nodes []corev1.Node) {
	if !cfg.JanitorNodeCleanup {
		return
	}
	deleted := 0
	for _, c := range staleNodeCandidates(now, cfg, nodes) {
		if deleted >= maxNodeCleanupPerTick {
			log.Printf("[janitor] node-cleanup cap (%d/tick) reached; remaining candidates wait for the next tick", maxNodeCleanupPerTick)
			return
		}
		_, exists, err := cloud.InstanceByID(c.InstanceID)
		if err != nil {
			log.Printf("[janitor] node-cleanup %s: describe %s failed, skipping (absence must be proven): %v", c.NodeName, c.InstanceID, err)
			continue
		}
		if exists {
			continue // instance alive (line flap / autonomy window) - not ours to touch
		}
		log.Printf("[janitor] ALERT node %s: NotReady %s and instance %s CONFIRMED GONE -> deleting Node object (set JANITOR_NODE_CLEANUP=false to disable)",
			c.NodeName, c.NotReadyFor.Round(time.Second), c.InstanceID)
		if err := kube.DeleteNodeObject(ctx, c.NodeName); err != nil {
			log.Printf("[janitor]   node-cleanup delete %s failed: %v", c.NodeName, err)
			continue
		}
		deleted++
	}
}

// runJanitor wires the scan into the world: gathers inputs, logs every
// finding as an ALERT, executes the releases that passed all gates.
func runJanitor(now time.Time, cfg *Config, cloud CloudAPI, joined map[string]struct{}, nodeKeys map[string]bool) {
	owned, err := cloud.AllOwnedInstances(false)
	if err != nil {
		log.Printf("[janitor] describe failed: %v", err)
		return
	}
	unjoined := 0
	for _, inst := range owned {
		if _, ok := joined[inst.ID]; !ok && inst.Status == "Running" {
			unjoined++
		}
	}
	metrics.setGauges(len(owned), unjoined)
	for _, a := range janitorScan(now, cfg, owned, joined, nodeKeys) {
		if a.Release {
			log.Printf("[janitor] ALERT %s: %s -> RELEASING (JANITOR_RELEASE=true, no Node correlates)", a.InstanceID, a.Reason)
			if err := cloud.ReleaseInstance(a.InstanceID); err != nil {
				log.Printf("[janitor]   release %s failed: %v", a.InstanceID, err)
			}
		} else {
			log.Printf("[janitor] ALERT %s: %s (alert-only: set JANITOR_RELEASE=true to auto-release)", a.InstanceID, a.Reason)
		}
	}
}
