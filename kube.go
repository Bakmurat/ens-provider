package main

// Kubernetes glue (client-go): managed-node listing, the providerID
// reconciler, and Node-object deletion on scale-down.
//
// providerID is the linchpin: ENS edge nodes register with an EMPTY
// spec.providerID and CA can't correlate a Node to its cloud instance without
// one. K8s allows patching empty -> value, so we set <ens-region>.<instance-id>,
// matching node to instance by PRIVATE IP first (edge workers have no public
// IP), then public IP, then instance name == node name.

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Kube struct {
	cfg    *Config
	client kubernetes.Interface
}

// timeNow is indirected for deterministic lease-release tests.
var timeNow = time.Now

func NewKube(cfg *Config) (*Kube, error) {
	var rc *rest.Config
	var err error
	if kc := os.Getenv("KUBECONFIG"); kc != "" {
		rc, err = clientcmd.BuildConfigFromFlags("", kc)
	} else if home, _ := os.UserHomeDir(); home != "" && fileExists(filepath.Join(home, ".kube", "config")) {
		rc, err = clientcmd.BuildConfigFromFlags("", filepath.Join(home, ".kube", "config"))
	} else {
		rc, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("kube config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("kube client: %w", err)
	}
	return &Kube{cfg: cfg, client: cs}, nil
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// Ping is the readiness reachability check (v0.1.16, Issue 3): a bounded
// Nodes().List with Limit:1 proves API transport + credentials + the exact
// nodes:list authorization the provider already relies on, in one cheap call.
// A /readyz probe would prove API health but add a separate non-resource-URL
// authorization surface without proving node-list permission. The caller bounds
// this with its own context timeout.
func (k *Kube) Ping(ctx context.Context) error {
	_, err := k.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 1})
	return err
}

// ListManagedNodes returns all Nodes across ALL our edge nodepools.
func (k *Kube) ListManagedNodes(ctx context.Context) ([]corev1.Node, error) {
	ids := map[string]struct{}{}
	for _, g := range k.cfg.Groups {
		ids[g.NodepoolID] = struct{}{}
	}
	list := make([]string, 0, len(ids))
	for id := range ids {
		list = append(list, id)
	}
	sort.Strings(list)
	sel := fmt.Sprintf("%s in (%s)", NodepoolLabel, strings.Join(list, ","))
	nodes, err := k.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		return nil, err
	}
	return nodes.Items, nil
}

// JoinedInstanceIDs returns the ENS instance ids that already have a
// correlated kube Node (providerID patched). v0.1.12 (design plan): a kube API
// error is RETURNED, not swallowed - the old empty-set-on-error made every
// owned instance look 'creating' for that RPC (silently wrong data beats no
// data never).
func (k *Kube) JoinedInstanceIDs(ctx context.Context) (map[string]struct{}, error) {
	ids := map[string]struct{}{}
	nodes, err := k.ListManagedNodes(ctx)
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		if iid := InstanceIDFromProviderID(n.Spec.ProviderID); iid != "" {
			ids[iid] = struct{}{}
		}
	}
	return ids, nil
}

// ManagedNodeKeys returns every identity a managed Node exposes (name +
// instance id from providerID + all addresses) - the janitor's
// does-a-Node-exist gate.
func (k *Kube) ManagedNodeKeys(ctx context.Context) (map[string]bool, error) {
	nodes, err := k.ListManagedNodes(ctx)
	if err != nil {
		return nil, err
	}
	keys := map[string]bool{}
	for _, n := range nodes {
		keys[n.Name] = true
		if iid := InstanceIDFromProviderID(n.Spec.ProviderID); iid != "" {
			keys[iid] = true
		}
		for _, a := range n.Status.Addresses {
			if a.Address != "" {
				keys[a.Address] = true
			}
		}
	}
	return keys, nil
}

// DeleteNodeObject removes the Node object after the ENS instance is released
// (ENS edge nodes have no CCM to reap them; CA has already cordoned+drained).
// Not-found is success; any other error is RETURNED so DeleteNodes can
// aggregate it (v0.1.8 - no more swallowed failures).
func (k *Kube) DeleteNodeObject(ctx context.Context, name string) error {
	err := k.client.CoreV1().Nodes().Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		log.Printf("[kube] delete node %s: %v", name, err)
		return err
	}
	log.Printf("[kube] deleted Node object %s", name)
	return nil
}

// Clientset exposes the underlying client for the leader-election lock
// (leader.go builds the LeaseLock against CoordinationV1 directly).
func (k *Kube) Clientset() kubernetes.Interface { return k.client }

// SetPodLabel / RemovePodLabel: the leader-routing label surface (v0.1.25,
// C-14). The Service selects `ens-provider.io/role: leader`, so these
// two calls ARE endpoint membership. Merge patches; removal of an absent
// label is idempotent success.
func (k *Kube) SetPodLabel(ctx context.Context, namespace, pod, key, value string) error {
	patch := []byte(fmt.Sprintf(`{"metadata":{"labels":{%q:%q}}}`, key, value))
	_, err := k.client.CoreV1().Pods(namespace).Patch(ctx, pod, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

func (k *Kube) RemovePodLabel(ctx context.Context, namespace, pod, key string) error {
	patch := []byte(fmt.Sprintf(`{"metadata":{"labels":{%q:null}}}`, key))
	_, err := k.client.CoreV1().Pods(namespace).Patch(ctx, pod, types.MergePatchType, patch, metav1.PatchOptions{})
	if apierrors.IsNotFound(err) {
		return nil // pod object gone: nothing selects it either
	}
	return err
}

// ClearLeaseHolder is the OWNER-CHECKED fast lease release (C-13/C-15): GET the Lease, verify holderIdentity is OURS, then clear it via
// the fetched resourceVersion (optimistic concurrency — a concurrent acquire
// by the peer makes the Update fail, never overwrites). Mirrors client-go's
// release() record shape (LeaseDurationSeconds=1) WITHOUT its unconditional
// semantics. Callers only invoke this AFTER the election goroutine is joined,
// so our own renewer can never race the clear (C-15).
func (k *Kube) ClearLeaseHolder(ctx context.Context, namespace, name, identity string) error {
	lease, err := k.client.CoordinationV1().Leases(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get lease %s/%s: %w", namespace, name, err)
	}
	holder := ""
	if lease.Spec.HolderIdentity != nil {
		holder = *lease.Spec.HolderIdentity
	}
	if holder != identity {
		return fmt.Errorf("lease %s/%s held by %q, not us (%q) - leaving untouched", namespace, name, holder, identity)
	}
	empty := ""
	one := int32(1)
	now := metav1.NewMicroTime(timeNow())
	lease.Spec.HolderIdentity = &empty
	lease.Spec.LeaseDurationSeconds = &one
	lease.Spec.RenewTime = &now
	lease.Spec.AcquireTime = &now
	if _, err := k.client.CoordinationV1().Leases(namespace).Update(ctx, lease, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("clear lease %s/%s: %w", namespace, name, err)
	}
	return nil
}

// autonomy annotation constants (official-docs audit 2026-07-14): autonomy is
// OFF by default; without it a >5-min cloud-edge disconnect evicts every edge
// pod. It is a per-node ANNOTATION (pool labels cannot carry it), so the
// reconciler stamps it here on every managed node, idempotently.
const (
	autonomyDurationAnno = "node.alibabacloud.com/autonomy-duration" // bounded, K8s >= 1.28
	autonomyLegacyAnno   = "node.beta.openyurt.io/autonomy"          // unbounded
)

// ensureAutonomy stamps the configured autonomy annotation on nodes that lack it.
func (k *Kube) ensureAutonomy(ctx context.Context, nodes []corev1.Node) {
	mode := strings.TrimSpace(k.cfg.AutonomyDuration)
	if mode == "" || strings.EqualFold(mode, "off") {
		return
	}
	anno, val := autonomyDurationAnno, mode
	if strings.EqualFold(mode, "unbounded") || strings.EqualFold(mode, "true") {
		anno, val = autonomyLegacyAnno, "true"
	}
	for _, n := range nodes {
		if n.Annotations[anno] == val {
			continue
		}
		patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, anno, val))
		if _, err := k.client.CoreV1().Nodes().Patch(ctx, n.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
			log.Printf("[kube] autonomy patch %s failed: %v", n.Name, err)
			continue
		}
		log.Printf("[kube] autonomy set %s -> %s=%s", n.Name, anno, val)
	}
}

// InstanceLister is the slice of CloudAPI the reconciler needs (interface so
// tests can fake it).
type InstanceLister interface {
	AllOwnedInstances(force bool) ([]Instance, error)
}

// ReconcileProviderIDs patches empty providerIDs on managed nodes and ensures
// the edge-autonomy annotation on all of them.
func (k *Kube) ReconcileProviderIDs(ctx context.Context, cloud InstanceLister) (int, error) {
	nodes, err := k.ListManagedNodes(ctx)
	if err != nil {
		return 0, err
	}
	k.ensureAutonomy(ctx, nodes)
	var need []corev1.Node
	for _, n := range nodes {
		if n.Spec.ProviderID == "" {
			need = append(need, n)
		}
	}
	if len(need) == 0 {
		return 0, nil
	}
	owned, err := cloud.AllOwnedInstances(true)
	if err != nil {
		return 0, err
	}
	byIP := map[string]*Instance{}
	byName := map[string]*Instance{}
	for i := range owned {
		o := &owned[i]
		if o.PublicIP != "" {
			byIP[o.PublicIP] = o
		}
		if o.PrivateIP != "" {
			byIP[o.PrivateIP] = o // private-first world: edge workers have no public IP
		}
		byName[o.Name] = o
	}
	patched := 0
	for _, n := range need {
		addrs := map[corev1.NodeAddressType]string{}
		for _, a := range n.Status.Addresses {
			addrs[a.Type] = a.Address
		}
		inst := byIP[addrs[corev1.NodeInternalIP]]
		if inst == nil {
			inst = byIP[addrs[corev1.NodeExternalIP]]
		}
		if inst == nil {
			inst = byName[n.Name]
		}
		if inst == nil {
			log.Printf("[kube] providerID: no ENS match yet for node %s (%v)", n.Name, addrs)
			continue
		}
		pid := k.cfg.ProviderID(inst.ID)
		patch := []byte(fmt.Sprintf(`{"spec":{"providerID":%q}}`, pid))
		if _, err := k.client.CoreV1().Nodes().Patch(ctx, n.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
			log.Printf("[kube] providerID patch %s failed: %v", n.Name, err)
			continue
		}
		log.Printf("[kube] providerID set %s -> %s", n.Name, pid)
		patched++
	}
	return patched, nil
}
