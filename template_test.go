package main

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

// Issue 4: every node is capped at 64 pods by the /26 node CIDR. The provider's
// shared default must advertise 64, so web/system/camera (all
// ens.sn1.small == default, NO template) inherit 64. The stream group IS a
// partial template measured on the live cluster (five measured sn1.tiny fields, `pods` inherited),
// so BuildTemplateNode's field-wise merge is a live code path. The test below
// exercises that merge with a DISTINCT value (32 vs the 64 default) so it would
// fail if the override path were broken (C-014).

// A default-type group with NO template inherits the corrected shared default:
// both Capacity.Pods and Allocatable.Pods must be 64.
func TestTemplateDefaultPods64(t *testing.T) {
	cfg := loadTestConfig(t) // no TMPL_PODS env -> DefTemplate.Pods == Go default
	g := cfg.GroupByID("np-a") // web: Template == nil
	if g.Template != nil {
		t.Fatalf("test group should have no template")
	}
	n := BuildTemplateNode(cfg, g)
	if got := n.Status.Capacity.Pods().String(); got != "64" {
		t.Errorf("default Capacity.Pods = %s, want 64", got)
	}
	if got := n.Status.Allocatable.Pods().String(); got != "64" {
		t.Errorf("default Allocatable.Pods = %s, want 64", got)
	}
}

// A PARTIAL template with a DISTINCT value (32, not the 64 default) proves the
// override path actually runs: Capacity.Pods and Allocatable.Pods become 32,
// while cpu/memory/ephemeral still inherit the shared default (C-014).
func TestTemplatePartialOverrideDistinctValue(t *testing.T) {
	cfg := loadTestConfig(t)
	def := cfg.DefTemplate

	g := &Group{
		ID:         "partial",
		NamePrefix: "edge-partial",
		NodepoolID: "np-partial",
		Template:   &Template{Pods: "32"}, // partial: ONLY pods set
	}
	n := BuildTemplateNode(cfg, g)

	// pods overridden to the distinct value, on BOTH capacity and allocatable
	if got := n.Status.Capacity.Pods().String(); got != "32" {
		t.Errorf("partial Capacity.Pods = %s, want 32", got)
	}
	if got := n.Status.Allocatable.Pods().String(); got != "32" {
		t.Errorf("partial Allocatable.Pods = %s, want 32", got)
	}
	// everything else inherits the shared default (proves partial, not replace)
	if got := n.Status.Capacity.Cpu().String(); got != def.CPU {
		t.Errorf("partial Capacity.Cpu = %s, want inherited %s", got, def.CPU)
	}
	if got := n.Status.Capacity.Memory().String(); got != mustQty(def.Memory) {
		t.Errorf("partial Capacity.Memory = %s, want inherited %s", got, def.Memory)
	}
	if got := n.Status.Capacity.StorageEphemeral().String(); got != mustQty(def.Ephemeral) {
		t.Errorf("partial Capacity.Ephemeral = %s, want inherited %s", got, def.Ephemeral)
	}
	if got := n.Status.Allocatable.Cpu().String(); got != def.CPUAlloc {
		t.Errorf("partial Allocatable.Cpu = %s, want inherited %s", got, def.CPUAlloc)
	}
	if got := n.Status.Allocatable.Memory().String(); got != mustQty(def.MemAlloc) {
		t.Errorf("partial Allocatable.Memory = %s, want inherited %s", got, def.MemAlloc)
	}
}

// mustQty normalizes a quantity string the way BuildTemplateNode does (parse ->
// String), so inherited values compare equal regardless of formatting.
func mustQty(s string) string {
	q := resource.MustParse(s)
	return q.String()
}
