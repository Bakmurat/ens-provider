package main

// Scale-from-zero template. When a group is at 0 nodes CA has no live node to
// template from, so it asks what a new node in THIS group would look like —
// labels, taints, capacity. This is the one place Go structurally beats the
// old Python implementation: the response field is a real k8s.io/api/core/v1
// Node, built with the canonical types instead of a hand-maintained
// wire-compatible proto subset.
//
// The labels MUST include what pending pods select on (group labels + the
// nodepool id) and the taints MUST match BuildUserdata(), or CA wrongly
// concludes a new node can't host the pod and refuses to scale from zero.

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func BuildTemplateNode(cfg *Config, g *Group) *corev1.Node {
	name := g.NamePrefix + "-template"
	labels := map[string]string{
		"alibabacloud.com/is-edge-worker":               "true",
		NodepoolLabel:                                   g.NodepoolID,
		"apps.openyurt.io/desired-nodepool":             g.NodepoolID,
		"alibabacloud.com/edge-enable-addon-flannel":    "true",
		"alibabacloud.com/edge-enable-addon-kube-proxy": "true",
		"alibabacloud.com/edge-enable-addon-coredns":    "true",
		"kubernetes.io/os":                              "linux",
		"kubernetes.io/arch":                            "amd64",
		"beta.kubernetes.io/os":                         "linux",
		"beta.kubernetes.io/arch":                       "amd64",
		"kubernetes.io/hostname":                        name, // CA sanitizes to a unique name per simulated node
	}
	for k, v := range g.Labels {
		labels[k] = v
	}

	taints := make([]corev1.Taint, 0, len(g.Taints)) // empty honored as-is
	for _, t := range g.Taints {
		eff := t.Effect
		if eff == "" {
			eff = "NoSchedule"
		}
		taints = append(taints, corev1.Taint{Key: t.Key, Value: t.Value, Effect: corev1.TaintEffect(eff)})
	}

	tmpl := cfg.DefTemplate
	if g.Template != nil {
		if g.Template.CPU != "" {
			tmpl.CPU = g.Template.CPU
		}
		if g.Template.Memory != "" {
			tmpl.Memory = g.Template.Memory
		}
		if g.Template.Pods != "" {
			tmpl.Pods = g.Template.Pods
		}
		if g.Template.Ephemeral != "" {
			tmpl.Ephemeral = g.Template.Ephemeral
		}
		if g.Template.CPUAlloc != "" {
			tmpl.CPUAlloc = g.Template.CPUAlloc
		}
		if g.Template.MemAlloc != "" {
			tmpl.MemAlloc = g.Template.MemAlloc
		}
	}

	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Spec:       corev1.NodeSpec{Taints: taints},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse(tmpl.CPU),
				corev1.ResourceMemory:           resource.MustParse(tmpl.Memory),
				corev1.ResourcePods:             resource.MustParse(tmpl.Pods),
				corev1.ResourceEphemeralStorage: resource.MustParse(tmpl.Ephemeral),
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse(tmpl.CPUAlloc),
				corev1.ResourceMemory:           resource.MustParse(tmpl.MemAlloc),
				corev1.ResourcePods:             resource.MustParse(tmpl.Pods),
				corev1.ResourceEphemeralStorage: resource.MustParse(tmpl.Ephemeral),
			},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
}
