package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	pb "ens-provider/protos"

	"github.com/alibabacloud-go/tea/tea"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func jcfg(release bool) *Config {
	return &Config{JoinTimeout: 30 * time.Minute, JanitorRelease: release}
}

var now = time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)

func age(d time.Duration) string { return now.Add(-d).Format(time.RFC3339) }

func TestJanitorHealthyJoinedIgnored(t *testing.T) {
	owned := []Instance{{ID: "i-1", Status: "Running", CreationTime: age(2 * time.Hour)}}
	acts := janitorScan(now, jcfg(true), owned, map[string]struct{}{"i-1": {}}, nil)
	if len(acts) != 0 {
		t.Fatalf("joined instance must never be flagged: %v", acts)
	}
}

func TestJanitorYoungUnjoinedIgnored(t *testing.T) {
	owned := []Instance{{ID: "i-1", Status: "Running", CreationTime: age(5 * time.Minute)}}
	acts := janitorScan(now, jcfg(true), owned, nil, nil)
	if len(acts) != 0 {
		t.Fatalf("inside the join budget must not alert: %v", acts)
	}
}

func TestJanitorOldUnjoinedAlertsOnlyByDefault(t *testing.T) {
	owned := []Instance{{ID: "i-1", Status: "Running", CreationTime: age(45 * time.Minute)}}
	acts := janitorScan(now, jcfg(false), owned, nil, nil)
	if len(acts) != 1 || acts[0].Release {
		t.Fatalf("flag off => alert-only, got %v", acts)
	}
}

func TestJanitorOldUnjoinedReleasesWithFlag(t *testing.T) {
	owned := []Instance{{ID: "i-1", Status: "Running", CreationTime: age(45 * time.Minute)}}
	acts := janitorScan(now, jcfg(true), owned, nil, nil)
	if len(acts) != 1 || !acts[0].Release {
		t.Fatalf("flag on + no node => release, got %v", acts)
	}
}

func TestJanitorNodeExistsBlocksRelease(t *testing.T) {
	owned := []Instance{{ID: "i-1", Status: "Running", PrivateIP: "10.0.0.9", CreationTime: age(45 * time.Minute)}}
	nodeKeys := map[string]bool{"10.0.0.9": true} // a Node with that IP registered
	acts := janitorScan(now, jcfg(true), owned, nil, nodeKeys)
	if len(acts) != 1 || acts[0].Release {
		t.Fatalf("an existing Node must block release even with the flag on: %v", acts)
	}
}

func TestJanitorDeadStates(t *testing.T) {
	owned := []Instance{
		{ID: "i-stopped", Status: "Stopped"},
		{ID: "i-expired", Status: "Expired"},
	}
	acts := janitorScan(now, jcfg(true), owned, nil, nil)
	if len(acts) != 2 {
		t.Fatalf("dead states must be flagged: %v", acts)
	}
	for _, a := range acts {
		if !a.Release {
			t.Fatalf("dead state + flag on + no node => release: %v", a)
		}
	}
	// flag off: alert only
	acts = janitorScan(now, jcfg(false), owned, nil, nil)
	for _, a := range acts {
		if a.Release {
			t.Fatalf("flag off must never release: %v", a)
		}
	}
}

func TestJanitorNoCreationTimeSkipped(t *testing.T) {
	owned := []Instance{{ID: "i-1", Status: "Running", CreationTime: ""}}
	if acts := janitorScan(now, jcfg(true), owned, nil, nil); len(acts) != 0 {
		t.Fatalf("un-ageable instance must be skipped: %v", acts)
	}
}

// ---- plan 1.4: the explicit state table ----

func TestStateForTable(t *testing.T) {
	cases := map[string]pb.InstanceStatus_InstanceState{
		"Running":      pb.InstanceStatus_instanceRunning,
		"Pending":      pb.InstanceStatus_instanceCreating,
		"Starting":     pb.InstanceStatus_instanceCreating,
		"Creating":     pb.InstanceStatus_instanceCreating,
		"Building":     pb.InstanceStatus_instanceCreating,
		"Preparing":    pb.InstanceStatus_instanceCreating,
		"Stopping":     pb.InstanceStatus_instanceDeleting,
		"Releasing":    pb.InstanceStatus_instanceDeleting,
		"Deleting":     pb.InstanceStatus_instanceDeleting,
		"Stopped":      pb.InstanceStatus_instanceDeleting, // was Running before v0.1.9!
		"Expired":      pb.InstanceStatus_instanceDeleting, // was Running before v0.1.9!
		"SomethingNew": pb.InstanceStatus_instanceCreating, // unknown: NEVER running
		"":             pb.InstanceStatus_instanceCreating,
	}
	for in, want := range cases {
		if got := stateFor(in); got != want {
			t.Errorf("stateFor(%q) = %v, want %v", in, got, want)
		}
	}
}

// ---- v0.1.10 idempotency helpers (plan 1.6) ----

func TestIsAmbiguousTransport(t *testing.T) {
	amb := []string{"read tcp: i/o timeout", "context deadline exceeded", "unexpected EOF",
		"connection reset by peer", "write: broken pipe", "context canceled"}
	notAmb := []string{"OrderFailed: please try again", "InvalidParameter.Bandwidth",
		"Forbidden.RAM", "SDKError: code=InvalidInstanceType"}
	for _, m := range amb {
		if !isAmbiguousTransport(errFrom(m)) {
			t.Errorf("%q must be ambiguous", m)
		}
	}
	for _, m := range notAmb {
		if isAmbiguousTransport(errFrom(m)) {
			t.Errorf("%q must NOT be ambiguous (clean API rejection)", m)
		}
	}
	if isAmbiguousTransport(nil) {
		t.Errorf("nil is not ambiguous")
	}
}

// ---- v0.1.24 already-gone classification (backlog item 15a) ----

// sdkErr builds a *tea.SDKError exactly the way darabonba-openapi v0.2.0
// does for API rejections (client.go:584-588): code + composed message +
// the whole decoded error map as "data". Data matters: SDKError.Error()
// renders it, and the pre-v0.1.24 substring predicate matched into it.
func sdkErr(code, message string, data map[string]interface{}) error {
	obj := map[string]interface{}{"code": code, "message": message}
	if data != nil {
		obj["data"] = data
	}
	return tea.NewSDKError(obj)
}

// TestIsInstanceGone pins the strict structured-code contract: GONE iff the
// error is a *tea.SDKError whose Code is exactly InstanceIdNotFound - the
// only documented already-gone code for ReleasePostPaidInstance (doc
// 2637441). Rows 9-11 lock OUT the removed legacy substring matches so
// undocumented codes and Message/Data prose can never silently re-enter.
func TestIsInstanceGone(t *testing.T) {
	gone := sdkErr("InstanceIdNotFound",
		"code: 400, The input parameter instancdId that is not found. request id: ABC-123", nil)
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"official gone code", gone, true},
		{"gone code wrapped", fmt.Errorf("attempt 2: %w", gone), true},
		{"DependencyViolation.Snapshot", sdkErr("DependencyViolation.Snapshot",
			"code: 400, This Instance has dependent Snapshot and cannot be deleted. request id: X", nil), false},
		{"NoPermission", sdkErr("NoPermission", "code: 400, Permission denied. request id: X", nil), false},
		{"IncorrectInstanceStatus", sdkErr("IncorrectInstanceStatus",
			"code: 400, The current status of the resource does not support this operation. request id: X", nil), false},
		// The 15a regression: fatal code, misleading Message prose. The old
		// substring predicate returned true here.
		{"fatal code with gone-looking message", sdkErr("CallInterface",
			"code: 500, internal error: instance does not exist in cache layer. request id: X", nil), false},
		{"transport prose with NotFound", errFrom(`Get "https://ens.example.com/": NotFound`), false},
		{"nil", nil, false},
		// Removed legacy matches (ECS-family codes, absent from the ENS doc).
		{"legacy InvalidInstanceId code", sdkErr("InvalidInstanceId",
			"code: 400, The specified InstanceId is invalid. request id: X", nil), false},
		{"legacy InvalidInstance.NotFound code", sdkErr("InvalidInstance.NotFound",
			"code: 400, The specified instance is not found. request id: X", nil), false},
		// Fatal code whose rendered Data blob carries gone-looking prose -
		// the old predicate searched the whole Error() string, Data included.
		{"fatal code with gone-looking Data", sdkErr("IncorrectInstanceStatus",
			"code: 400, The current status of the resource does not support this operation. request id: X",
			map[string]interface{}{"Recommend": "related resource does not exist, NotFound"}), false},
	}
	for _, c := range cases {
		if got := isInstanceGone(c.err); got != c.want {
			t.Errorf("%s: isInstanceGone = %v, want %v (err: %v)", c.name, got, c.want, c.err)
		}
	}
}

func errFrom(m string) error { return &strErr{m} }

type strErr struct{ s string }

func (e *strErr) Error() string { return e.s }

func TestInstancesWithPrefix(t *testing.T) {
	all := []Instance{
		{ID: "i-1", Name: "edge-stream-op1a2b-111"},
		{ID: "i-2", Name: "edge-stream-op1a2b-111-uniq2"}, // ENS UniqueSuffix sibling
		{ID: "i-3", Name: "edge-stream-opFFFF-222"},       // different op
		{ID: "i-4", Name: "edge-web-op1a2b-111"},          // different group
	}
	got := instancesWithPrefix(all, "edge-stream-op1a2b-111")
	if len(got) != 2 || got[0] != "i-1" || got[1] != "i-2" {
		t.Fatalf("op-prefix matching wrong: %v", got)
	}
}

func TestSanitizeErrStripsQueryString(t *testing.T) {
	e := errFrom(`Get "https://ens.example.com/?AccessKeyId=STS.SECRET&Signature=SIG&UserData=BASE64TOKEN": context deadline exceeded`)
	got := sanitizeErr(e)
	for _, leak := range []string{"STS.SECRET", "Signature=SIG", "BASE64TOKEN"} {
		if strings.Contains(got, leak) {
			t.Fatalf("sanitized error still leaks %q: %s", leak, got)
		}
	}
	if !strings.Contains(got, "?REDACTED") || !strings.Contains(got, "deadline exceeded") {
		t.Fatalf("sanitizer mangled the message: %s", got)
	}
	if sanitizeErr(nil) != "" {
		t.Fatalf("nil handling")
	}
}

// TestCloudErrorWrapsAreSanitized enforces the ens.go boundary rule (15.2,
// v0.1.20): every fmt.Errorf that wraps a signed-request SDK error must go
// through sanitizeErr - a raw %w there puts the STS token, signature and
// base64 userdata (attach token) into logs the moment a transport error
// fires. Source-scan, so a re-introduced raw wrap fails the suite.
func TestCloudErrorWrapsAreSanitized(t *testing.T) {
	src, err := os.ReadFile("ens.go")
	if err != nil {
		t.Fatalf("read ens.go: %v", err)
	}
	signedOps := []string{
		"DescribeInstances", "DescribeClusterAttachScripts",
		"RunInstances", "ReleasePostPaidInstance",
	}
	for i, line := range strings.Split(string(src), "\n") {
		if !strings.Contains(line, "fmt.Errorf") || !strings.Contains(line, "err") {
			continue
		}
		for _, op := range signedOps {
			if strings.Contains(line, `"`+op) && !strings.Contains(line, "sanitizeErr") {
				t.Errorf("ens.go:%d wraps a %s error without sanitizeErr: %s",
					i+1, op, strings.TrimSpace(line))
			}
		}
	}
}

func TestMetricsRender(t *testing.T) {
	m := &providerMetrics{}
	m.ordersSuccess.Add(3)
	m.ordersFailure.Add(1)
	m.ordersAmbiguous.Add(2)
	m.released.Add(4)
	m.setGauges(5, 1)
	out := m.render()
	for _, want := range []string{
		`ens_provider_orders_total{outcome="success"} 3`,
		`ens_provider_orders_total{outcome="failure"} 1`,
		`ens_provider_orders_total{outcome="ambiguous"} 2`,
		`ens_provider_instances_released_total 4`,
		`ens_provider_release_failures_total 0`,
		`ens_provider_owned_instances 5`,
		`ens_provider_unjoined_instances 1`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, out)
		}
	}
}

// ── v0.1.19 stale-Node cleanup (backlog item 15.1 / T7 finding) ─────────────────

func cleanCfg(enabled bool) *Config {
	return &Config{JanitorNodeCleanup: enabled, NodeCleanupGrace: 5 * time.Minute,
		EnsRegion: "ens-region-1"}
}

func mkNode(name, providerID string, ready corev1.ConditionStatus, since time.Duration) corev1.Node {
	n := corev1.Node{}
	n.Name = name
	n.Spec.ProviderID = providerID
	n.Status.Conditions = []corev1.NodeCondition{{
		Type:               corev1.NodeReady,
		Status:             ready,
		LastTransitionTime: metav1.Time{Time: now.Add(-since)},
	}}
	return n
}

func TestStaleCandidatesDecisionTable(t *testing.T) {
	cfg := cleanCfg(true)
	nodes := []corev1.Node{
		mkNode("i-ready", "ens-region-1.i-ready", corev1.ConditionTrue, time.Hour),      // healthy
		mkNode("i-young", "ens-region-1.i-young", corev1.ConditionFalse, time.Minute),   // inside grace
		mkNode("i-gone", "ens-region-1.i-gone", corev1.ConditionFalse, 10*time.Minute),  // candidate (providerID)
		mkNode("i-byname", "", corev1.ConditionUnknown, 20*time.Minute),                   // candidate (name fallback)
		mkNode("cloud-node-1", "", corev1.ConditionFalse, time.Hour),                      // no derivable id -> skip
	}
	// node with NO Ready condition at all (just registered) must be skipped
	blank := corev1.Node{}
	blank.Name = "i-blank"
	nodes = append(nodes, blank)

	got := staleNodeCandidates(now, cfg, nodes)
	if len(got) != 2 {
		t.Fatalf("want exactly 2 candidates, got %+v", got)
	}
	if got[0].InstanceID != "i-gone" || got[1].InstanceID != "i-byname" {
		t.Fatalf("wrong candidates/order: %+v", got)
	}
	if got[1].NodeName != "i-byname" {
		t.Fatalf("name-fallback candidate must keep node name: %+v", got[1])
	}
}

type fakeGetter struct {
	exists map[string]bool
	errs   map[string]error
	calls  []string
}

func (f *fakeGetter) InstanceByID(id string) (Instance, bool, error) {
	f.calls = append(f.calls, id)
	if err := f.errs[id]; err != nil {
		return Instance{}, false, err
	}
	return Instance{ID: id}, f.exists[id], nil
}

type fakeDeleter struct{ deleted []string }

func (f *fakeDeleter) DeleteNodeObject(_ context.Context, name string) error {
	f.deleted = append(f.deleted, name)
	return nil
}

func TestNodeCleanupOnlyConfirmedGone(t *testing.T) {
	cfg := cleanCfg(true)
	nodes := []corev1.Node{
		mkNode("i-alive", "ens-region-1.i-alive", corev1.ConditionFalse, time.Hour), // flapped, instance EXISTS
		mkNode("i-dead", "ens-region-1.i-dead", corev1.ConditionFalse, time.Hour),   // instance gone
		mkNode("i-err", "ens-region-1.i-err", corev1.ConditionFalse, time.Hour),     // describe fails
	}
	cloud := &fakeGetter{
		exists: map[string]bool{"i-alive": true, "i-dead": false},
		errs:   map[string]error{"i-err": context.DeadlineExceeded},
	}
	kube := &fakeDeleter{}
	runNodeCleanup(context.Background(), now, cfg, cloud, kube, nodes)
	if len(kube.deleted) != 1 || kube.deleted[0] != "i-dead" {
		t.Fatalf("must delete exactly the confirmed-gone node, got %v", kube.deleted)
	}
}

func TestNodeCleanupDisabledDoesNothing(t *testing.T) {
	cfg := cleanCfg(false)
	nodes := []corev1.Node{mkNode("i-dead", "ens-region-1.i-dead", corev1.ConditionFalse, time.Hour)}
	cloud := &fakeGetter{exists: map[string]bool{}}
	kube := &fakeDeleter{}
	runNodeCleanup(context.Background(), now, cfg, cloud, kube, nodes)
	if len(cloud.calls) != 0 || len(kube.deleted) != 0 {
		t.Fatalf("disabled cleanup must not touch cloud or kube: calls=%v deleted=%v", cloud.calls, kube.deleted)
	}
}

func TestNodeCleanupPerTickCap(t *testing.T) {
	cfg := cleanCfg(true)
	var nodes []corev1.Node
	for _, id := range []string{"i-d1", "i-d2", "i-d3", "i-d4"} {
		nodes = append(nodes, mkNode(id, "ens-region-1."+id, corev1.ConditionFalse, time.Hour))
	}
	cloud := &fakeGetter{exists: map[string]bool{}} // all gone
	kube := &fakeDeleter{}
	runNodeCleanup(context.Background(), now, cfg, cloud, kube, nodes)
	if len(kube.deleted) != maxNodeCleanupPerTick {
		t.Fatalf("cap must hold: deleted %d, want %d", len(kube.deleted), maxNodeCleanupPerTick)
	}
}
