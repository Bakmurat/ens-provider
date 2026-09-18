package main

// Fake-based RPC tests (design plan). The fakes implement CloudAPI/KubeAPI so
// the Server state machine is exercised without Alibaba or Kubernetes.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "ens-provider/protos"
)

type fakeCloud struct {
	instances       []Instance
	releaseErr      map[string]error // per-instance release outcome (nil = ok)
	released        []string
	runCalls        int
	runErr          error
	describeErr     error
	instanceByID    map[string]Instance
	instanceByIDErr error
	lookupCalls     int
}

func (f *fakeCloud) OwnedInstances(g *Group, force bool) ([]Instance, error) {
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	var out []Instance
	for _, i := range f.instances {
		if i.GroupID == g.ID {
			out = append(out, i)
		}
	}
	return out, nil
}

func (f *fakeCloud) AllOwnedInstances(force bool) ([]Instance, error) {
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	return f.instances, nil
}

func (f *fakeCloud) InstanceByID(id string) (Instance, bool, error) {
	f.lookupCalls++
	if f.instanceByIDErr != nil {
		return Instance{}, false, f.instanceByIDErr
	}
	if f.instanceByID != nil {
		instance, found := f.instanceByID[id]
		return instance, found, nil
	}
	for _, instance := range f.instances {
		if instance.ID == id {
			return instance, true, nil
		}
	}
	return Instance{}, false, nil
}

func (f *fakeCloud) RunInstances(ctx context.Context, g *Group, count int32) ([]string, error) {
	f.runCalls++
	if f.runErr != nil {
		return nil, f.runErr
	}
	var ids []string
	for i := int32(0); i < count; i++ {
		ids = append(ids, "i-new")
	}
	return ids, nil
}

func (f *fakeCloud) ReleaseInstance(id string) error {
	if err, ok := f.releaseErr[id]; ok && err != nil {
		return err
	}
	f.released = append(f.released, id)
	return nil
}

func (f *fakeCloud) InvalidateCache() {}

type fakeKube struct {
	joined     map[string]struct{}
	joinedErr  error
	deleteErr  map[string]error
	deleted    []string
	reconciles int
}

func (f *fakeKube) JoinedInstanceIDs(ctx context.Context) (map[string]struct{}, error) {
	return f.joined, f.joinedErr
}

func (f *fakeKube) DeleteNodeObject(ctx context.Context, name string) error {
	if err, ok := f.deleteErr[name]; ok && err != nil {
		return err
	}
	f.deleted = append(f.deleted, name)
	return nil
}

func (f *fakeKube) ReconcileProviderIDs(ctx context.Context, cloud InstanceLister) (int, error) {
	f.reconciles++
	return 0, nil
}

func newTestServer(t *testing.T, fc *fakeCloud, fk *fakeKube) *Server {
	t.Helper()
	cfg := loadTestConfig(t) // groups np-a (web, [6,12]) + np-b (system, [3,5])
	return NewServer(cfg, fc, fk)
}

func pid(iid string) string { return "ens-region-1." + iid }

// ---- DeleteNodes (v0.1.8 transactional semantics) ----

func TestDeleteNodesHappyPath(t *testing.T) {
	fc := &fakeCloud{instances: []Instance{{ID: "i-1", GroupID: "np-a", Status: "Running"}}}
	fk := &fakeKube{}
	s := newTestServer(t, fc, fk)
	_, err := s.NodeGroupDeleteNodes(context.Background(), &pb.NodeGroupDeleteNodesRequest{
		Id: "np-a", Nodes: []*pb.ExternalGrpcNode{{Name: "i-1", ProviderID: pid("i-1")}},
	})
	if err != nil {
		t.Fatalf("want success, got %v", err)
	}
	if len(fc.released) != 1 || fc.released[0] != "i-1" {
		t.Fatalf("release not called: %v", fc.released)
	}
	if len(fk.deleted) != 1 || fk.deleted[0] != "i-1" {
		t.Fatalf("node object not deleted: %v", fk.deleted)
	}
}

func TestDeleteNodesWrongGroupRefused(t *testing.T) {
	fc := &fakeCloud{instances: []Instance{{ID: "i-1", GroupID: "np-b", Status: "Running"}}}
	fk := &fakeKube{}
	s := newTestServer(t, fc, fk)
	_, err := s.NodeGroupDeleteNodes(context.Background(), &pb.NodeGroupDeleteNodesRequest{
		Id: "np-a", Nodes: []*pb.ExternalGrpcNode{{Name: "i-1", ProviderID: pid("i-1")}},
	})
	if err == nil {
		t.Fatalf("cross-group delete must error")
	}
	if len(fc.released) != 0 {
		t.Fatalf("release must NOT be called for a wrong-group node")
	}
	if len(fk.deleted) != 0 {
		t.Fatalf("node object must NOT be deleted for a wrong-group node")
	}
}

func TestDeleteNodesReleaseFailureKeepsNode(t *testing.T) {
	fc := &fakeCloud{
		instances:  []Instance{{ID: "i-1", GroupID: "np-a", Status: "Running"}},
		releaseErr: map[string]error{"i-1": errors.New("ENS 5xx")},
	}
	fk := &fakeKube{}
	s := newTestServer(t, fc, fk)
	_, err := s.NodeGroupDeleteNodes(context.Background(), &pb.NodeGroupDeleteNodesRequest{
		Id: "np-a", Nodes: []*pb.ExternalGrpcNode{{Name: "i-1", ProviderID: pid("i-1")}},
	})
	if err == nil {
		t.Fatalf("release failure must surface as an error")
	}
	if len(fk.deleted) != 0 {
		t.Fatalf("Node object must SURVIVE a failed release (got deletions: %v)", fk.deleted)
	}
}

func TestDeleteNodesPartialBatch(t *testing.T) {
	fc := &fakeCloud{
		instances: []Instance{
			{ID: "i-ok", GroupID: "np-a", Status: "Running"},
			{ID: "i-bad", GroupID: "np-a", Status: "Running"},
		},
		releaseErr: map[string]error{"i-bad": errors.New("throttled")},
	}
	fk := &fakeKube{}
	s := newTestServer(t, fc, fk)
	_, err := s.NodeGroupDeleteNodes(context.Background(), &pb.NodeGroupDeleteNodesRequest{
		Id: "np-a", Nodes: []*pb.ExternalGrpcNode{
			{Name: "i-ok", ProviderID: pid("i-ok")},
			{Name: "i-bad", ProviderID: pid("i-bad")},
		},
	})
	if err == nil {
		t.Fatalf("partial failure must error (never partial-success-as-success)")
	}
	if !strings.Contains(err.Error(), "1/2") {
		t.Fatalf("error should count failures: %v", err)
	}
	// the good node still fully processed (idempotent convergence on retry)
	if len(fc.released) != 1 || fc.released[0] != "i-ok" {
		t.Fatalf("good node should be released: %v", fc.released)
	}
	if len(fk.deleted) != 1 || fk.deleted[0] != "i-ok" {
		t.Fatalf("good node object should be deleted: %v", fk.deleted)
	}
}

func TestDeleteNodesAlreadyGoneInstance(t *testing.T) {
	// Instance absent from both inventories is already gone: do not call the
	// mutating release API, but clean the stale Node object.
	fc := &fakeCloud{instanceByID: map[string]Instance{}}
	fk := &fakeKube{}
	s := newTestServer(t, fc, fk)
	_, err := s.NodeGroupDeleteNodes(context.Background(), &pb.NodeGroupDeleteNodesRequest{
		Id: "np-a", Nodes: []*pb.ExternalGrpcNode{{Name: "n1", ProviderID: pid("i-ghost")}},
	})
	if err != nil {
		t.Fatalf("already-gone instance must converge: %v", err)
	}
	if len(fc.released) != 0 {
		t.Fatalf("already-gone instance must not be released: %v", fc.released)
	}
	if len(fk.deleted) != 1 {
		t.Fatalf("orphan Node object should be cleaned: %v", fk.deleted)
	}
}

func TestDeleteNodesAlreadyGoneDeleteFailureIsReported(t *testing.T) {
	fc := &fakeCloud{instanceByID: map[string]Instance{}}
	fk := &fakeKube{deleteErr: map[string]error{"n1": errors.New("apiserver unavailable")}}
	s := newTestServer(t, fc, fk)
	_, err := s.NodeGroupDeleteNodes(context.Background(), &pb.NodeGroupDeleteNodesRequest{
		Id: "np-a", Nodes: []*pb.ExternalGrpcNode{{Name: "n1", ProviderID: pid("i-gone")}},
	})
	if err == nil || !strings.Contains(err.Error(), "node object delete") {
		t.Fatalf("stale Node deletion failure must be reported, got %v", err)
	}
	if len(fc.released) != 0 {
		t.Fatalf("already-gone instance must not be released: %v", fc.released)
	}
}

func TestDeleteNodesCacheMissRequestedGroupProceeds(t *testing.T) {
	fc := &fakeCloud{instanceByID: map[string]Instance{
		"i-fresh": {ID: "i-fresh", Name: "edge-web-opabcd-1700000000", Status: "Running"},
	}}
	fk := &fakeKube{}
	s := newTestServer(t, fc, fk)
	_, err := s.NodeGroupDeleteNodes(context.Background(), &pb.NodeGroupDeleteNodesRequest{
		Id: "np-a", Nodes: []*pb.ExternalGrpcNode{{
			Name: "n1", ProviderID: pid("i-fresh"), Labels: map[string]string{NodepoolLabel: "np-a"},
		}},
	})
	if err != nil {
		t.Fatalf("exact requested-group ownership should proceed: %v", err)
	}
	if len(fc.released) != 1 || fc.released[0] != "i-fresh" {
		t.Fatalf("owned cache-miss instance should be released: %v", fc.released)
	}
	if len(fk.deleted) != 1 || fk.deleted[0] != "n1" {
		t.Fatalf("released Node should be deleted: %v", fk.deleted)
	}
}

func TestDeleteNodesCacheMissOtherManagedGroupRefused(t *testing.T) {
	fc := &fakeCloud{instanceByID: map[string]Instance{
		"i-system": {ID: "i-system", Name: "edge-system-opabcd-1700000000", Status: "Running"},
	}}
	fk := &fakeKube{}
	s := newTestServer(t, fc, fk)
	_, err := s.NodeGroupDeleteNodes(context.Background(), &pb.NodeGroupDeleteNodesRequest{
		Id: "np-a", Nodes: []*pb.ExternalGrpcNode{{Name: "n1", ProviderID: pid("i-system")}},
	})
	if err == nil {
		t.Fatalf("instance owned by another managed group must be refused")
	}
	if len(fc.released) != 0 || len(fk.deleted) != 0 {
		t.Fatalf("wrong-group cache miss changed state: released=%v deleted=%v", fc.released, fk.deleted)
	}
}

func TestDeleteNodesForeignInstanceRefused(t *testing.T) {
	fc := &fakeCloud{instanceByID: map[string]Instance{
		"i-foreign": {ID: "i-foreign", Name: "other-cluster-node", Status: "Running"},
	}}
	fk := &fakeKube{}
	s := newTestServer(t, fc, fk)
	_, err := s.NodeGroupDeleteNodes(context.Background(), &pb.NodeGroupDeleteNodesRequest{
		Id: "np-a", Nodes: []*pb.ExternalGrpcNode{{Name: "n1", ProviderID: pid("i-foreign")}},
	})
	if err == nil {
		t.Fatalf("foreign instance must be refused")
	}
	if len(fc.released) != 0 || len(fk.deleted) != 0 {
		t.Fatalf("foreign instance changed state: released=%v deleted=%v", fc.released, fk.deleted)
	}
}

func TestDeleteNodesOwnershipLookupErrorFailsClosed(t *testing.T) {
	fc := &fakeCloud{instanceByIDErr: errors.New("ENS throttled")}
	fk := &fakeKube{}
	s := newTestServer(t, fc, fk)
	_, err := s.NodeGroupDeleteNodes(context.Background(), &pb.NodeGroupDeleteNodesRequest{
		Id: "np-a", Nodes: []*pb.ExternalGrpcNode{{Name: "n1", ProviderID: pid("i-unknown")}},
	})
	if err == nil {
		t.Fatalf("ownership lookup failure must fail closed")
	}
	if len(fc.released) != 0 || len(fk.deleted) != 0 {
		t.Fatalf("lookup failure changed state: released=%v deleted=%v", fc.released, fk.deleted)
	}
}

func TestDeleteNodesMalformedProviderIDRefusedBeforeLookup(t *testing.T) {
	for _, providerID := range []string{
		"", "garbage", "cn-hangzhou.i-foreign", "ens-region-1.i-abc/def",
	} {
		fc := &fakeCloud{}
		fk := &fakeKube{}
		s := newTestServer(t, fc, fk)
		_, err := s.NodeGroupDeleteNodes(context.Background(), &pb.NodeGroupDeleteNodesRequest{
			Id: "np-a", Nodes: []*pb.ExternalGrpcNode{{Name: "n1", ProviderID: providerID}},
		})
		if err == nil {
			t.Fatalf("providerID %q must be refused", providerID)
		}
		if fc.lookupCalls != 0 || len(fc.released) != 0 || len(fk.deleted) != 0 {
			t.Fatalf("providerID %q reached stateful path: lookups=%d released=%v deleted=%v", providerID, fc.lookupCalls, fc.released, fk.deleted)
		}
	}
}

func TestDeleteNodesWrongNodepoolLabelRefusedBeforeLookup(t *testing.T) {
	fc := &fakeCloud{}
	fk := &fakeKube{}
	s := newTestServer(t, fc, fk)
	_, err := s.NodeGroupDeleteNodes(context.Background(), &pb.NodeGroupDeleteNodesRequest{
		Id: "np-a", Nodes: []*pb.ExternalGrpcNode{{
			Name: "n1", ProviderID: pid("i-system"), Labels: map[string]string{NodepoolLabel: "np-b"},
		}},
	})
	if err == nil {
		t.Fatalf("wrong nodepool label must be refused")
	}
	if fc.lookupCalls != 0 || len(fc.released) != 0 || len(fk.deleted) != 0 {
		t.Fatalf("wrong nodepool label reached stateful path: lookups=%d released=%v deleted=%v", fc.lookupCalls, fc.released, fk.deleted)
	}
}

func TestDeleteNodesNoProviderIDRefused(t *testing.T) {
	fc := &fakeCloud{}
	fk := &fakeKube{}
	s := newTestServer(t, fc, fk)
	_, err := s.NodeGroupDeleteNodes(context.Background(), &pb.NodeGroupDeleteNodesRequest{
		Id: "np-a", Nodes: []*pb.ExternalGrpcNode{{Name: "n1", ProviderID: ""}},
	})
	if err == nil {
		t.Fatalf("uncorrelatable node must be refused, never deleted blind")
	}
	if len(fk.deleted) != 0 {
		t.Fatalf("nothing should be deleted: %v", fk.deleted)
	}
}

// ---- the neighbours the fakes make cheap to cover ----

func TestTargetSizeIsOwnedCount(t *testing.T) {
	fc := &fakeCloud{instances: []Instance{
		{ID: "i-1", GroupID: "np-a", Status: "Running"},
		{ID: "i-2", GroupID: "np-a", Status: "Pending"},
		{ID: "i-3", GroupID: "np-b", Status: "Running"},
	}}
	s := newTestServer(t, fc, &fakeKube{})
	resp, err := s.NodeGroupTargetSize(context.Background(), &pb.NodeGroupTargetSizeRequest{Id: "np-a"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.TargetSize != 2 {
		t.Fatalf("target = owned: want 2, got %d", resp.TargetSize)
	}
}

func TestIncreaseSizeMaxGuard(t *testing.T) {
	var insts []Instance
	for i := 0; i < 12; i++ { // np-a max=12, already full
		insts = append(insts, Instance{ID: "i-x", GroupID: "np-a", Status: "Running"})
	}
	fc := &fakeCloud{instances: insts}
	s := newTestServer(t, fc, &fakeKube{})
	_, err := s.NodeGroupIncreaseSize(context.Background(), &pb.NodeGroupIncreaseSizeRequest{Id: "np-a", Delta: 1})
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("want OutOfRange at max, got %v", err)
	}
	if fc.runCalls != 0 {
		t.Fatalf("RunInstances must not be called past max")
	}
}

// v0.1.21 (backlog item 15.3): the authoritative ceiling decision, extracted pure.
func TestMaxGuardBoundaries(t *testing.T) {
	if err := maxGuard("g", 11, 1, 12); err != nil {
		t.Fatalf("11+1 must fit max 12: %v", err)
	}
	if err := maxGuard("g", 0, 12, 12); err != nil {
		t.Fatalf("scale-from-zero straight to max must fit: %v", err)
	}
	if err := maxGuard("g", 12, 1, 12); !errors.Is(err, ErrExceedsMax) {
		t.Fatalf("12+1 vs max 12: want ErrExceedsMax, got %v", err)
	}
	if err := maxGuard("g", 11, 2, 12); !errors.Is(err, ErrExceedsMax) {
		t.Fatalf("11+2 vs max 12: want ErrExceedsMax, got %v", err)
	}
}

// The under-lock guard fires inside cloud.RunInstances (where the server's
// cached fast-fail cannot see it); the server must surface it as OutOfRange
// - CA then backs the group off instead of counting a provider fault.
func TestIncreaseSizeUnderLockGuardMapsToOutOfRange(t *testing.T) {
	fc := &fakeCloud{
		instances: []Instance{{ID: "i-1", GroupID: "np-a", Status: "Running"}},
		runErr:    maxGuard("np-a", 12, 1, 12),
	}
	s := newTestServer(t, fc, &fakeKube{})
	_, err := s.NodeGroupIncreaseSize(context.Background(), &pb.NodeGroupIncreaseSizeRequest{Id: "np-a", Delta: 1})
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("want OutOfRange from the under-lock guard, got %v", err)
	}
}

func TestNodeGroupNodesStates(t *testing.T) {
	fc := &fakeCloud{instances: []Instance{
		{ID: "i-joined", GroupID: "np-a", Status: "Running"},
		{ID: "i-booting", GroupID: "np-a", Status: "Running"},
		{ID: "i-creating", GroupID: "np-a", Status: "Creating"},
	}}
	fk := &fakeKube{joined: map[string]struct{}{"i-joined": {}}}
	s := newTestServer(t, fc, fk)
	resp, err := s.NodeGroupNodes(context.Background(), &pb.NodeGroupNodesRequest{Id: "np-a"})
	if err != nil {
		t.Fatal(err)
	}
	states := map[string]pb.InstanceStatus_InstanceState{}
	for _, i := range resp.Instances {
		states[InstanceIDFromProviderID(i.Id)] = i.Status.InstanceState
	}
	if states["i-joined"] != pb.InstanceStatus_instanceRunning {
		t.Fatalf("joined must be running")
	}
	if states["i-booting"] != pb.InstanceStatus_instanceCreating {
		t.Fatalf("running-but-unjoined must be creating (I2)")
	}
	if states["i-creating"] != pb.InstanceStatus_instanceCreating {
		t.Fatalf("Creating must be creating")
	}
}

func TestUnknownGroupIsNotFound(t *testing.T) {
	s := newTestServer(t, &fakeCloud{}, &fakeKube{})
	_, err := s.NodeGroupTargetSize(context.Background(), &pb.NodeGroupTargetSizeRequest{Id: "np-nope"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

// ---- v0.1.12: 1.7 N-11 + 1.8 error propagation ----

func TestDecreaseTargetSizeExplicitNonSuccess(t *testing.T) {
	fc := &fakeCloud{instances: []Instance{{ID: "i-1", GroupID: "np-a", Status: "Running"}}}
	fk := &fakeKube{}
	s := newTestServer(t, fc, fk)
	_, err := s.NodeGroupDecreaseTargetSize(context.Background(),
		&pb.NodeGroupDecreaseTargetSizeRequest{Id: "np-a", Delta: -1})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition, got %v", err)
	}
	if len(fc.released) != 0 || len(fk.deleted) != 0 {
		t.Fatalf("DecreaseTargetSize must never release or delete anything")
	}
}

func TestNodeGroupNodesPropagatesKubeError(t *testing.T) {
	fc := &fakeCloud{instances: []Instance{{ID: "i-1", GroupID: "np-a", Status: "Running"}}}
	fk := &fakeKube{joinedErr: errors.New("apiserver blip")}
	s := newTestServer(t, fc, fk)
	_, err := s.NodeGroupNodes(context.Background(), &pb.NodeGroupNodesRequest{Id: "np-a"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("kube list error must propagate (NOT report all-creating): %v", err)
	}
}
