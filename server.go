package main

// The externalgrpc CloudProvider service. Semantics are a faithful port of
// the live-validated Python implementation (the design notes):
//   * TargetSize == owned instances (joined + in-flight) so CA never overshoots
//     during the ~2.5min ENS boot+join window
//   * booted-but-unjoined instances are reported as 'creating' (upcoming)
//   * DeleteNodes releases the ENS instance AND deletes the Node object
//   * TemplateNodeInfo serves the scale-from-zero template
//
// UPGRADE NOTE (CA >= 1.35): upstream PR #8660 changed
// NodeGroupTemplateNodeInfoResponse from an embedded v1.Node (field 1) to
// `bytes nodeBytes` (field 2). When bumping the CA image past 1.34: run
// `make vendor-protos CA_TAG=cluster-autoscaler-1.35.x`, then switch
// NodeGroupTemplateNodeInfo below to marshal the node into NodeBytes.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "ens-provider/protos"
)

// CloudAPI / KubeAPI: the exact surfaces the RPCs consume — interfaces so the
// server state machine is testable against fakes (design plan).
type CloudAPI interface {
	OwnedInstances(g *Group, force bool) ([]Instance, error)
	AllOwnedInstances(force bool) ([]Instance, error)
	InstanceByID(instanceID string) (Instance, bool, error)
	RunInstances(ctx context.Context, g *Group, count int32) ([]string, error)
	ReleaseInstance(instanceID string) error
	InvalidateCache()
}

type KubeAPI interface {
	JoinedInstanceIDs(ctx context.Context) (map[string]struct{}, error)
	DeleteNodeObject(ctx context.Context, name string) error
	ReconcileProviderIDs(ctx context.Context, cloud InstanceLister) (int, error)
}

type Server struct {
	pb.UnimplementedCloudProviderServer
	cfg   *Config
	cloud CloudAPI
	kube  KubeAPI
	ngs   map[string]*pb.NodeGroup
}

func NewServer(cfg *Config, cloud CloudAPI, kube KubeAPI) *Server {
	ngs := map[string]*pb.NodeGroup{}
	for _, g := range cfg.Groups {
		ngs[g.ID] = &pb.NodeGroup{Id: g.ID, MinSize: g.Min, MaxSize: g.Max, Debug: g.Debug()}
	}
	return &Server{cfg: cfg, cloud: cloud, kube: kube, ngs: ngs}
}

func (s *Server) group(id string) (*Group, error) {
	if g := s.cfg.GroupByID(id); g != nil {
		return g, nil
	}
	return nil, status.Errorf(codes.NotFound, "unknown node group %s", id)
}

// ---- provider-level RPCs ---------------------------------------------------

func (s *Server) NodeGroups(ctx context.Context, _ *pb.NodeGroupsRequest) (*pb.NodeGroupsResponse, error) {
	out := make([]*pb.NodeGroup, 0, len(s.ngs))
	for _, g := range s.cfg.Groups { // stable order
		out = append(out, s.ngs[g.ID])
	}
	return &pb.NodeGroupsResponse{NodeGroups: out}, nil
}

func (s *Server) NodeGroupForNode(ctx context.Context, req *pb.NodeGroupForNodeRequest) (*pb.NodeGroupForNodeResponse, error) {
	if req.Node != nil {
		if g := s.cfg.GroupForNodeLabels(req.Node.Labels); g != nil {
			return &pb.NodeGroupForNodeResponse{NodeGroup: s.ngs[g.ID]}, nil
		}
	}
	// cloud system/ess node or anything else -> empty id => CA ignores it
	return &pb.NodeGroupForNodeResponse{NodeGroup: &pb.NodeGroup{Id: ""}}, nil
}

func (s *Server) GPULabel(ctx context.Context, _ *pb.GPULabelRequest) (*pb.GPULabelResponse, error) {
	return &pb.GPULabelResponse{Label: "nvidia.com/gpu"}, nil
}

func (s *Server) GetAvailableGPUTypes(ctx context.Context, _ *pb.GetAvailableGPUTypesRequest) (*pb.GetAvailableGPUTypesResponse, error) {
	return &pb.GetAvailableGPUTypesResponse{}, nil
}

func (s *Server) Cleanup(ctx context.Context, _ *pb.CleanupRequest) (*pb.CleanupResponse, error) {
	return &pb.CleanupResponse{}, nil
}

func (s *Server) Refresh(ctx context.Context, _ *pb.RefreshRequest) (*pb.RefreshResponse, error) {
	s.cloud.InvalidateCache()
	if _, err := s.kube.ReconcileProviderIDs(ctx, s.cloud); err != nil {
		log.Printf("[server] Refresh providerID reconcile: %v", err)
	}
	return &pb.RefreshResponse{}, nil
}

// ---- node-group RPCs ---------------------------------------------------------

func (s *Server) NodeGroupTargetSize(ctx context.Context, req *pb.NodeGroupTargetSizeRequest) (*pb.NodeGroupTargetSizeResponse, error) {
	g, err := s.group(req.Id)
	if err != nil {
		return nil, err
	}
	owned, err := s.cloud.OwnedInstances(g, false)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "describe: %v", err)
	}
	return &pb.NodeGroupTargetSizeResponse{TargetSize: int32(len(owned))}, nil
}

func (s *Server) NodeGroupIncreaseSize(ctx context.Context, req *pb.NodeGroupIncreaseSizeRequest) (*pb.NodeGroupIncreaseSizeResponse, error) {
	g, err := s.group(req.Id)
	if err != nil {
		return nil, err
	}
	owned, err := s.cloud.OwnedInstances(g, false)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "describe: %v", err)
	}
	// ADVISORY fast-fail on the cached count: rejects the obviously-over
	// request without an uncached describe or the group lock. NOT the
	// authority - the binding check re-runs INSIDE RunInstances under the
	// group lock on a fresh count (v0.1.21, backlog item 15.3).
	target := int32(len(owned)) + req.Delta
	if target > g.Max {
		return nil, status.Errorf(codes.OutOfRange,
			"[%s] increase by %d exceeds max %d (owned=%d)", g.ID, req.Delta, g.Max, len(owned))
	}
	log.Printf("[server] IncreaseSize group=%s delta=%d (owned %d -> %d)", g.ID, req.Delta, len(owned), target)
	if _, err := s.cloud.RunInstances(ctx, g, req.Delta); err != nil {
		if errors.Is(err, ErrExceedsMax) {
			return nil, status.Errorf(codes.OutOfRange, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "RunInstances: %v", err)
	}
	return &pb.NodeGroupIncreaseSizeResponse{}, nil
}

// NodeGroupDeleteNodes is TRANSACTIONAL per node (v0.1.8, plan 1.3):
//  1. the node must belong to the REQUESTED group (refuse cross-group deletes);
//  2. the ENS release must be ACCEPTED before the Node object is touched — a
//     failed release with a deleted Node meant the kubelet re-registered and
//     CA/billing drifted (design record §12.4);
//  3. not-found on release == idempotent success (handled inside ReleaseInstance);
//  4. ANY per-node failure -> aggregated non-nil gRPC error. CA keeps the node
//     accounted and re-drives the whole operation next loop; the already-
//     released nodes converge idempotently on the retry.
func (s *Server) NodeGroupDeleteNodes(ctx context.Context, req *pb.NodeGroupDeleteNodesRequest) (*pb.NodeGroupDeleteNodesResponse, error) {
	g, err := s.group(req.Id)
	if err != nil {
		return nil, err
	}
	owned, err := s.cloud.AllOwnedInstances(false)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "describe before delete: %v", err)
	}
	byID := map[string]Instance{}
	for _, o := range owned {
		byID[o.ID] = o
	}
	var errs []string
	for _, node := range req.Nodes {
		// Labels are not the authority for cloud ownership, but when CA supplies
		// the ACK nodepool label it must agree with the requested group.
		if nodepoolID := node.Labels[NodepoolLabel]; nodepoolID != "" && nodepoolID != g.NodepoolID {
			errs = append(errs, fmt.Sprintf("%s: nodepool label %q does not match requested group %q - refusing", node.Name, nodepoolID, g.NodepoolID))
			continue
		}

		// Mutating correlation is deliberately stricter than the read path:
		// only <this-ENS-region>.i-<alphanumeric> may become a release candidate.
		iid := s.cfg.StrictEdgeInstanceID(node.ProviderID)
		log.Printf("[server] DeleteNodes group=%s node=%s providerID=%s -> ens %s", g.ID, node.Name, node.ProviderID, iid)
		if iid == "" {
			errs = append(errs, fmt.Sprintf("%s: providerID %q is not an instance in ENS region %s - refusing", node.Name, node.ProviderID, s.cfg.EnsRegion))
			continue
		}

		if inst, ok := byID[iid]; ok {
			if inst.GroupID != g.ID {
				errs = append(errs, fmt.Sprintf("%s: instance %s belongs to group %s not %s - refusing", node.Name, iid, inst.GroupID, g.ID))
				continue
			}
		} else {
			// A miss in the prefix-filtered inventory is ambiguous: the instance
			// may be gone, foreign, in another managed group, or newly visible.
			// Resolve it directly and fail closed unless exact requested-group
			// ownership is proven.
			instance, found, lookupErr := s.cloud.InstanceByID(iid)
			if lookupErr != nil {
				errs = append(errs, fmt.Sprintf("%s: ownership lookup for %s: %v", node.Name, iid, lookupErr))
				continue
			}
			if !found {
				// P0-3 residual (2026-08 review, documented BY DECISION; wording
				// per C-03): this lookup runs WITHOUT the group order lock, so
				// it can race an in-flight order whose instance is not yet
				// Describe-visible. Taking the lock would not close it —
				// reservations are keyed by op NAME and carry no instance IDs
				// (none exist before visibility), so there is nothing to
				// correlate iid against — and it would put another network
				// call under the lock (the P0-2 class). The race is
				// LOW-LIKELIHOOD under the observed lifecycle, not impossible:
				// a Node's providerID only proves the reconciler saw the
				// instance in ONE fresh inventory (kube.go
				// ReconcileProviderIDs), and ENS Describe results are not
				// guaranteed monotonic. If it misfires, the failure direction
				// is recoverable: only the Node object is deleted here — the
				// release path is never taken — so a live kubelet re-registers
				// its Node, and an instance left with no correlated Node is
				// surfaced by the janitor's alert classes (Running past
				// JOIN_TIMEOUT measured from CreationTime, or Stopped/Expired)
				// on a later sweep, with release still gated behind
				// JANITOR_RELEASE.
				log.Printf("[server]   instance %s already gone -> deleting stale Node object", iid)
				if node.Name != "" {
					if err := s.kube.DeleteNodeObject(ctx, node.Name); err != nil {
						errs = append(errs, fmt.Sprintf("%s: node object delete: %v", node.Name, err))
					}
				}
				continue
			}

			owner := s.cfg.GroupForInstanceName(instance.Name)
			if owner == nil {
				log.Printf("[server]   REFUSING release of %s: name %q is not owned by any configured group", iid, instance.Name)
				errs = append(errs, fmt.Sprintf("%s: instance %s name %q is not owned by any configured group - refusing", node.Name, iid, instance.Name))
				continue
			}
			if owner.ID != g.ID {
				log.Printf("[server]   REFUSING release of %s: name %q belongs to group %s, requested %s", iid, instance.Name, owner.ID, g.ID)
				errs = append(errs, fmt.Sprintf("%s: instance %s belongs to group %s not %s - refusing", node.Name, iid, owner.ID, g.ID))
				continue
			}
			log.Printf("[server]   instance %s ownership confirmed by uncached lookup (group=%s name=%q)", iid, owner.ID, instance.Name)
		}

		// release FIRST; only delete the Node after ENS accepts the release.
		if err := s.cloud.ReleaseInstance(iid); err != nil {
			log.Printf("[server]   release %s FAILED, keeping Node object: %v", iid, err)
			errs = append(errs, fmt.Sprintf("%s: release %s: %v", node.Name, iid, err))
			continue
		}
		log.Printf("[server]   release %s accepted -> deleting Node object", iid)
		if node.Name != "" {
			if err := s.kube.DeleteNodeObject(ctx, node.Name); err != nil {
				errs = append(errs, fmt.Sprintf("%s: node object delete: %v", node.Name, err))
			}
		}
	}
	if len(errs) > 0 {
		return nil, status.Errorf(codes.Internal, "DeleteNodes: %d/%d failed: %s",
			len(errs), len(req.Nodes), strings.Join(errs, "; "))
	}
	return &pb.NodeGroupDeleteNodesResponse{}, nil
}

// NodeGroupDecreaseTargetSize returns EXPLICIT NON-SUCCESS (v0.1.12, plan 1.7;
// design record §12.5). The RPC exists for providers whose target can exceed
// actual instances (async fulfilment). Here RunInstances is synchronous and
// TargetSize is COMPUTED from owned instances - an unfulfilled delta cannot
// exist, so there is never anything to cancel. A silent success-no-op would
// mislead CA's accounting (review finding, adopted); steady state never
// triggers this RPC at all (asserted by N-11).
func (s *Server) NodeGroupDecreaseTargetSize(ctx context.Context, req *pb.NodeGroupDecreaseTargetSizeRequest) (*pb.NodeGroupDecreaseTargetSizeResponse, error) {
	if _, err := s.group(req.Id); err != nil {
		return nil, err
	}
	log.Printf("[server] DecreaseTargetSize group=%s delta=%d -> FailedPrecondition (unfulfilled deltas cannot exist here; investigate WHY CA called this)", req.Id, req.Delta)
	return nil, status.Errorf(codes.FailedPrecondition,
		"[%s] DecreaseTargetSize(%d) unsupported by design: TargetSize==owned (RunInstances is synchronous); nothing to cancel and no Node may be deleted via this RPC", req.Id, req.Delta)
}

// stateFor is an EXPLICIT table (v0.1.9, plan 1.4). The old default->Running
// meant a Stopped/Expired/unknown-status instance looked like healthy capacity
// to CA forever. Now: only "Running" is running; dead states report deleting
// (the janitor releases them); an UNKNOWN status is logged loudly and reported
// creating - upcoming at worst, never schedulable capacity.
func stateFor(ensStatus string) pb.InstanceStatus_InstanceState {
	switch ensStatus {
	case "Running":
		return pb.InstanceStatus_instanceRunning
	case "Pending", "Starting", "Creating", "Building", "Preparing":
		return pb.InstanceStatus_instanceCreating
	case "Stopping", "Releasing", "Deleting", "Stopped", "Expired":
		return pb.InstanceStatus_instanceDeleting
	default:
		log.Printf("[server] UNKNOWN ENS status %q - reporting instanceCreating, never running (plan 1.4)", ensStatus)
		return pb.InstanceStatus_instanceCreating
	}
}

func (s *Server) NodeGroupNodes(ctx context.Context, req *pb.NodeGroupNodesRequest) (*pb.NodeGroupNodesResponse, error) {
	g, err := s.group(req.Id)
	if err != nil {
		return nil, err
	}
	owned, err := s.cloud.OwnedInstances(g, false)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "describe: %v", err)
	}
	joined, err := s.kube.JoinedInstanceIDs(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "kube node list: %v", err)
	}
	instances := make([]*pb.Instance, 0, len(owned))
	for _, o := range owned {
		st := stateFor(o.Status)
		// A booted instance whose kube Node hasn't joined+correlated yet is still
		// UPCOMING to CA — report 'creating' so CA counts it toward the target.
		if st == pb.InstanceStatus_instanceRunning {
			if _, ok := joined[o.ID]; !ok {
				st = pb.InstanceStatus_instanceCreating
			}
		}
		instances = append(instances, &pb.Instance{
			Id:     s.cfg.ProviderID(o.ID), // MUST equal node.spec.providerID
			Status: &pb.InstanceStatus{InstanceState: st},
		})
	}
	return &pb.NodeGroupNodesResponse{Instances: instances}, nil
}

func (s *Server) NodeGroupTemplateNodeInfo(ctx context.Context, req *pb.NodeGroupTemplateNodeInfoRequest) (*pb.NodeGroupTemplateNodeInfoResponse, error) {
	g, err := s.group(req.Id)
	if err != nil {
		return nil, err
	}
	node := BuildTemplateNode(s.cfg, g)
	log.Printf("[server] TemplateNodeInfo group=%s served (scale-from-zero template)", g.ID)
	// CA <= 1.34 contract: response field 1 = k8s.io.api.core.v1.Node.
	// CA >= 1.35: switch to NodeBytes (field 2) — see the header note.
	return &pb.NodeGroupTemplateNodeInfoResponse{NodeInfo: node}, nil
}

// PricingNodePrice / PricingPodPrice / NodeGroupGetOptions: inherited from
// UnimplementedCloudProviderServer -> gRPC code 12 (Unimplemented), which the
// CA client treats as "optional RPC not provided". Exactly what we want.
