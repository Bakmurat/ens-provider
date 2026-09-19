# ens-provider

**Autoscaling for the edge node pools Alibaba Cloud does not autoscale.**

`ens-provider` is a [Kubernetes Cluster Autoscaler `externalgrpc`](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler/cloudprovider/externalgrpc)
cloud provider for **Alibaba Cloud ENS edge node pools** in **ACK@Edge** clusters. ENS edge pools ship with no
autoscaling at all: the vendor's Cluster Autoscaler supports only central ESS pools, and Karpenter has no ENS
provider. Until now, edge capacity had to be pinned by hand, which means paying for idle edge nodes around the
clock or being caught short at the one place where latency matters most. `ens-provider` gives ENS pools the full
Cluster Autoscaler experience: autonomous scale-up and scale-down, scale-from-zero, PDB-aware draining, expanders,
backoff, and metrics, with the upstream autoscaler left completely unforked.

Author: Bakmurat Kubanaliev. License: Apache-2.0 (see `LICENSE` and `NOTICE`).

## Why it matters

- **Edge is where the traffic lands.** Regional edge sites serve the latency-critical path. A capacity miss there is
  visible to users immediately; a capacity surplus there is billed by the hour, per site.
- **Cloud economics only work if release is real.** An autoscaler that deletes a Node but leaves the paid instance
  behind saves nothing. `ens-provider` releases the ENS instance first and deletes the Node only after the cloud
  accepts the release, and the billing stop was verified against the cloud's own records.
- **Correctness under eventual consistency.** The ENS inventory API lags behind orders. Every guard in this provider
  exists because a naive implementation over-orders in exactly that window. The residual failure direction is
  deliberately safe: temporary under-scaling, never a pool above its ceiling.
- **Nothing forked.** One Cluster Autoscaler node group maps to one ENS nodepool behind the `externalgrpc` contract,
  so upstream fixes and features arrive without a merge.

## Capabilities

| Capability | What it delivers |
|---|---|
| **In-flight capacity accounting** | `TargetSize` counts joined plus in-flight instances, so the autoscaler never overshoots during the boot-and-join window; booted-but-unjoined instances report as `creating`. |
| **Three-layer ceiling guard** | Advisory fast-fail on the cached count, authoritative re-check under a per-group order lock, and in-memory reservations for orders ENS has accepted but not yet listed. |
| **Transactional scale-down** | Release the ENS instance first, delete the Node second; cross-group deletes refused; not-found releases treated as idempotent success so retried batches converge. |
| **Janitor** | Alerts on ordered-but-never-joined instances; opt-in release of stranded paid instances; reversible reaping of stale Node objects whose instance is confirmed gone. |
| **Scale-from-zero** | Measured node templates per group (CPU, memory, pods, ephemeral storage, allocatable) let the autoscaler plan capacity for an empty pool; pod capacity capped by the node CIDR mask, not the SKU. |
| **Active-passive HA** | Two replicas on a coordination Lease, fail-closed gRPC interceptors on the follower, leader-label Service routing, quiescence-gated lease handoff on shutdown. |
| **Kubernetes-gated readiness** | Only Kubernetes API reachability gates readiness; cloud hiccups are a metrics concern and never trigger restarts. |
| **Secret hygiene** | Every SDK error scrubbed at the boundary (STS tokens, signatures, userdata), attach tokens scrubbed from join logs, bounded SDK timeouts. |
| **Supply-chain posture** | Single static binary, distroless non-root image, vendored upstream protobuf stubs with provenance, 125 tests run under the race detector. |
| **Observability** | Zero-dependency Prometheus metrics for orders, releases, reservations, ownership, and leadership; ISO-8601 UTC logs. |

```
┌────────────────────┐  gRPC (externalgrpc)  ┌──────────────────┐
│ cluster-autoscaler │ ────────────────────► │   ens-provider   │
│  (upstream, cloud) │                       │ (this repo, ×2   │
└────────────────────┘                       │  active-passive) │
                                             └───────┬──────────┘
                                    ENS API (orders, │ CS API (attach
                                    describe, release)│ tokens)
                                                     ▼
                                       ENS edge instances (self-join
                                       via edgeadm userdata → Nodes)
```

## Proven on a live cluster

`ens-provider` was verified end to end on a live ACK@Edge cluster (control plane in Alibaba Cloud Singapore, edge
workers at an ENS site in Vietnam) in July and August 2026, across 16 tagged releases (v0.1.13 to v0.1.28):

| Drill | Result |
|---|---|
| First autonomous scale-up of an edge pool | 24 July 2026 |
| Acceptance suite (scale-up, scale-down, scale-from-zero, ceiling, failure injection) | 9 of 9 scenarios passed |
| HA failover: leader pod deleted / SIGKILL / node drain / rolling update | takeover in 2.1 s / ~4.5 s / 2.0 s / 1.5 s, zero autoscaler errors |
| End-to-end drill: pool 0 → 2 → 0 | autoscaler decision 7 s after the trigger; pods Running 3 min 44 s after the trigger; HA takeover ~2.6 s; ENS billing stop confirmed in the cloud's records 12 min 45 s after scale-down; cluster byte-identical to its baseline afterwards |

The `v0.1.x` line is what those drills ran. Run the test suite and validate against your own pools before rollout;
the design notes below explain every non-obvious decision.

## How it works

- **Scale-up.** Cluster Autoscaler calls `NodeGroupIncreaseSize`; the provider
  orders ENS instances (`RunInstances`, PostPaid) with a cloud-init userdata
  script that downloads `edgeadm` and joins the node into the right ACK
  nodepool with the group's labels and taints. **In-flight capacity
  accounting:** `TargetSize` equals owned instances (joined plus in-flight), so
  the autoscaler never overshoots during the boot-and-join window; booted but
  unjoined instances are reported as `creating`.
- **Ceiling guard, three layers.** An advisory fast-fail on the cached count,
  then an authoritative re-check under the per-group order lock on a fresh
  inventory, plus **in-memory reservations** for orders that ENS has accepted
  but that are not yet visible to `Describe` (ENS inventory is eventually
  consistent). The residual failure direction is deliberately safe: temporary
  under-scaling, never a pool above `max`. See `reservations.go`.
- **Transactional scale-down.** `NodeGroupDeleteNodes` is transactional per
  node: cross-group deletes are refused, the ENS instance is released
  **first**, and the Node object is deleted only after ENS accepts the
  release. Not-found releases count as idempotent success so retried batches
  converge.
- **Janitor** (background loop, leader-scoped under HA). Alerts on
  ordered-but-never-joined instances past `JOIN_TIMEOUT`; releasing stranded
  paid instances requires the explicit `JANITOR_RELEASE=true` opt-in plus
  further gates (alert-only by default). Stale Node objects whose instance is
  *confirmed gone* by an uncached describe are reaped after
  `NODE_CLEANUP_GRACE`; this is reversible (a live kubelet re-registers), so it
  defaults on.
- **Kubernetes-gated readiness.** Only Kubernetes API reachability gates
  readiness; transient ENS or cloud health is a metrics concern and never
  triggers restarts. The readiness controller structurally cannot see the
  cloud client (`readiness.go`). Liveness is a plain `tcpSocket`.
- **Active-passive HA (optional).** With `LEADER_ELECT=true`, two replicas run
  active-passive on a coordination Lease; only the leader serves (fail-closed
  gRPC interceptors), and a leader pod label routes the Service. Both pods stay
  kubelet-Ready; shutdown performs a quiescence-gated, owner-checked lease
  handoff. `LEADER_ELECT=false` (default) is single-replica with no election
  constructed. See `leader.go`.
- **Scale-from-zero templates.** Each group may carry a measured node template
  (CPU, memory, pods, ephemeral storage, and allocatable values) so the
  autoscaler can plan a scale-up from an empty pool; the pod capacity is
  capped by the node CIDR mask, not the instance SKU.
- **Secret hygiene.** Every error born from an Alibaba SDK call is scrubbed at
  the `ens.go` boundary (`sanitizeErr` strips URL query strings that carry the
  STS token, signature, and base64 userdata). Userdata runs without `set -x`
  and scrubs the attach token from `edgeadm`'s own log. SDK calls carry
  bounded timeouts.

## Repository layout

Flat, single `package main`, five logical layers with one-way dependencies:

| Layer    | Files | Responsibility |
|----------|-------|----------------|
| entry    | `main.go` | wiring: signals, lifecycle, background loop, health, ISO-8601 UTC log timestamps |
| serving  | `server.go` `readiness.go` `leader.go` `metrics.go` | externalgrpc RPCs, Kubernetes-gated readiness, leader election, Prometheus text endpoint |
| decision | `reservations.go` `janitor.go` `template.go` `userdata.go` | order guard and reservations, stranded-instance and stale-Node handling, scale-from-zero template, join script |
| clients  | `ens.go` `kube.go` | Alibaba ENS/CS API glue (describe cache, single-flight, order submission), Kubernetes client (providerID reconciler, autonomy annotation) |
| config   | `groups.go` | env and `NODE_GROUPS` parsing, startup validation (fail loud at boot, never at first RPC) |

`protos/` vendors the upstream externalgrpc stubs verbatim from
kubernetes/autoscaler (provenance in `protos/VENDORED_FROM`, reproducible with
`hack/vendor-protos.sh`); they keep their upstream copyright and license.
Tests map 1:1 to implementations (`janitor.go` ↔ `janitor_test.go`, …); the
core state machine runs against `CloudAPI`/`KubeAPI` interfaces so it is
testable with fakes under `-race`.

`charts/edge-autoscaler/` is the Helm chart: RBAC, the provider Deployment
(RRSA-authenticated), the upstream Cluster Autoscaler Deployment configured for
`--cloud-provider=externalgrpc`, and PodDisruptionBudgets.

## Configuration

Everything is read from the environment once at startup (rendered by the
`edge-autoscaler` Helm chart from your values). A config error kills the
process at boot with a group-specific message. **Env values are read at
startup only; after changing them, restart the deployment**
(`kubectl rollout restart deploy/ens-provider`).

Required:

| Var | Meaning |
|-----|---------|
| `ENS_REGION` | ENS edge region the instances live in |
| `CLUSTER_ID` | ACK cluster id (attach-token source) |
| `NODE_GROUPS` | JSON list, one entry per edge nodepool: `id`, `nodepool_id`, `name_prefix` (instance-ownership prefix; must be disjoint across groups), `min`/`max`, machine spec (`instance_type`, `image_id`, `disk_category`, `disk_size`, …), `labels`, `taints`, optional measured `template` for scale-from-zero |
| `NETWORK_ID` / `VSWITCH_ID` / `SECURITY_GROUP_ID` | shared ENS network defaults (overridable per group) |

Credentials resolve through the standard `aliyun/credentials-go` env chain:
RRSA/OIDC (`ALIBABA_CLOUD_ROLE_ARN` + `ALIBABA_CLOUD_OIDC_PROVIDER_ARN` +
`ALIBABA_CLOUD_OIDC_TOKEN_FILE`) in-cluster, AK/SK as a local-run fallback.

Common knobs (defaults in `groups.go`):

| Var | Default | Meaning |
|-----|---------|---------|
| `CONTROL_REGION` | `ap-southeast-1` | ACK control-plane region (CS endpoint, OSS host for edgeadm) |
| `IMAGE_REPO_TYPE` | `public` | `private` = internal OSS/registry endpoints over a dedicated line |
| `PUBLIC_IP` | `false` | edge workers get no public IP; egress rides the ENS NAT gateway |
| `DESCRIBE_CACHE_TTL` | `10` (s) | region-describe cache (protects the ENS API from the autoscaler's tight loop) |
| `JOIN_TIMEOUT` | `30m` | janitor alert threshold for never-joined instances |
| `JANITOR_RELEASE` | `false` | opt-in auto-release of stranded paid instances (alert-only by default) |
| `JANITOR_NODE_CLEANUP` / `NODE_CLEANUP_GRACE` | `true` / `5m` | reap Node objects whose instance is confirmed gone |
| `JOIN_TOKEN_TTL` | `2h` | attach-token validity; exactly `0` (Alibaba's 1h default) or `[1h, 24h]` |
| `NODE_AUTONOMY_DURATION` | `10m` | OpenYurt edge-autonomy annotation stamped on managed nodes |
| `LEADER_ELECT` | `false` | active-passive HA; requires `POD_NAME`/`POD_NAMESPACE` (downward API) |
| `LISTEN_ADDR` / `METRICS_ADDR` | `:8086` / `:9090` | gRPC (plaintext, in-cluster only) / Prometheus text endpoint |

## Build, test, deploy

```sh
make test                          # go vet + unit tests
make test-race                     # go test -race ./...  (run before any release)
make build                         # static binary -> bin/ens-provider
make build-release VERSION=v0.1.0  # release binary -> release/
make image                         # distroless image from release/ (linux/amd64)
make push VERSION=v0.1.0 REGISTRY=registry.example.com/<namespace>
make vendor-protos CA_TAG=cluster-autoscaler-1.32.1   # (re)vendor gRPC stubs
```

Install the chart with your own values (cluster id, RRSA role and OIDC
provider ARNs, ENS network ids, the provider image, and one `edgeGroups`
entry per edge nodepool):

```sh
helm upgrade --install edge-autoscaler charts/edge-autoscaler -n kube-system -f my-values.yaml
```

The vendored protos **must match the Cluster Autoscaler image's minor
version**. When bumping the autoscaler past 1.34: upstream PR #8660 changed
`NodeGroupTemplateNodeInfoResponse` to `nodeBytes`; re-vendor and flip the
marked line in `server.go` (see the Makefile's upgrade playbook). A proto
mismatch fails *silently* as broken scale-from-zero, not loudly.

The image is distroless (`gcr.io/distroless/static-debian12:nonroot`): no
shell, nonroot, single static binary.

## Observability

- **Metrics** (`:9090`, hand-rolled Prometheus text, zero dependencies by design):
  `ens_provider_orders_total{outcome=success|failure|ambiguous}`,
  `ens_provider_instances_released_total`, `ens_provider_release_failures_total`,
  `ens_provider_reservations_expired_total`, `ens_provider_owned_instances`,
  `ens_provider_unjoined_instances`, `ens_provider_is_leader`,
  `ens_provider_routing_published`, `ens_provider_leadership_transitions_total`.
  Counters are process-local and reset on pod restart. Under HA the Service
  routes to the leader only, so scrape **pods**, not the Service.
- **Logs:** stdlib log lines carry ISO-8601 UTC timestamps so log pipelines
  parse event time instead of falling back to ingest time. klog
  (leader-election) lines are left untouched.

## Design notes

Non-obvious decisions in the code cite their origin in place (a live drill, a
CLI bisection, an upstream PR, or a design-review finding). When changing such
code, follow the citation before assuming the comment is stale.
