package main

// Node-group model + configuration. One CA node group per ENS edge nodepool
// (the ACK@Edge analog of "one AWS nodegroup = one ASG"). Groups arrive as
// JSON in NODE_GROUPS (rendered by the Helm chart from Phase-2 Terraform);
// every field falls back to the shared ENS_* env defaults.
//
// Env names match the keys the Helm chart renders into the ens-provider-config
// ConfigMap (chart/templates/provider.yaml) — change them in lockstep.

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

// NodepoolLabel is the label ACK always sets on a node, identifying its edge
// nodepool — our group key.
const NodepoolLabel = "alibabacloud.com/nodepool-id"

// C-01 allowlists (design review): CONTROL_REGION, K8S_VERSION and
// RUNTIME_VERSION reach the SAME single-quoted/interpolated join-script sink
// as the labels (userdata.go: the edgeadm wget URL, the --region argument,
// the node-spec runtime_version), so the P0-1 gate must cover every script
// input, not only the label-shaped ones. Narrow identifier charsets — no
// shell metacharacter passes. Live values stay accepted: ap-southeast-1 /
// 1.32.1-aliyun.1 / 1.6.39.
var (
	regionIDRe  = regexp.MustCompile(`^[a-z0-9-]+$`)
	versionIDRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
)

type Taint struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	Effect string `json:"effect"`
}

type Template struct {
	CPU       string `json:"cpu"`
	Memory    string `json:"memory"`
	Pods      string `json:"pods"`
	Ephemeral string `json:"ephemeral"`
	CPUAlloc  string `json:"cpu_alloc"`
	MemAlloc  string `json:"mem_alloc"`
}

type Group struct {
	ID              string            `json:"id"`
	NodepoolID      string            `json:"nodepool_id"`
	NamePrefix      string            `json:"name_prefix"`
	Min             int32             `json:"min"`
	Max             int32             `json:"max"`
	InstanceType    string            `json:"instance_type"`
	ImageID         string            `json:"image_id"`
	DiskCategory    string            `json:"disk_category"`
	DiskSize        json.Number       `json:"disk_size"`
	Keypair         string            `json:"keypair"`
	Bandwidth       json.Number       `json:"bandwidth"`
	NetworkID       string            `json:"network_id"`
	VSwitchID       string            `json:"vswitch_id"`
	SecurityGroupID string            `json:"security_group_id"`
	PublicIP        *bool             `json:"public_ip"`
	Labels          map[string]string `json:"labels"`
	Taints          []Taint           `json:"taints"`
	Template        *Template         `json:"template"`
}

// Config is everything read from the environment once at startup.
type Config struct {
	EnsRegion      string
	ControlRegion  string
	ClusterID      string
	K8sVersion     string
	RuntimeVersion string
	ListenAddr     string
	MetricsAddr    string
	// Where the JOIN pipeline downloads from (v0.1.18, hardening checklist item D1):
	// "public" = public OSS endpoints via the ENS NAT (the proven default path);
	// "private" = internal OSS endpoints (oss-<region>-internal) over the
	// dedicated line — requires the TR's oss-* internal-endpoint statics to
	// route the 100.x service CIDRs (shared-TR envs transit the VPC that owns
	// them; verified live on the UAT cluster 2026-07-26: artifact download HTTP 200).
	// Governs BOTH the edgeadm binary host and the node-spec imageRepoType
	// (which drives edgeadm's component downloads).
	ImageRepoType string

	// shared defaults a group may override
	DefImage     string
	DefInstance  string
	DefNetwork   string
	DefVSwitch   string
	DefSecurity  string
	DefDiskCat   string
	DefDiskSize  string
	DefKeypair   string
	DefBandwidth string
	DefPublicIP  bool
	DefTemplate  Template

	DescribeTTLSeconds   int
	ReconcileLoopSeconds int
	// janitor (v0.1.9, design plan): alert past JoinTimeout; release only if
	// JanitorRelease AND no Node correlates. Alert-only by default.
	JoinTimeout    time.Duration
	JanitorRelease bool
	// node cleanup (v0.1.19, backlog item 15.1 / acceptance-T7 finding): delete
	// Node objects whose instance is CONFIRMED gone (uncached describe) and
	// which have been NotReady past the grace window. Unlike instance release
	// this is reversible (a live kubelet re-registers its Node), so it
	// defaults ON; the grace + exists-check + per-tick cap keep it tame
	// during line flaps (flapped nodes' instances still exist -> skipped).
	JanitorNodeCleanup bool
	NodeCleanupGrace   time.Duration
	// readiness (v0.1.16, Issue 3): the K8s-reachability controller's cadence and
	// per-check budget. envDuration FATAL-exits on <=0, so the ticker is always
	// positive. There is deliberately NO always-Ready escape hatch (C-006).
	ReadinessCheckInterval time.Duration
	ReadinessCheckTimeout  time.Duration
	Groups                 []*Group

	// Edge node autonomy (official-docs audit 2026-07-14): autonomy is OFF by
	// default and is a per-node ANNOTATION, so the reconciler stamps it on every
	// managed node. Bounded autonomy ("500s"/"10m", K8s >= 1.28) survives line
	// flaps but still frees pods of genuinely dead nodes for CA replacement;
	// "unbounded" = node.beta.openyurt.io/autonomy=true; "off" disables.
	AutonomyDuration string

	// Active-passive HA (v0.1.25): with LEADER_ELECT=true the
	// process joins a Lease election; only the leader serves (leader.go).
	// Default false preserves v0.1.24 operational semantics exactly — no
	// Lease client is even constructed. Timings follow CA's own defaults and
	// must satisfy client-go's inequalities (validated at startup).
	LeaderElect         bool
	LeaderLeaseDuration time.Duration
	LeaderRenewDeadline time.Duration
	LeaderRetryPeriod   time.Duration
	// Pod identity via the downward API — REQUIRED when LeaderElect: the
	// election identity and the leader-label target. FATAL if missing.
	PodName      string
	PodNamespace string

	// Join-token validity (v0.1.23, backlog item 13): one CA wave = one order =
	// one shared attach token; it must outlive issuance -> boot + edgeadm
	// download + the FIRST authenticated call (~75s measured) PLUS the retry
	// tail (userdata re-runs `edgeadm join` up to 3x with backoff, each retry
	// re-presenting the token) and provisioning delays. Default 2h = twice
	// Alibaba's documented 1h default validity (their guide recommends
	// extending it for batch addition). 0 = do NOT send Expired: Alibaba's
	// 1h default applies (the escape hatch - NOT "non-expiring"). Domain is
	// exactly 0 | [1h, 24h]; anything else fails startup.
	JoinTokenTTL time.Duration
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	// strict: a non-integer value is a config typo — fail loud, not truncate
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: env %s=%q is not an integer\n", key, v)
		os.Exit(1)
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil || d <= 0 {
		fmt.Fprintf(os.Stderr, "FATAL: env %s=%q is not a positive duration\n", key, v)
		os.Exit(1)
	}
	return d
}

// parseJoinTokenTTL: dedicated parser for JOIN_TOKEN_TTL. envDuration cannot
// express the 0-means-omit escape hatch (it fatals on <=0), and this knob
// needs BOUNDS: a floor of 1h so a typo ("2m") can't make behavior worse
// than Alibaba's documented default, and a 24h cap so a typo ("200h") can't
// mint a day-long-plus credential. Returns an error (LoadConfig propagates)
// instead of os.Exit so it is unit-testable.
func parseJoinTokenTTL(v string) (time.Duration, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 2 * time.Hour, nil
	}
	if v == "0" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("JOIN_TOKEN_TTL=%q is not a duration (want \"0\" or e.g. \"2h\")", v)
	}
	if d < time.Hour || d > 24*time.Hour {
		return 0, fmt.Errorf("JOIN_TOKEN_TTL=%q out of range: must be exactly \"0\" or within [1h, 24h]", v)
	}
	return d, nil
}

func LoadConfig() (*Config, error) {
	c := &Config{
		EnsRegion:      os.Getenv("ENS_REGION"),
		ControlRegion:  env("CONTROL_REGION", "ap-southeast-1"),
		ClusterID:      os.Getenv("CLUSTER_ID"),
		K8sVersion:     env("K8S_VERSION", "1.32.1-aliyun.1"),
		RuntimeVersion: env("RUNTIME_VERSION", "1.6.39"),
		ListenAddr:     env("LISTEN_ADDR", "0.0.0.0:8086"),
		MetricsAddr:    env("METRICS_ADDR", "0.0.0.0:9090"),
		ImageRepoType:  env("IMAGE_REPO_TYPE", "public"),

		DefImage:     env("IMAGE_ID", "alibaba_cloud_linux_3.2104_x64_20G_20240527"),
		DefInstance:  env("INSTANCE_TYPE", "ens.sn1.small"),
		DefNetwork:   os.Getenv("NETWORK_ID"),
		DefVSwitch:   os.Getenv("VSWITCH_ID"),
		DefSecurity:  os.Getenv("SECURITY_GROUP_ID"),
		DefDiskCat:   env("DISK_CATEGORY", "local_ssd"),
		DefDiskSize:  env("DISK_SIZE", "50"),
		DefKeypair:   env("KEYPAIR", "ens-provider-key"),
		DefBandwidth: env("BANDWIDTH_OUT", "100"),
		// default: PRIVATE workers (no public IP; egress via the ENS NAT GW,
		// cloud<->edge rides the dedicated line). PUBLIC_IP=true = the PoC mode.
		DefPublicIP: strings.EqualFold(env("PUBLIC_IP", "false"), "true"),
		DefTemplate: Template{
			CPU:    env("TMPL_CPU", "4"),
			Memory: env("TMPL_MEMORY", "7938184Ki"),
			// 64, not 128: node_cidr_mask=/26 caps pods-per-node at 64 for EVERY
			// SKU (not just sn1.tiny) - all live sn1.small nodes report cap/alloc
			// pods=64. The earlier "128 on small" was an error. TMPL_PODS is an
			// internal provider override only (deliberately NOT a chart knob -
			// the cap is a measured network invariant, not a tuning control).
			Pods:      env("TMPL_PODS", "64"),
			Ephemeral: env("TMPL_EPHEMERAL", "40900288Ki"),
			CPUAlloc:  env("TMPL_CPU_ALLOC", "3920m"),
			MemAlloc:  env("TMPL_MEM_ALLOC", "7370888Ki"),
		},
		DescribeTTLSeconds:     envInt("DESCRIBE_CACHE_TTL", 10),
		ReconcileLoopSeconds:   envInt("PROVIDERID_RECONCILE_SECONDS", 15),
		JoinTimeout:            envDuration("JOIN_TIMEOUT", 30*time.Minute),
		JanitorRelease:         strings.EqualFold(env("JANITOR_RELEASE", "false"), "true"),
		JanitorNodeCleanup:     strings.EqualFold(env("JANITOR_NODE_CLEANUP", "true"), "true"),
		NodeCleanupGrace:       envDuration("NODE_CLEANUP_GRACE", 5*time.Minute),
		AutonomyDuration:       env("NODE_AUTONOMY_DURATION", "10m"),
		ReadinessCheckInterval: envDuration("READINESS_CHECK_INTERVAL", 10*time.Second),
		ReadinessCheckTimeout:  envDuration("READINESS_CHECK_TIMEOUT", 5*time.Second),
		LeaderElect:            strings.EqualFold(env("LEADER_ELECT", "false"), "true"),
		LeaderLeaseDuration:    envDuration("LEADER_LEASE_DURATION", 15*time.Second),
		LeaderRenewDeadline:    envDuration("LEADER_RENEW_DEADLINE", 10*time.Second),
		LeaderRetryPeriod:      envDuration("LEADER_RETRY_PERIOD", 2*time.Second),
		PodName:                os.Getenv("POD_NAME"),
		PodNamespace:           os.Getenv("POD_NAMESPACE"),
	}
	if c.LeaderElect {
		if c.PodName == "" || c.PodNamespace == "" {
			return nil, fmt.Errorf("LEADER_ELECT=true requires POD_NAME and POD_NAMESPACE (downward API)")
		}
		// client-go's own constraints, surfaced at OUR startup with a clear
		// message instead of a panic inside NewLeaderElector.
		if c.LeaderLeaseDuration <= c.LeaderRenewDeadline {
			return nil, fmt.Errorf("LEADER_LEASE_DURATION (%s) must be greater than LEADER_RENEW_DEADLINE (%s)", c.LeaderLeaseDuration, c.LeaderRenewDeadline)
		}
		if float64(c.LeaderRenewDeadline) <= 1.2*float64(c.LeaderRetryPeriod) {
			return nil, fmt.Errorf("LEADER_RENEW_DEADLINE (%s) must exceed 1.2 * LEADER_RETRY_PERIOD (%s)", c.LeaderRenewDeadline, c.LeaderRetryPeriod)
		}
	}
	ttl, err := parseJoinTokenTTL(os.Getenv("JOIN_TOKEN_TTL"))
	if err != nil {
		return nil, err
	}
	c.JoinTokenTTL = ttl
	if c.EnsRegion == "" || c.ClusterID == "" {
		return nil, fmt.Errorf("ENS_REGION and CLUSTER_ID are required")
	}
	if c.ImageRepoType != "public" && c.ImageRepoType != "private" {
		return nil, fmt.Errorf("IMAGE_REPO_TYPE must be \"public\" or \"private\" (got %q)", c.ImageRepoType)
	}
	// C-01 : script-sink inputs — see the allowlist comment above.
	if !regionIDRe.MatchString(c.ControlRegion) {
		return nil, fmt.Errorf("CONTROL_REGION %q is not a valid region id (must match %s — it is embedded in the join script)", c.ControlRegion, regionIDRe)
	}
	if !versionIDRe.MatchString(c.K8sVersion) {
		return nil, fmt.Errorf("K8S_VERSION %q is not a valid version id (must match %s — it is embedded in the join script)", c.K8sVersion, versionIDRe)
	}
	if !versionIDRe.MatchString(c.RuntimeVersion) {
		return nil, fmt.Errorf("RUNTIME_VERSION %q is not a valid version id (must match %s — it is embedded in the node-spec)", c.RuntimeVersion, versionIDRe)
	}

	raw := strings.TrimSpace(os.Getenv("NODE_GROUPS"))
	if raw == "" {
		return nil, fmt.Errorf("NODE_GROUPS is required (JSON list, one entry per edge nodepool)")
	}
	if err := json.Unmarshal([]byte(raw), &c.Groups); err != nil {
		return nil, fmt.Errorf("parse NODE_GROUPS: %w", err)
	}
	if len(c.Groups) == 0 {
		return nil, fmt.Errorf("NODE_GROUPS is empty")
	}
	for _, g := range c.Groups {
		if g.ID == "" || g.NamePrefix == "" {
			return nil, fmt.Errorf("every node group needs id + name_prefix (got %+v)", g)
		}
		if g.NodepoolID == "" {
			g.NodepoolID = g.ID
		}
		if g.Max == 0 {
			g.Max = 4
		}
		if g.InstanceType == "" {
			g.InstanceType = c.DefInstance
		}
		if g.ImageID == "" {
			g.ImageID = c.DefImage
		}
		if g.DiskCategory == "" {
			g.DiskCategory = c.DefDiskCat
		}
		if g.DiskSize == "" {
			g.DiskSize = json.Number(c.DefDiskSize)
		}
		if g.Keypair == "" {
			g.Keypair = c.DefKeypair
		}
		if g.Bandwidth == "" {
			g.Bandwidth = json.Number(c.DefBandwidth)
		}
		if g.NetworkID == "" {
			g.NetworkID = c.DefNetwork
		}
		if g.VSwitchID == "" {
			g.VSwitchID = c.DefVSwitch
		}
		if g.SecurityGroupID == "" {
			g.SecurityGroupID = c.DefSecurity
		}
		if g.PublicIP == nil {
			v := c.DefPublicIP
			g.PublicIP = &v
		}
		if g.Labels == nil {
			g.Labels = map[string]string{}
		}
	}
	// guard: prefixes must be disjoint (no prefix may be a prefix of another,
	// and no two may be EQUAL — the old `p != q` guard let duplicates through),
	// since instance ownership is scoped purely by InstanceName prefix.
	prefixes := make([]string, 0, len(c.Groups))
	seenPrefix := map[string]string{}
	for _, g := range c.Groups {
		if other, dup := seenPrefix[g.NamePrefix]; dup {
			return nil, fmt.Errorf("groups %s and %s share name_prefix %q — prefixes must be unique", other, g.ID, g.NamePrefix)
		}
		seenPrefix[g.NamePrefix] = g.ID
		prefixes = append(prefixes, g.NamePrefix)
	}
	sort.Slice(prefixes, func(i, j int) bool { return len(prefixes[i]) > len(prefixes[j]) })
	for i, p := range prefixes {
		for _, q := range prefixes[i+1:] {
			if strings.HasPrefix(p, q) {
				return nil, fmt.Errorf("node-group name_prefix %q is a prefix of %q — make them disjoint", q, p)
			}
		}
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// validate enforces the startup contract (plan 1.1): a config error must kill
// the provider at boot with a group-specific message, never surface later as
// an RPC failure or - worst - a resource.MustParse panic at the FIRST
// scale-from-zero TemplateNodeInfo call.
func (c *Config) validate() error {
	validEffects := map[string]bool{"": true, "NoSchedule": true, "PreferNoSchedule": true, "NoExecute": true}
	seenID := map[string]bool{}
	seenNP := map[string]bool{}
	// shared default template must parse too (it backs every group without one)
	if err := validateTemplate("shared default", &c.DefTemplate); err != nil {
		return err
	}
	for _, g := range c.Groups {
		if seenID[g.ID] {
			return fmt.Errorf("[%s] duplicate group id", g.ID)
		}
		seenID[g.ID] = true
		if seenNP[g.NodepoolID] {
			return fmt.Errorf("[%s] duplicate nodepool_id %s", g.ID, g.NodepoolID)
		}
		seenNP[g.NodepoolID] = true
		if g.Min < 0 {
			return fmt.Errorf("[%s] min %d < 0", g.ID, g.Min)
		}
		if g.Max < g.Min {
			return fmt.Errorf("[%s] max %d < min %d", g.ID, g.Max, g.Min)
		}
		if _, err := g.DiskSize.Int64(); err != nil {
			return fmt.Errorf("[%s] disk_size %q is not a number: %v", g.ID, g.DiskSize, err)
		}
		if _, err := g.Bandwidth.Int64(); err != nil {
			return fmt.Errorf("[%s] bandwidth %q is not a number: %v", g.ID, g.Bandwidth, err)
		}
		// P0-1 (2026-08 review): labels, taints and the nodepool id are embedded
		// SINGLE-QUOTED inside the join shell script (userdata.go's
		// --node-spec='…'); json.Marshal escapes <>&" but NOT the single quote,
		// so a ' would terminate the quoting and hand the rest of the JSON to
		// the shell on every new node. Enforcing Kubernetes label syntax closes
		// that class completely (its charset carries no shell metacharacter)
		// AND fails at boot what the kubelet would reject at join anyway.
		for k, v := range g.Labels {
			if errs := validation.IsQualifiedName(k); len(errs) > 0 {
				return fmt.Errorf("[%s] label key %q is invalid: %s", g.ID, k, strings.Join(errs, "; "))
			}
			if errs := validation.IsValidLabelValue(v); len(errs) > 0 {
				return fmt.Errorf("[%s] label %q value %q is invalid: %s", g.ID, k, v, strings.Join(errs, "; "))
			}
		}
		for _, t := range g.Taints {
			if !validEffects[t.Effect] {
				return fmt.Errorf("[%s] taint %s has invalid effect %q", g.ID, t.Key, t.Effect)
			}
			if t.Key == "" {
				return fmt.Errorf("[%s] taint with empty key", g.ID)
			}
			if errs := validation.IsQualifiedName(t.Key); len(errs) > 0 {
				return fmt.Errorf("[%s] taint key %q is invalid: %s", g.ID, t.Key, strings.Join(errs, "; "))
			}
			if errs := validation.IsValidLabelValue(t.Value); len(errs) > 0 {
				return fmt.Errorf("[%s] taint %q value %q is invalid: %s", g.ID, t.Key, t.Value, strings.Join(errs, "; "))
			}
		}
		// NodepoolID rides into the node-spec as the desired-nodepool label
		// VALUE (userdata.go joinLabels) and onto the template node — same
		// embedding, same rule.
		if errs := validation.IsValidLabelValue(g.NodepoolID); len(errs) > 0 {
			return fmt.Errorf("[%s] nodepool_id %q is not a valid label value: %s", g.ID, g.NodepoolID, strings.Join(errs, "; "))
		}
		// I4: a group on a NON-default instance type MUST carry its own
		// measured template — silently inheriting the default let sn1.tiny
		// simulate as a 2x larger sn1.small (the stream-group bug, design record §12.1).
		if g.InstanceType != c.DefInstance && g.Template == nil {
			return fmt.Errorf("[%s] instance_type %s differs from the default %s but has no template — add a measured template block", g.ID, g.InstanceType, c.DefInstance)
		}
		if g.Template != nil {
			if err := validateTemplate(g.ID, g.Template); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateTemplate(gid string, t *Template) error {
	for name, v := range map[string]string{
		"cpu": t.CPU, "memory": t.Memory, "pods": t.Pods,
		"ephemeral": t.Ephemeral, "cpu_alloc": t.CPUAlloc, "mem_alloc": t.MemAlloc,
	} {
		if v == "" {
			continue // empty fields inherit the shared default at build time
		}
		q, err := resource.ParseQuantity(v)
		if err != nil {
			return fmt.Errorf("[%s] template %s=%q does not parse: %v", gid, name, v, err)
		}
		if q.Sign() <= 0 {
			return fmt.Errorf("[%s] template %s=%q must be positive", gid, name, v)
		}
	}
	// allocatable must not exceed capacity where both are set
	if t.CPUAlloc != "" && t.CPU != "" {
		alloc, cap := resource.MustParse(t.CPUAlloc), resource.MustParse(t.CPU)
		if alloc.Cmp(cap) > 0 {
			return fmt.Errorf("[%s] template cpu_alloc %s > cpu %s", gid, t.CPUAlloc, t.CPU)
		}
	}
	if t.MemAlloc != "" && t.Memory != "" {
		alloc, cap := resource.MustParse(t.MemAlloc), resource.MustParse(t.Memory)
		if alloc.Cmp(cap) > 0 {
			return fmt.Errorf("[%s] template mem_alloc %s > memory %s", gid, t.MemAlloc, t.Memory)
		}
	}
	return nil
}

func (c *Config) GroupByID(id string) *Group {
	for _, g := range c.Groups {
		if g.ID == id {
			return g
		}
	}
	return nil
}

func (c *Config) GroupForNodeLabels(labels map[string]string) *Group {
	npid := labels[NodepoolLabel]
	if npid == "" {
		return nil
	}
	for _, g := range c.Groups {
		if g.NodepoolID == npid {
			return g
		}
	}
	return nil
}

// GroupForInstanceName resolves the single node group that owns an ENS
// instance name. Prefixes are required to be disjoint at startup, but keeping
// longest-prefix resolution here aligned with describeRegion makes the
// ownership rule explicit and prevents the read and delete paths from
// diverging if that validation ever changes.
func (c *Config) GroupForInstanceName(name string) *Group {
	var owner *Group
	for _, candidate := range c.Groups {
		if strings.HasPrefix(name, candidate.NamePrefix) &&
			(owner == nil || len(candidate.NamePrefix) > len(owner.NamePrefix)) {
			owner = candidate
		}
	}
	return owner
}

func (g *Group) Debug() string {
	keys := make([]string, 0, len(g.Labels))
	for k := range g.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return fmt.Sprintf("ens edge nodepool %s prefix=%s type=%s disk=%s [%d,%d] labels=%v",
		g.NodepoolID, g.NamePrefix, g.InstanceType, g.DiskCategory, g.Min, g.Max, keys)
}

// ProviderID mirrors the cloud-node format <region>.<instance-id>; we return
// the SAME string as the externalgrpc Instance.id so CA can correlate.
func (c *Config) ProviderID(instanceID string) string {
	return c.EnsRegion + "." + instanceID
}

func InstanceIDFromProviderID(pid string) string {
	if i := strings.Index(pid, "."); i > 0 {
		return pid[i+1:]
	}
	return ""
}

var ensInstanceIDPattern = regexp.MustCompile(`^i-[a-z0-9]+$`)

// StrictEdgeInstanceID parses provider IDs on mutating paths. Read-only
// correlation intentionally keeps using InstanceIDFromProviderID, but an ENS
// release candidate must be from this provider's exact region and have the
// documented ENS instance-ID shape.
func (c *Config) StrictEdgeInstanceID(providerID string) string {
	prefix := c.EnsRegion + "."
	if !strings.HasPrefix(providerID, prefix) {
		return ""
	}
	instanceID := strings.TrimPrefix(providerID, prefix)
	if !ensInstanceIDPattern.MatchString(instanceID) {
		return ""
	}
	return instanceID
}
