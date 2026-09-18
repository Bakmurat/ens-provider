package main

// Alibaba Cloud API glue: ENS instance lifecycle + ACK attach tokens.
//
// Auth: github.com/aliyun/credentials-go resolves the credential chain from the
// STANDARD env vars the chart already sets — ALIBABA_CLOUD_ROLE_ARN +
// ALIBABA_CLOUD_OIDC_PROVIDER_ARN + ALIBABA_CLOUD_OIDC_TOKEN_FILE selects the
// RRSA/OIDC provider natively (no aliyun-CLI config file, no `mode: OIDC`
// gotcha). AK/SK env vars work as a fallback for local runs.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	cs "github.com/alibabacloud-go/cs-20151215/v5/client"
	openapiv1 "github.com/alibabacloud-go/darabonba-openapi/client" // ens SDK is darabonba v1
	openapiv2 "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	ens "github.com/alibabacloud-go/ens-20171110/v3/client"
	openapiutil "github.com/alibabacloud-go/openapi-util/service"
	util "github.com/alibabacloud-go/tea-utils/v2/service"
	"github.com/alibabacloud-go/tea/tea"
	"github.com/aliyun/credentials-go/credentials"
)

type Instance struct {
	ID           string
	Name         string
	GroupID      string
	Status       string // Running / Pending / Starting / Stopped / ...
	PublicIP     string
	PrivateIP    string
	CreationTime string // RFC3339 from DescribeInstances (janitor age source)
}

// Bounded SDK timeouts (2026-08 review P0-2): tea sets http.Client.Timeout
// straight from RuntimeOptions.ReadTimeout, so an options-less call has NO
// deadline (tea v1.3.11 tea.go:389; Timeout 0 = unbounded in net/http). One
// hung DescribeInstances would then hold the per-group order lock
// (reservations.go) and the process-global fetchMu single-flight forever —
// wedging every scale-up and every cache refresh while readiness (Kubernetes
// reachability only, BY DESIGN) never restarts the pod. Every ENS/CS call
// therefore goes through its *WithOptions variant with these bounds; the
// source-scan in ens_test.go locks the rule. Vars, not consts, so the
// bounded-hang regression test can shrink them.
var (
	sdkConnectTimeoutMs = 5000
	sdkReadTimeoutMs    = 30000 // one Describe page / one release / one token fetch
	// Orders default higher: a premature cutoff manufactures an ambiguous
	// outcome (reservation held, reconcile-by-listing) — safe but not free.
	// Used ONLY for no-deadline callers; a caller deadline is the budget
	// otherwise (orderBudget, C-09).
	orderReadTimeoutMs = 60000
)

func sdkRuntime() *util.RuntimeOptions {
	return &util.RuntimeOptions{
		ConnectTimeout: tea.Int(sdkConnectTimeoutMs),
		ReadTimeout:    tea.Int(sdkReadTimeoutMs),
	}
}

// orderBudget makes the ONE atomic caller-deadline decision for an order
// (C-02 + C-09): it either refuses the caller, or returns the exact
// RuntimeOptions the submit MUST use — both derived from the SAME deadline
// read and the SAME clock sample, so no later re-read or unit truncation can
// disagree with the gate (the C-09 defect: a >500ms nanosecond-precision gate
// paired with a truncating milliseconds check promoted a 500ms+ε caller to
// the 60s default; DoRPCRequest carries no Go context, so nothing would have
// canceled that request when the caller's deadline passed).
//
// Rules:
//   - canceled/expired context, or ≤500ms remaining -> refused. A refusal
//     means no RunInstances order (no mutating submit) reached ENS and no
//     reservation was created — the preceding inventory is a READ that has
//     already happened (wording per C-10).
//   - a deadline-carrying caller is NEVER given the 60s default: its submit
//     budget IS the remaining deadline (truncation only shaves <1ms off,
//     and the gate guarantees ≥500ms of it).
//   - the 60s default applies exclusively to contexts with NO deadline
//     (background/manual callers).
func orderBudget(ctx context.Context, now time.Time) (*util.RuntimeOptions, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("order not submitted: caller context is dead (%w)", err)
	}
	rt := &util.RuntimeOptions{
		ConnectTimeout: tea.Int(sdkConnectTimeoutMs),
		ReadTimeout:    tea.Int(orderReadTimeoutMs), // no-deadline callers only
	}
	if dl, ok := ctx.Deadline(); ok {
		rem := dl.Sub(now)
		if rem <= 500*time.Millisecond {
			return nil, fmt.Errorf("order not submitted: only %s left on the caller deadline (need >500ms)", rem.Round(time.Millisecond))
		}
		rt.ReadTimeout = tea.Int(int(rem.Milliseconds()))
	}
	return rt, nil
}

type Cloud struct {
	cfg *Config
	ens *ens.Client
	cs  *cs.Client

	mu      sync.Mutex
	cacheAt time.Time
	cache   []Instance
	// fetchMu serializes cache refreshes (v0.1.12, design plan): concurrent
	// expiry callers previously all hit the ENS API (thundering herd).
	fetchMu sync.Mutex

	// per-group order state (v0.1.10 lock, upgraded v0.1.22): serializes
	// orders AND carries the not-yet-Describe-visible reservations the
	// ceiling guard needs (see reservations.go).
	ordMu  sync.Mutex
	orders map[string]*groupOrderState
}

func (c *Cloud) orderState(id string) *groupOrderState {
	c.ordMu.Lock()
	defer c.ordMu.Unlock()
	if c.orders == nil {
		c.orders = map[string]*groupOrderState{}
	}
	if c.orders[id] == nil {
		c.orders[id] = newGroupOrderState()
	}
	return c.orders[id]
}

func NewCloud(cfg *Config) (*Cloud, error) {
	cred, err := credentials.NewCredential(nil) // env chain: OIDC (RRSA) > AK/SK
	if err != nil {
		return nil, fmt.Errorf("credentials: %w", err)
	}
	ensClient, err := ens.NewClient(&openapiv1.Config{
		Credential: cred,
		// ens.ap-southeast-1.aliyuncs.com, NOT ens.aliyuncs.com: the bare
		// hostname is the mainland-China site; on an international account the
		// commerce layer rejects orders arriving under that Host with a generic
		// OrderFailed (Alibaba support read our failing RequestIds server-side,
		// 2026-07-16). Reads served either way — only orders exposed it. The
		// CLI never hit this: it resolves the endpoint from the profile region
		// via the Location service.
		Endpoint: tea.String("ens.ap-southeast-1.aliyuncs.com"),
	})
	if err != nil {
		return nil, fmt.Errorf("ens client: %w", err)
	}
	csClient, err := cs.NewClient(&openapiv2.Config{
		Credential: cred,
		Endpoint:   tea.String("cs." + cfg.ControlRegion + ".aliyuncs.com"),
	})
	if err != nil {
		return nil, fmt.Errorf("cs client: %w", err)
	}
	return &Cloud{cfg: cfg, ens: ensClient, cs: csClient}, nil
}

// InvalidateCache forces the next describe to hit the API (called on Refresh).
func (c *Cloud) InvalidateCache() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cacheAt = time.Time{}
}

// describeRegion lists all ENS instances in the region owned by ANY group
// (matched by InstanceName prefix; longest prefix wins), excluding
// Released/Releasing. One region-wide call per TTL window protects the ENS API
// from throttling (CA calls NodeGroupNodes every loop with no CA-side cache).
func (c *Cloud) describeRegion(force bool) ([]Instance, error) {
	fresh := func() ([]Instance, bool) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if !force && c.cache != nil && time.Since(c.cacheAt) < time.Duration(c.cfg.DescribeTTLSeconds)*time.Second {
			return c.cache, true
		}
		return nil, false
	}
	if out, ok := fresh(); ok {
		return out, nil
	}
	// single-flight: one fetcher at a time; late arrivals re-check the cache
	// another fetcher may just have refreshed.
	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()
	if out, ok := fresh(); ok {
		return out, nil
	}

	var owned []Instance
	for page := int32(1); ; page++ {
		resp, err := c.ens.DescribeInstancesWithOptions(&ens.DescribeInstancesRequest{
			EnsRegionId: tea.String(c.cfg.EnsRegion),
			PageSize:    tea.String("100"),
			PageNumber:  tea.Int32(page),
		}, sdkRuntime())
		if err != nil {
			return nil, fmt.Errorf("DescribeInstances page %d: %s", page, sanitizeErr(err))
		}
		body := resp.Body
		if body == nil || body.Instances == nil {
			break
		}
		for _, i := range body.Instances.Instance {
			name := tea.StringValue(i.InstanceName)
			gid := ""
			longest := -1
			for _, g := range c.cfg.Groups {
				if strings.HasPrefix(name, g.NamePrefix) && len(g.NamePrefix) > longest {
					gid, longest = g.ID, len(g.NamePrefix)
				}
			}
			if gid == "" {
				continue
			}
			status := tea.StringValue(i.Status)
			if status == "Released" || status == "Releasing" {
				continue
			}
			inst := Instance{
				ID:           tea.StringValue(i.InstanceId),
				Name:         name,
				GroupID:      gid,
				Status:       status,
				CreationTime: tea.StringValue(i.CreationTime),
			}
			if i.PublicIpAddress != nil && len(i.PublicIpAddress.IpAddress) > 0 {
				inst.PublicIP = tea.StringValue(i.PublicIpAddress.IpAddress[0])
			}
			if i.PrivateIpAddresses != nil && len(i.PrivateIpAddresses.PrivateIpAddress) > 0 {
				inst.PrivateIP = tea.StringValue(i.PrivateIpAddresses.PrivateIpAddress[0].Ip)
			}
			if inst.PrivateIP == "" && i.NetworkAttributes != nil &&
				i.NetworkAttributes.PrivateIpAddress != nil &&
				len(i.NetworkAttributes.PrivateIpAddress.IpAddress) > 0 {
				inst.PrivateIP = tea.StringValue(i.NetworkAttributes.PrivateIpAddress.IpAddress[0])
			}
			owned = append(owned, inst)
		}
		if int32(page)*100 >= tea.Int32Value(body.TotalCount) {
			break
		}
	}

	c.mu.Lock()
	c.cache, c.cacheAt = owned, time.Now()
	c.mu.Unlock()
	return owned, nil
}

func (c *Cloud) OwnedInstances(g *Group, force bool) ([]Instance, error) {
	all, err := c.describeRegion(force)
	if err != nil {
		return nil, err
	}
	var out []Instance
	for _, i := range all {
		if i.GroupID == g.ID {
			out = append(out, i)
		}
	}
	return out, nil
}

// InstanceByID performs an uncached, unfiltered lookup of one ENS instance.
// The normal region inventory intentionally hides names outside the configured
// group prefixes, so it cannot distinguish an already-gone instance from a
// live foreign instance. DeleteNodes uses this lookup only when the owned
// inventory has no matching ID and then applies the exact group ownership gate.
func (c *Cloud) InstanceByID(instanceID string) (Instance, bool, error) {
	resp, err := c.ens.DescribeInstancesWithOptions(&ens.DescribeInstancesRequest{
		EnsRegionId: tea.String(c.cfg.EnsRegion),
		InstanceId:  tea.String(instanceID),
		PageSize:    tea.String("10"),
	}, sdkRuntime())
	if err != nil {
		return Instance{}, false, fmt.Errorf("DescribeInstances(%s): %s", instanceID, sanitizeErr(err))
	}
	if resp.Body == nil || resp.Body.Instances == nil {
		return Instance{}, false, nil
	}
	for _, item := range resp.Body.Instances.Instance {
		if tea.StringValue(item.InstanceId) != instanceID {
			continue
		}
		state := tea.StringValue(item.Status)
		if state == "Released" || state == "Releasing" {
			return Instance{}, false, nil
		}
		return Instance{
			ID:     instanceID,
			Name:   tea.StringValue(item.InstanceName),
			Status: state,
		}, true, nil
	}
	return Instance{}, false, nil
}

func (c *Cloud) AllOwnedInstances(force bool) ([]Instance, error) {
	return c.describeRegion(force)
}

var tokenRe = regexp.MustCompile(`--openapi-token=(\S+)`)

// buildAttachScriptsRequest is a PURE builder (v0.1.23): it takes `now`
// explicitly so tests can assert the exact Expired timestamp instead of a
// flaky wall-clock window. JoinTokenTTL == 0 means the field stays nil and
// Alibaba's documented 1h default validity applies (NOT "non-expiring").
func buildAttachScriptsRequest(cfg *Config, g *Group, now time.Time) *cs.DescribeClusterAttachScriptsRequest {
	req := &cs.DescribeClusterAttachScriptsRequest{
		NodepoolId: tea.String(g.NodepoolID),
		Arch:       tea.String("amd64"),
	}
	if cfg.JoinTokenTTL > 0 {
		// Absolute UNIX timestamp per the API contract, computed from the
		// provider's clock. One wave = one shared token: this must outlive
		// the SLOWEST machine's issuance->first-authenticated-call window
		// plus the join-retry tail (see Config.JoinTokenTTL).
		req.Expired = tea.Int64(now.Add(cfg.JoinTokenTTL).Unix())
	}
	return req
}

// FetchAttachToken gets a fresh, nodepool-scoped edge attach token (fetched
// per scale-up so the node joins the RIGHT pool; validity = JOIN_TOKEN_TTL,
// contract-backed via the request's Expired field).
func (c *Cloud) FetchAttachToken(g *Group) (string, error) {
	resp, err := c.cs.DescribeClusterAttachScriptsWithOptions(tea.String(c.cfg.ClusterID),
		buildAttachScriptsRequest(c.cfg, g, time.Now()), map[string]*string{}, sdkRuntime())
	if err != nil {
		return "", fmt.Errorf("DescribeClusterAttachScripts: %s", sanitizeErr(err))
	}
	m := tokenRe.FindStringSubmatch(tea.StringValue(resp.Body))
	if m == nil {
		return "", fmt.Errorf("could not parse attach token from script (%d bytes)", len(tea.StringValue(resp.Body)))
	}
	return m[1], nil
}

// sanitizeErr strips query strings from error text before it reaches ANY log
// or gRPC status: the Go HTTP client embeds the full request URL in transport
// errors - for a signed classic-RPC GET that URL carries the STS token, the
// signature AND the base64 userdata (attach token inside). Found live by the
// N-3 drill 2026-07-19. BOUNDARY RULE (v0.1.20): every error born from an
// aliyun SDK call is scrubbed HERE in ens.go before it is returned or logged
// - downstream loggers (server/janitor/main) may then print cloud errors
// verbatim. The raw error must still be inspected (isInstanceGone,
// isAmbiguousTransport) BEFORE wrapping; sanitize at the return, not before.
var urlQueryRe = regexp.MustCompile(`\?[^"\s]*`)

func sanitizeErr(err error) string {
	if err == nil {
		return ""
	}
	return urlQueryRe.ReplaceAllString(err.Error(), "?REDACTED")
}

// ErrExceedsMax marks an order refused by the authoritative under-lock
// ceiling check (backlog item 15.3). The gRPC layer maps it to OutOfRange so CA
// backs off the group instead of treating it as a provider fault.
var ErrExceedsMax = errors.New("would exceed group max")

// maxGuard is the pure ceiling decision: owned+count must fit max.
func maxGuard(gid string, owned int, count, max int32) error {
	if int32(owned)+count > max {
		return fmt.Errorf("[%s] increase by %d with owned=%d %w %d", gid, count, owned, ErrExceedsMax, max)
	}
	return nil
}

// isAmbiguousTransport: did the request POSSIBLY reach ENS although we got no
// usable response? Timeouts and mid-flight connection failures are ambiguous
// (the order may have been accepted); clean API rejections are NOT.
func isAmbiguousTransport(err error) bool {
	if err == nil {
		return false
	}
	m := strings.ToLower(err.Error())
	for _, sig := range []string{"timeout", "deadline", "eof", "connection reset", "broken pipe", "canceled"} {
		if strings.Contains(m, sig) {
			return true
		}
	}
	return false
}

func instancesWithPrefix(all []Instance, prefix string) []string {
	var ids []string
	for _, i := range all {
		if strings.HasPrefix(i.Name, prefix) {
			ids = append(ids, i.ID)
		}
	}
	return ids
}

// awaitOpInstances polls the region for instances carrying THIS operation's
// name prefix - the reconcile-by-listing that replaces a durable journal
// (design record §12.5): order-accept to Describe-visibility is seconds.
// Context-aware (v0.1.25, C-03): a demoted or shutting-down leader
// must stop polling; aborting returns nil, which submitOrder classifies as
// orderUncertain — the reservation stays held, failing toward temporary
// under-scaling, never past max.
func (c *Cloud) awaitOpInstances(ctx context.Context, opName string, want int32, attempts int) []string {
	for a := 0; a < attempts; a++ {
		select {
		case <-ctx.Done():
			log.Printf("[ens] awaitOpInstances op=%s aborted (%v) - outcome stays uncertain", opName, ctx.Err())
			return nil
		case <-time.After(5 * time.Second):
		}
		all, err := c.describeRegion(true)
		if err != nil {
			continue
		}
		ids := instancesWithPrefix(all, opName)
		if int32(len(ids)) >= want {
			return ids
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	all, err := c.describeRegion(true)
	if err != nil {
		return nil
	}
	return instancesWithPrefix(all, opName)
}

// RunInstances creates `count` ENS edge instances for the group; they
// self-join via UserData. Uses the generic OpenAPI call because the typed
// SDK request omits SystemDisk.Category — and ENS inventory at the target site is
// disk-category-specific (local_ssd), so Category must be sent.
func (c *Cloud) RunInstances(ctx context.Context, g *Group, count int32) ([]string, error) {
	if count <= 0 {
		return nil, nil
	}
	// Preparation (userdata fetch, op naming, query build) runs BEFORE the
	// order lock: it cannot fail in a way that reaches ENS, so no reservation
	// may exist for it yet, and keeping it outside shortens the critical
	// section. The guard + reservation + submit sequence - the part that must
	// be atomic per group - lives in executeGuardedOrder (v0.1.21 moved the
	// ceiling check under the lock; v0.1.22 added reservations for the ENS
	// Describe-visibility window; see reservations.go).
	userdata, err := BuildUserdata(c, c.cfg, g)
	if err != nil {
		return nil, fmt.Errorf("build userdata: %w", err)
	}
	// operation-scoped name: prefix-op<id>-<ts>. Still owned by the group
	// prefix (HasPrefix), but uniquely identifies THIS order so an ambiguous
	// timeout can be reconciled by listing (no journal - design record §8.7).
	opID := make([]byte, 2)
	if _, err := rand.Read(opID); err != nil {
		return nil, fmt.Errorf("op id: %w", err)
	}
	name := fmt.Sprintf("%s-op%s-%d", g.NamePrefix, hex.EncodeToString(opID), time.Now().Unix())
	sysDisk, _ := json.Marshal(map[string]any{"Size": g.DiskSize, "Category": g.DiskCategory})

	query := map[string]any{
		// RegionId: the CLI injects it from the profile on EVERY call and the
		// console order event records it - the ENS commerce layer appears to
		// need it for billing routing (2026-07-16 hunt; our Describe calls
		// tolerate its absence, orders may not).
		"RegionId":           c.cfg.ControlRegion,
		"EnsRegionId":        c.cfg.EnsRegion,
		"ScheduleAreaLevel":  "Region",
		"NetWorkId":          g.NetworkID,
		"VSwitchId":          g.VSwitchID,
		"SecurityId":         g.SecurityGroupID,
		"InstanceType":       g.InstanceType,
		"ImageId":            g.ImageID,
		"KeyPairName":        g.Keypair,
		"InstanceChargeType": "PostPaid",
		// PAYG billing params. BillingCycle=Hour is THE OrderFailed root cause
		// (found by CLI bisection 2026-07-16): the ENS order system requires it
		// for PostPaid, our call omitted it, and every order failed with the
		// generic "Order failed, please try again" (worked in the June PoC -
		// the requirement is new). InstanceChargeStrategy=instance =
		// "Instance-Based Pay-As-You-Go" (console + terraform-v2 both send it).
		// Proven set = exactly this call (CLI tests A2/B).
		"InstanceChargeStrategy": "instance",
		"BillingCycle":           "Hour",
		"SystemDisk":             string(sysDisk),
		"InstanceName":           name,
		"UniqueSuffix":           "true",
		"Amount":                 fmt.Sprintf("%d", count),
		"UserData":               userdata,
	}
	if *g.PublicIP {
		query["PublicIpIdentification"] = "true"
		query["InternetMaxBandwidthOut"] = g.Bandwidth.String()
	} else {
		// default: PRIVATE worker — cloud<->edge over the dedicated line,
		// public egress (registry/NTP/edgeadm-OSS) via the ENS NAT gateway.
		// InternetMaxBandwidthOut=0 rides along because the proven-working
		// call (terraform-v2 + the CLI bisection) always sends it.
		query["PublicIpIdentification"] = "false"
		query["InternetMaxBandwidthOut"] = "0"
	}

	log.Printf("[ens] RunInstances group=%s amount=%d type=%s region=%s name~=%s public_ip=%v (max %d)",
		g.ID, count, g.InstanceType, c.cfg.EnsRegion, name, *g.PublicIP, g.Max)

	return executeGuardedOrder(ctx, c.orderState(g.ID), g.ID, name, count, g.Max, time.Now(),
		func() ([]Instance, error) { return c.OwnedInstances(g, true) },
		func(rt *util.RuntimeOptions) ([]string, orderOutcome, error) {
			return c.submitOrder(ctx, name, count, query, rt)
		},
	)
}

// submitOrder performs the RunInstances RPC and classifies the outcome
// EXPLICITLY (see orderOutcome) so the reservation layer never has to guess
// from wrapped error text. Called with the group order lock held; rt is the
// gate-computed budget from orderBudget (C-09: submitOrder never samples the
// clock or the deadline itself — the gate's decision is the only decision).
func (c *Cloud) submitOrder(ctx context.Context, name string, count int32, query map[string]any, rt *util.RuntimeOptions) ([]string, orderOutcome, error) {
	// DoRPCRequest, NOT CallApi: CallApi (SignatureAlgorithm unset) routes to
	// DoRequest = ACS3-HMAC-SHA256 HEADER signing (x-acs-action, Authorization:
	// ACS3-...), while every PROVEN successful RunInstances on this account
	// (aliyun CLI testA2/B/C/D incl. testD with THIS role's own STS session)
	// signs the CLASSIC RPC way: Action/Version/Timestamp/AccessKeyId/
	// SecurityToken/SignatureMethod=HMAC-SHA1/Signature all in the QUERY
	// STRING. DoRPCRequest reproduces that byte-for-byte. Describe* calls work
	// over ACS3, so the gateway accepts it — but the ENS commerce layer draws
	// a generic OrderFailed on ACS3-signed orders (2026-07-16 hunt, theory #10
	// after testD eliminated credential type).
	req := &openapiv1.OpenApiRequest{Query: openapiutil.Query(query)}
	raw, err := c.ens.DoRPCRequest(tea.String("RunInstances"), tea.String("2017-11-10"),
		tea.String("HTTPS"), tea.String("GET"), tea.String("AK"), tea.String("json"),
		req, rt)
	if err != nil {
		if !isAmbiguousTransport(err) {
			metrics.ordersFailure.Add(1)
			return nil, orderRejected, fmt.Errorf("RunInstances: %s", sanitizeErr(err))
		}
		metrics.ordersAmbiguous.Add(1)
		// AMBIGUOUS: the order may have been accepted. Reconcile by listing
		// the operation prefix before declaring anything (plan 1.6 / N-3).
		log.Printf("[ens] RunInstances AMBIGUOUS (%s) - reconciling op %s by listing", sanitizeErr(err), name)
		ids := c.awaitOpInstances(ctx, name, count, 6)
		if int32(len(ids)) == count {
			log.Printf("[ens]   op %s reconciled: all %d instances found: %v", name, count, ids)
			metrics.ordersSuccess.Add(1)
			c.InvalidateCache()
			return ids, orderAccepted, nil
		}
		if len(ids) > 0 {
			// partial visibility: report the truth; TargetSize==owned counts
			// what exists and CA's next IncreaseSize orders only the difference
			c.InvalidateCache()
			return ids, orderUncertain, fmt.Errorf("RunInstances ambiguous: %d/%d instances visible for op %s (owned math absorbs the rest)", len(ids), count, name)
		}
		// N-3 finding 2026-07-19: order-accept to Describe-visibility can exceed
		// this window - the owned-math backstop converges it (CA's next loop
		// counts the instance as upcoming; no re-order happened in the drill).
		// v0.1.22: the reservation stays held - the order MAY have landed, and
		// safety prefers temporary under-scaling over exceeding max.
		metrics.ordersFailure.Add(1)
		return nil, orderUncertain, fmt.Errorf("RunInstances ambiguous, nothing appeared for op %s yet (owned math will absorb late arrivals): %s", name, sanitizeErr(err))
	}
	c.InvalidateCache()

	var ids []string
	if body, ok := raw["body"].(map[string]any); ok {
		switch v := body["InstanceIds"].(type) {
		case []any: // {"InstanceIds": ["i-a", "i-b"]} — the ENS shape
			for _, x := range v {
				if s, ok := x.(string); ok {
					ids = append(ids, s)
				}
			}
		case map[string]any: // tolerate the wrapped {"InstanceIds":{"InstanceIds":[...]}} shape
			if inner, ok := v["InstanceIds"].([]any); ok {
				for _, x := range inner {
					if s, ok := x.(string); ok {
						ids = append(ids, s)
					}
				}
			}
		}
	}
	log.Printf("[ens]   -> created: %v", ids)
	metrics.ordersSuccess.Add(1)
	return ids, orderAccepted, nil
}

// ReleaseInstance releases one PayAsYouGo ENS instance. Not-found is
// IDEMPOTENT SUCCESS (v0.1.8, plan 1.3): a retried DeleteNodes after a
// partial failure must converge, not error on the already-released members.
func (c *Cloud) ReleaseInstance(instanceID string) error {
	log.Printf("[ens] ReleasePostPaidInstance %s", instanceID)
	_, err := c.ens.ReleasePostPaidInstanceWithOptions(&ens.ReleasePostPaidInstanceRequest{
		InstanceId: tea.String(instanceID),
	}, sdkRuntime())
	if err != nil {
		if isInstanceGone(err) {
			log.Printf("[ens]   %s already gone - idempotent success", instanceID)
			metrics.released.Add(1)
			c.InvalidateCache()
			return nil
		}
		metrics.releaseFailures.Add(1)
		return fmt.Errorf("ReleasePostPaidInstance %s: %s", instanceID, sanitizeErr(err))
	}
	metrics.released.Add(1)
	c.InvalidateCache()
	return nil
}

// isInstanceGone: is this ReleasePostPaidInstance rejection the API saying
// the instance no longer exists? Decided ONLY by the structured SDK error
// Code (v0.1.24, backlog item 15a): the official already-gone code is
// InstanceIdNotFound (doc 2637441). Message/Data text is prose and is never
// consulted - a fatal error that merely MENTIONS a missing resource must
// stay fatal, or DeleteNodes deletes the Node object while the paid
// instance keeps running and the janitor logs a failing release as
// "already gone" every sweep. Non-SDK errors (transport) are never "gone".
func isInstanceGone(err error) bool {
	var sdkErr *tea.SDKError
	return errors.As(err, &sdkErr) && tea.StringValue(sdkErr.Code) == "InstanceIdNotFound"
}
