package main

// Tests for the P0-2 bounded-timeout boundary (2026-08 review): tea maps
// RuntimeOptions.ReadTimeout straight onto http.Client.Timeout, and an unset
// value means NO deadline — under the group order lock and the global fetchMu
// single-flight that is a whole-provider wedge. Three layers here:
//   1. AST scan: no options-less SDK call may reappear in ens.go;
//   2. orderBudget: one atomic refuse-or-budget decision — the submit path
//      is never unbounded and never outlives a caller deadline (C-02/C-09);
//   3. a live hanging-endpoint test proving describeRegion actually returns.

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	openapiv1 "github.com/alibabacloud-go/darabonba-openapi/client"
	ens "github.com/alibabacloud-go/ens-20171110/v3/client"
	"github.com/alibabacloud-go/tea/tea"
	"github.com/aliyun/credentials-go/credentials"
)

// TestCloudCallsAreBounded enforces the P0-2 boundary rule structurally
// (C-08 upgraded it from string matching to AST inspection): EVERY method
// call on the `.ens` / `.cs` clients inside ens.go must be one of the
// allowlisted bounded forms — the *WithOptions variants (which take
// RuntimeOptions) or DoRPCRequest (whose last argument is the
// orderBudget-computed runtime). A newly added SDK operation in any spelling
// or line layout lands here as a failure until it is called bounded.
// Precise claim: this covers calls on those two client fields in ens.go —
// it does not scan other files (none hold SDK clients) or non-SDK I/O.
func TestCloudCallsAreBounded(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "ens.go", nil, 0)
	if err != nil {
		t.Fatalf("parse ens.go: %v", err)
	}
	allowed := map[string]bool{
		"DescribeInstancesWithOptions":            true,
		"ReleasePostPaidInstanceWithOptions":      true,
		"DescribeClusterAttachScriptsWithOptions": true,
		"DoRPCRequest": true, // runtime is its last argument
	}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		recv, ok := sel.X.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if recv.Sel.Name != "ens" && recv.Sel.Name != "cs" {
			return true
		}
		if !allowed[sel.Sel.Name] {
			t.Errorf("ens.go:%d: .%s.%s(...) is not on the bounded-call allowlist — use a *WithOptions variant with sdkRuntime()/orderBudget()",
				fset.Position(call.Pos()).Line, recv.Sel.Name, sel.Sel.Name)
		}
		return true
	})
	// An EMPTY RuntimeOptions means no timeout; gofmt renders the empty
	// composite literal on one line, so a textual check suffices for it.
	src, err := os.ReadFile("ens.go")
	if err != nil {
		t.Fatalf("read ens.go: %v", err)
	}
	for i, line := range strings.Split(string(src), "\n") {
		if strings.Contains(line, "&util.RuntimeOptions{}") {
			t.Errorf("ens.go:%d builds an EMPTY RuntimeOptions (= no timeout): %s", i+1, strings.TrimSpace(line))
		}
	}
}

// C-02 + C-09: the ONE atomic order-budget decision, driven by an explicit
// clock so every case — including the truncation boundary that C-09 caught in
// the split preSubmitCtxErr/orderRuntime design — is deterministic.
func TestOrderBudget(t *testing.T) {
	now := time.Now()

	// no deadline: pass, with the 60s default (its ONLY legal use)
	rt, err := orderBudget(context.Background(), now)
	if err != nil {
		t.Fatalf("no-deadline context must pass: %v", err)
	}
	if got := tea.IntValue(rt.ReadTimeout); got != orderReadTimeoutMs {
		t.Errorf("no-deadline ReadTimeout = %d, want the %dms default", got, orderReadTimeoutMs)
	}
	if got := tea.IntValue(rt.ConnectTimeout); got != sdkConnectTimeoutMs {
		t.Errorf("ConnectTimeout = %d, want %d", got, sdkConnectTimeoutMs)
	}

	// a live CA deadline: pass, budget = the remaining deadline
	live, cancelLive := context.WithDeadline(context.Background(), now.Add(10*time.Second))
	defer cancelLive()
	rt, err = orderBudget(live, now)
	if err != nil {
		t.Fatalf("10s-remaining deadline must pass: %v", err)
	}
	if got := tea.IntValue(rt.ReadTimeout); got != 10000 {
		t.Errorf("deadline-derived ReadTimeout = %d, want 10000", got)
	}

	// C-09 boundary, just over the gate: 500ms + 1ns passes, and the budget
	// is the truncated ~500ms remaining — NEVER the 60s default
	edge, cancelEdge := context.WithDeadline(context.Background(), now.Add(500*time.Millisecond+time.Nanosecond))
	defer cancelEdge()
	rt, err = orderBudget(edge, now)
	if err != nil {
		t.Fatalf("500ms+1ns remaining must pass the >500ms gate: %v", err)
	}
	if got := tea.IntValue(rt.ReadTimeout); got != 500 {
		t.Errorf("500ms+1ns remaining: ReadTimeout = %d, want 500 (the truncated remainder, NOT the %dms default)", got, orderReadTimeoutMs)
	}
	// ...and one full millisecond over: budget 501, still never the default
	edge2, cancelEdge2 := context.WithDeadline(context.Background(), now.Add(501*time.Millisecond))
	defer cancelEdge2()
	if rt, err = orderBudget(edge2, now); err != nil || tea.IntValue(rt.ReadTimeout) != 501 {
		t.Errorf("501ms remaining: rt=%v err=%v, want ReadTimeout 501", rt, err)
	}

	// C-09 boundary, at the gate: exactly 500ms is refused
	at, cancelAt := context.WithDeadline(context.Background(), now.Add(500*time.Millisecond))
	defer cancelAt()
	if _, err := orderBudget(at, now); err == nil {
		t.Errorf("exactly 500ms remaining must be refused")
	}
	// ...and below it
	near, cancelNear := context.WithDeadline(context.Background(), now.Add(300*time.Millisecond))
	defer cancelNear()
	if _, err := orderBudget(near, now); err == nil {
		t.Errorf("300ms remaining must be refused")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := orderBudget(canceled, now); err == nil {
		t.Errorf("canceled context must be refused")
	}

	expired, cancelExp := context.WithDeadline(context.Background(), now.Add(-time.Second))
	defer cancelExp()
	if _, err := orderBudget(expired, now); err == nil {
		t.Errorf("expired deadline must be refused")
	}

	if got := tea.IntValue(sdkRuntime().ReadTimeout); got != sdkReadTimeoutMs {
		t.Errorf("sdkRuntime ReadTimeout = %d, want %d", got, sdkReadTimeoutMs)
	}
}

// TestDescribeRegionBoundedOnHangingEndpoint proves the fix end to end: a
// DescribeInstances against an endpoint that accepts the connection, READS
// the request, and never answers must error out AT the ReadTimeout bound
// instead of holding fetchMu / the order lock forever (the pre-fix behavior:
// http.Client.Timeout stayed 0 = no deadline). C-06 hardened the assertions:
// the listener must have read request bytes (an immediate credential/signing
// error cannot pass), the elapsed time must bracket the configured timeout,
// and the error must classify as a timeout-class transport error via the
// provider's own isAmbiguousTransport — not a fragile full-string match.
func TestDescribeRegionBoundedOnHangingEndpoint(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	requestRead := make(chan struct{}, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1)
				if _, err := c.Read(buf); err == nil {
					select {
					case requestRead <- struct{}{}:
					default:
					}
				}
				io.Copy(io.Discard, c) // swallow the rest, never respond
			}(conn)
		}
	}()

	oldRead := sdkReadTimeoutMs
	sdkReadTimeoutMs = 1000
	defer func() { sdkReadTimeoutMs = oldRead }()

	// static AK/SK short-circuit the credential chain locally (no metadata call)
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "test-ak")
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "test-sk")
	cred, err := credentials.NewCredential(nil)
	if err != nil {
		t.Fatalf("credentials: %v", err)
	}
	client, err := ens.NewClient(&openapiv1.Config{
		Credential: cred,
		Endpoint:   tea.String(ln.Addr().String()),
		Protocol:   tea.String("http"),
	})
	if err != nil {
		t.Fatalf("ens client: %v", err)
	}

	c := &Cloud{cfg: &Config{EnsRegion: "ens-region-1", DescribeTTLSeconds: 10}, ens: client}
	start := time.Now()
	_, derr := c.describeRegion(true)
	elapsed := time.Since(start)
	if derr == nil {
		t.Fatalf("describe against a hanging endpoint must error")
	}
	// the request must have REACHED the hanging listener — otherwise the
	// error came from somewhere earlier (credentials, signing, client
	// construction) and proves nothing about the read timeout
	select {
	case <-requestRead:
	default:
		t.Fatalf("listener never read a request byte — error is not from the hung read: %v", derr)
	}
	// elapsed must bracket the configured 1000ms ReadTimeout
	if elapsed < 900*time.Millisecond {
		t.Fatalf("returned after only %s — faster than the 1000ms ReadTimeout, so the timeout did not decide this: %v", elapsed, derr)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("returned only after %s — ReadTimeout not honored (unbounded hang)", elapsed)
	}
	// timeout classification via the provider's own transport classifier
	if !isAmbiguousTransport(derr) {
		t.Fatalf("error does not classify as a timeout-class transport error: %v", derr)
	}
}
