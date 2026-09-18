package main

// Edge-node bootstrap userdata. The fix sequence (disk/kernel/DNS + the
// background self-heal for ENS ip-rule + containerd CNI ordering) is the
// validated script from the PoC (the design notes); the join node-spec
// carries THIS group's nodepool, labels and taints. Taints MUST live in the
// node-spec because edgeadm-joined nodes bypass ACK nodepool taint
// application (issue #18).
//
// v0.1.6 (plan 1.2): NO `set -x` — xtrace wrote the edgeadm attach token into
// the node's cloud-init output log (every traced command line lands there;
// the `> /var/log/edge-join.log` redirect only captures the command's
// OUTPUT). Instead: explicit stage-echo lines, the token held in a shell
// variable and unset right after the join, bounded retries with backoff on
// the edgeadm download AND the join, and a durable per-stage marker file
// (/var/log/ens-provider-join-marker) for janitor/N-2 diagnosis. The node-spec JSON
// is byte-identical to v0.1.5 (golden-tested).

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// tokenFetcher lets tests build userdata without a real CS client.
type tokenFetcher interface {
	FetchAttachToken(g *Group) (string, error)
}

func joinLabels(g *Group) map[string]string {
	labels := map[string]string{
		"apps.openyurt.io/desired-nodepool":     g.NodepoolID,
		"alibabacloud.com/interconnection-mode": "private",
	}
	for k, v := range g.Labels {
		labels[k] = v
	}
	return labels
}

type nodeSpecTaint struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	Effect string `json:"effect"`
}

// buildNodeSpec is the console-parity node-spec (captured 2026-07-16 from
// the ACK console's "Add Existing Node" generated script): imageRepoType from config
// (v0.1.18 — default "public": the leased-line default is INTERNAL, which
// the PoC cluster could not route it; the UAT cluster can — it transits the TR's oss-* statics),
// poolNodesConnected (the Inter-node Connection flag's node-spec sibling),
// allowedClusterAddons (replaces the PoC-era edge-enable-addon-* labels).
func buildNodeSpec(c *Config, g *Group) (string, error) {
	taints := make([]nodeSpecTaint, 0, len(g.Taints)) // empty list honored as-is (untainted system pools)
	for _, t := range g.Taints {
		eff := t.Effect
		if eff == "" {
			eff = "NoSchedule"
		}
		taints = append(taints, nodeSpecTaint{Key: t.Key, Value: t.Value, Effect: eff})
	}
	spec, err := json.Marshal(map[string]any{
		"allowedClusterAddons": []string{"kube-proxy", "flannel", "coredns"},
		"imageRepoType":        c.ImageRepoType,
		"interconnectMode":     "private",
		"manageRuntime":        true,
		"openapiType":          "public",
		"poolNodesConnected":   true,
		"quiet":                true,
		"selfHostNtpServer":    false,
		"runtime":              "containerd",
		"runtimeRootDir":       "/var/lib/containerd",
		"runtime_version":      c.RuntimeVersion,
		"labels":               joinLabels(g),
		"taints":               taints,
	})
	return string(spec), err
}

func BuildUserdata(c tokenFetcher, cfg *Config, g *Group) (string, error) {
	token, err := c.FetchAttachToken(g)
	if err != nil {
		return "", err
	}
	// P0-1 companion gate: the token is delivered single-quoted (TOKEN='…')
	// and later substituted into a double-quoted sed program. tokenRe's \S+
	// already excludes whitespace; a quote or backslash would break out of (or
	// corrupt) that quoting. ACK issues URL-safe tokens — refuse a
	// shell-breaking one loudly instead of emitting a corrupted join script.
	// The error deliberately carries NO token content.
	if strings.ContainsAny(token, `'\`) {
		return "", fmt.Errorf("attach token for nodepool %s contains a shell-breaking character (quote/backslash) - refusing to build userdata", g.NodepoolID)
	}
	nodespec, err := buildNodeSpec(cfg, g)
	if err != nil {
		return "", err
	}

	// The edgeadm binary rides the same choice as the components it will later
	// download: private = the internal OSS endpoint (…oss-<region>-internal…),
	// reached over the dedicated line via the TR's oss-* statics.
	ossRegion := cfg.ControlRegion
	if cfg.ImageRepoType == "private" {
		ossRegion = cfg.ControlRegion + "-internal"
	}

	script := fmt.Sprintf(`#!/bin/bash
# xtrace stays OFF: it would write the attach token into the cloud-init log.
MARK=/var/log/ens-provider-join-marker
stage() { echo "$(date -u +%%FT%%TZ) $1" | tee -a "$MARK"; }
stage begin
growpart /dev/vda 3 || true
resize2fs /dev/vda3 || true
modprobe iptable_nat
echo iptable_nat >> /etc/modules-load.d/iptable_nat.conf
sed -i 's/223.5.5.5/8.8.8.8/g' /etc/resolv.conf || true
stage os-prep-done
(
  for i in $(seq 1 90); do
    LOCALIP=$(ip -4 addr show eth0 | awk '/inet /{print $2}' | cut -d/ -f1 | head -1)
    [ -n "$LOCALIP" ] && ip rule del from "$LOCALIP" lookup ens_eth0 2>/dev/null
    if [ -f /etc/cni/net.d/10-flannel.conflist ] && [ -f /etc/cni/net.d/0-loopback.conf ]; then
      mv /etc/cni/net.d/0-loopback.conf /etc/cni/net.d/99-loopback.conf
      systemctl restart containerd
      break
    fi
    sleep 10
  done
) >/var/log/ens-provider-node-selfheal.log 2>&1 &
cd /root
export ARCH=$(uname -m | awk '{print ($1 == "x86_64") ? "amd64" : "arm64"}')
DL_OK=""
for i in 1 2 3 4 5; do
  if wget -q "https://aliacs-k8s-%s.oss-%s.aliyuncs.com/public/pkg/run/attach/%s/${ARCH}/edgeadm" -O edgeadm; then
    DL_OK=1; break
  fi
  stage "edgeadm-download-retry-$i"
  sleep $((i * 15))
done
[ -n "$DL_OK" ] || { stage edgeadm-download-FAILED; exit 1; }
chmod u+x edgeadm
stage edgeadm-downloaded
TOKEN='%s'
JOIN_OK=""
for i in 1 2 3; do
  if ./edgeadm join --openapi-token="$TOKEN" --region=%s --node-spec='%s' >> /var/log/edge-join.log 2>&1; then
    JOIN_OK=1; break
  fi
  stage "join-retry-$i"
  sleep $((i * 30))
done
# edgeadm logs its own argv (token included) into /var/log/edgeadm.log —
# scrub it (found by the N-9 sweep 2026-07-19; our own logs were clean).
sed -i "s/$TOKEN/REDACTED/g" /var/log/edgeadm.log 2>/dev/null || true
unset TOKEN
if [ -n "$JOIN_OK" ]; then stage join-done; else stage join-FAILED; exit 1; fi
`, cfg.ControlRegion, ossRegion, cfg.K8sVersion, token, cfg.ControlRegion, nodespec)

	return base64.StdEncoding.EncodeToString([]byte(script)), nil
}
