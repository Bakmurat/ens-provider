#!/usr/bin/env bash
# (Re)vendor the externalgrpc gRPC stubs from a kubernetes/autoscaler tag.
# Community pattern: the generated *.pb.go are copied verbatim — no protoc
# toolchain needed, and the contract stays pinned to the CA image version.
#
# Usage: hack/vendor-protos.sh cluster-autoscaler-1.32.1
#
# ⚠ CA >= 1.35: upstream PR #8660 changed NodeGroupTemplateNodeInfoResponse
# (embedded v1.Node field 1 -> `bytes nodeBytes` field 2) and the options/
# pricing messages. After vendoring a >=1.35 tag, update the marked line in
# server.go or scale-from-zero silently breaks.
set -euo pipefail
TAG="${1:?usage: $0 <cluster-autoscaler tag, e.g. cluster-autoscaler-1.32.1>}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE="https://raw.githubusercontent.com/kubernetes/autoscaler/${TAG}/cluster-autoscaler/cloudprovider/externalgrpc/protos"

for f in externalgrpc.pb.go externalgrpc_grpc.pb.go; do
  echo ">> $f @ $TAG"
  curl -fsSL "$BASE/$f" -o "$HERE/protos/$f"
done
echo "$TAG" > "$HERE/protos/VENDORED_FROM"
echo "done. vendored from $TAG — run: make test build"
