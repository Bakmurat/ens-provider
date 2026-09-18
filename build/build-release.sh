#!/bin/bash
# Release build: compiles the static provider binary into release/, where the
# Dockerfile COPYs it from. Also used locally via `make build-release`.
set -euo pipefail

VERSION="${1:?usage: $0 <version>}"
OUT=release

mkdir -p "$OUT"
echo "building ens-provider ${VERSION} (linux/amd64, static)"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath \
  -ldflags "-s -w" \
  -o "$OUT/ens-provider-linux-amd64" .
echo "built ${OUT}/ens-provider-linux-amd64 for version ${VERSION}"
