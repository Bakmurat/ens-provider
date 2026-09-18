# ens-provider build & release.
#
#   make test                          # go vet + unit tests
#   make test-race                     # go test -race ./... (run before any release)
#   make build                         # local static binary (./bin/ens-provider)
#   make build-release VERSION=v0.0.0-local   # release binary into release/
#   make image                         # docker image (linux/amd64, from release/)
#   make push VERSION=vX.Y.Z REGISTRY=registry.example.com/<namespace>
#                                      # the Makefile appends IMAGE (ens-provider)
#   make vendor-protos CA_TAG=cluster-autoscaler-1.32.1   # (re)vendor gRPC stubs
#
# UPGRADE PLAYBOOK (keep in sync with the CA image in chart values):
#   1. bump the CA image tag (autoscaler.image) to match the cluster's k8s minor
#   2. make vendor-protos CA_TAG=cluster-autoscaler-<same version>
#   3. if the new tag is >= 1.35: the TemplateNodeInfo response moved to
#      `nodeBytes` (field 2) — flip the marked line in server.go (upstream PR #8660)
#   4. make test test-race, then tag a release; build and push the image;
#      bump provider.image in your values; helm upgrade

IMAGE    ?= ens-provider
VERSION  ?= v0.1.0
# e.g. REGISTRY=registry.example.com/<namespace>
# (an inline comment here would leave trailing spaces inside the value)
REGISTRY ?= registry.example.invalid
CA_TAG   ?= cluster-autoscaler-1.32.1
PLATFORM ?= linux/amd64

.PHONY: build build-release test test-race image push vendor-protos

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/ens-provider .

# Release binary in release/ for the Dockerfile COPY.
build-release:
	./build/build-release.sh $(VERSION)

test:
	go vet ./...
	go test ./...

test-race:
	go test -race ./...

image: build-release
	docker build --platform $(PLATFORM) -t $(REGISTRY)/$(IMAGE):$(VERSION) .

push: image
	docker push $(REGISTRY)/$(IMAGE):$(VERSION)
	@echo ""
	@echo "set in your chart values:"
	@echo "  provider:"
	@echo "    image: $(REGISTRY)/$(IMAGE):$(VERSION)"

vendor-protos:
	./hack/vendor-protos.sh $(CA_TAG)
