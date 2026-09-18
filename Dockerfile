# ens-provider — distroless runtime over a PREBUILT static binary.
# The binary is compiled outside this Dockerfile (build/build-release.sh,
# locally `make build-release`) and lands in release/, which is the build
# context for the COPY below. No shell, no toolchain, no downloads at build
# or run time. BASE_IMAGE can be overridden by an organisation's base-image
# policy; TARGETOS/TARGETARCH are auto-provided by BuildKit (defaults keep
# plain `docker build` working).
ARG BASE_IMAGE=gcr.io/distroless/static-debian12:nonroot
FROM ${BASE_IMAGE}
ARG TARGETOS=linux
ARG TARGETARCH=amd64
COPY release/ens-provider-${TARGETOS}-${TARGETARCH} /ens-provider
USER nonroot
ENTRYPOINT ["/ens-provider"]
