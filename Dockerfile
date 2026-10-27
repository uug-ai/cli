# syntax=docker/dockerfile:1.7
FROM golang:1.26.10-bookworm@sha256:632dff7351799a5a9954a48dc06670ade72ee44e8c351cdc0e5a57af2fea21af AS go-toolchain

FROM cgr.dev/chainguard/wolfi-base:latest@sha256:0feab531471d2edfcca55f18883441cb7df598e724fa2dedf9985273f532eed7 AS native
RUN apk add --no-cache build-base coreutils git librdkafka-dev=2.16.0-r0 && \
    mkdir -p /runtime/etc/apk/keys && \
    cp /etc/apk/keys/* /runtime/etc/apk/keys/ && \
    cp /etc/apk/repositories /runtime/etc/apk/repositories && \
    apk --root /runtime --initdb --no-cache --no-scripts add \
      wolfi-baselayout wolfi-keys ca-certificates-bundle librdkafka=2.16.0-r0 && \
    apk --root /runtime info -v && \
    apk --root /runtime info --who-owns /usr/lib/librdkafka.so.1

FROM native AS builder
COPY --from=go-toolchain /usr/local/go/ /usr/local/go/
ENV PATH="/usr/local/go/bin:${PATH}" \
    GOPRIVATE=github.com/uug-ai/* \
    GOTOOLCHAIN=local \
    CGO_ENABLED=1 \
    GOMAXPROCS=2
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=secret,id=github_token \
    --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    if [ -s /run/secrets/github_token ]; then \
      export GIT_CONFIG_COUNT=1 \
        GIT_CONFIG_KEY_0="url.https://x-access-token:$(cat /run/secrets/github_token)@github.com/.insteadOf" \
        GIT_CONFIG_VALUE_0=https://github.com/; \
    fi; \
    go mod download && go mod verify

COPY main.go ./
COPY actions/ ./actions/
COPY database/ ./database/
COPY models/ ./models/
COPY queue/ ./queue/
COPY utils/ ./utils/
COPY indexes/ ./indexes/

# Use package-managed Kafka instead of the opaque bundled native archive.
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    test "$(go env GOVERSION)" = go1.26.10 && \
    test "$(pkg-config --modversion rdkafka)" = 2.16.0 && \
    go build -p=1 -mod=readonly -trimpath -buildvcs=false \
      -tags dynamic,timetzdata,netgo -ldflags="-w -buildid=" -o /out/main . && \
    readelf -d /out/main | grep 'Shared library: \[librdkafka.so.1\]' && \
    chmod 0555 /out/main && \
    find indexes -type d -exec chmod 0555 {} + && \
    find indexes -type f -exec chmod 0444 {} +

FROM builder AS test
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    go test -p=1 -mod=readonly -tags dynamic,timetzdata,netgo \
      -timeout 120s ./actions ./database ./queue ./models ./utils && \
    go test -c -p=1 -mod=readonly -tags dynamic,timetzdata,netgo -o /out/queue.test ./queue && \
    install -m 0555 /out/main /runtime/main && \
    install -m 0555 /out/queue.test /runtime/queue.test && \
    chroot --userspec=65532:65532 /runtime /main --help && \
    CLI_RUNTIME_FIXTURE=1 chroot --userspec=65532:65532 /runtime /queue.test \
      -test.run '^TestKafkaNativeMockRoundTrip$' -test.timeout 30s -test.v
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    go install golang.org/x/vuln/cmd/govulncheck@v1.9.0 && \
    govulncheck -tags dynamic,timetzdata,netgo ./... && \
    govulncheck -mode=binary /out/main

FROM scratch AS runtime
ARG VERSION=dev
ARG REVISION=unknown
ARG CREATED
LABEL org.opencontainers.image.title="cli" \
      org.opencontainers.image.source="https://github.com/uug-ai/cli" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.created="${CREATED}" \
      org.opencontainers.image.vendor="UUG.AI"

# Keep /main and /indexes compatible with existing operational Job manifests.
COPY --from=native --chown=0:0 /runtime/ /
COPY --from=builder --chown=0:0 /out/main /main
COPY --from=builder --chown=0:0 /src/indexes /indexes
USER 65532:65532
WORKDIR /
ENTRYPOINT ["/main"]
