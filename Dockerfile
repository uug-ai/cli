# syntax=docker/dockerfile:1.7
# glibc 2.44-r9; retain the vendor's complete native package inventory/SBOM.
FROM cgr.dev/chainguard/glibc-dynamic:latest@sha256:5380a8b913d0900cf44180e2205bf3ebf0e0b00a32a0f2fa4ab16cb06d7f9cdf AS runtime-base

FROM golang:1.26.10-trixie@sha256:909771fef53e3dcb0950255f0c9a8b4728c3b841e61509c7b29b098de7ee02d4 AS builder

WORKDIR /src
ENV GOPRIVATE=github.com/uug-ai/* \
    GOTOOLCHAIN=local \
    CGO_ENABLED=1 \
    GOMAXPROCS=2

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

# Kafka uses its bundled native librdkafka; disabling CGO removes this capability.
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    go build -p=1 -mod=readonly -trimpath -buildvcs=false \
      -tags timetzdata,netgo -ldflags="-w -buildid=" -o /out/main . && \
    chmod 0555 /out/main && \
    find indexes -type d -exec chmod 0555 {} + && \
    find indexes -type f -exec chmod 0444 {} +

FROM builder AS test
COPY --from=runtime-base /usr/lib/ /native-libs/
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    go test -p=1 -mod=readonly ./actions ./database ./queue ./models ./utils && \
    go test -c -p=1 -mod=readonly -tags timetzdata,netgo -o /out/queue.test ./queue && \
    case "$(uname -m)" in \
      x86_64) loader=/native-libs/ld-linux-x86-64.so.2 ;; \
      aarch64) loader=/native-libs/ld-linux-aarch64.so.1 ;; \
      *) exit 1 ;; \
    esac && \
    "$loader" --library-path /native-libs /out/main --help && \
    "$loader" --library-path /native-libs /out/queue.test \
      -test.run '^TestKafkaNativeMockRoundTrip$' -test.timeout 30s -test.v
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    go install golang.org/x/vuln/cmd/govulncheck@v1.1.4 && \
    govulncheck -tags timetzdata,netgo ./... && \
    govulncheck -mode=binary /out/main

FROM runtime-base AS runtime
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
COPY --from=builder --chown=0:0 /out/main /main
COPY --from=builder --chown=0:0 /src/indexes /indexes
USER 65532:65532
WORKDIR /
ENTRYPOINT ["/main"]
