# syntax=docker/dockerfile:1.7

# These defaults match go.version and the release image. Base-image upgrades
# must update each tag and its pinned digest together.
ARG GO_VERSION=1.26.6
ARG ALPINE_VERSION=3.24
ARG VERSION=dev
ARG VCS_REF=unknown
ARG BUILD_DATE=unknown

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine${ALPINE_VERSION}@sha256:af8d6740070b8906d12eae1c3e3ea0957fb63f492051ea05e354c38ef9fe88df AS build

ARG VERSION
ARG VCS_REF
ARG BUILD_DATE

RUN apk add --no-cache ca-certificates git

ENV CGO_ENABLED=0 \
    GOTOOLCHAIN=local \
    GOFLAGS=-mod=readonly

WORKDIR /src

# Cache dependencies independently from source changes. The readonly module
# cache prevents a build from silently changing the committed dependency graph.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    go mod download \
    && go mod verify

COPY . .

ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build \
      -trimpath \
      -ldflags="-s -w -buildid= \
        -X github.com/ryancswallace/jobman/internal/buildinfo.Version=${VERSION} \
        -X github.com/ryancswallace/jobman/internal/buildinfo.Commit=${VCS_REF} \
        -X github.com/ryancswallace/jobman/internal/buildinfo.Date=${BUILD_DATE}" \
      -o /out/jobman \
      .

FROM alpine:${ALPINE_VERSION}@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b AS runtime

# Bash is required by jobman's command runner. Tini forwards signals and reaps
# orphaned child processes when jobman is PID 1. CA roots and timezone data make
# networked and scheduled jobs useful without inflating the image excessively.
RUN apk add --no-cache bash ca-certificates tini tzdata \
    && addgroup -S -g 10001 jobman \
    && adduser -S -D -u 10001 -G jobman -h /home/jobman jobman \
    && mkdir -p /home/jobman/.config/jobman /home/jobman/.local/state/jobman /work \
    && chmod 0700 /home/jobman/.config/jobman /home/jobman/.local/state/jobman \
    && chown -R jobman:jobman /home/jobman /work

ARG VERSION
ARG VCS_REF
ARG BUILD_DATE
LABEL org.opencontainers.image.title="jobman" \
      org.opencontainers.image.description="A daemon-less command line job manager" \
      org.opencontainers.image.url="https://github.com/ryancswallace/jobman" \
      org.opencontainers.image.source="https://github.com/ryancswallace/jobman" \
      org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.created="$BUILD_DATE" \
      org.opencontainers.image.revision="$VCS_REF" \
      org.opencontainers.image.licenses="MIT"

COPY --from=build --chown=root:root /out/jobman /usr/local/bin/jobman
COPY --from=build --chown=root:root \
    /src/LICENSE \
    /src/THIRD_PARTY_NOTICES.md \
    /usr/share/licenses/jobman/

ENV HOME=/home/jobman \
    XDG_CONFIG_HOME=/home/jobman/.config \
    XDG_STATE_HOME=/home/jobman/.local/state

USER 10001:10001
WORKDIR /work

STOPSIGNAL SIGTERM
ENTRYPOINT ["/sbin/tini", "--", "jobman"]
CMD ["--help"]
