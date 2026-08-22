ARG GO_VERSION=1.26.6
ARG GO_FEATURE_VERSION=1.26

FROM golang:${GO_VERSION}-bookworm@sha256:116d58cbd88c1297624acc6e967a060012422bacf9930927e23fb719189c6f36 AS toolchain
FROM mcr.microsoft.com/devcontainers/go:2-${GO_FEATURE_VERSION}-bookworm@sha256:090a56c6c9c4e80a1573c18c1225eb851b8c86c199fc929b07ea2a67f7b4850f

COPY --from=toolchain /usr/local/go /usr/local/go

ARG GOPLS_VERSION=v0.23.0
ARG DELVE_VERSION=v1.27.0
ARG X_TOOLS_VERSION=v0.48.0
ARG GOTESTS_VERSION=v1.9.0
ARG IMPL_VERSION=v1.5.0
ARG STATICCHECK_VERSION=v0.7.0
ARG TTYD_VERSION=1.7.7

RUN apt-get update \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install --yes --no-install-recommends \
        bash-completion \
        ca-certificates \
        curl \
        ffmpeg \
        git \
        jq \
        less \
        libatk-bridge2.0-0 \
        libatk1.0-0 \
        libatspi2.0-0 \
        libcups2 \
        libnss3 \
        libxcomposite1 \
        libxdamage1 \
        make \
        man-db \
        openssh-client \
        procps \
        ripgrep \
        shellcheck \
        zsh \
    && rm -rf /var/lib/apt/lists/*

RUN architecture=$(dpkg --print-architecture) \
    && case "${architecture}" in \
        amd64) \
            ttyd_asset=ttyd.x86_64; \
            ttyd_sha256=8a217c968aba172e0dbf3f34447218dc015bc4d5e59bf51db2f2cd12b7be4f55 \
            ;; \
        arm64) \
            ttyd_asset=ttyd.aarch64; \
            ttyd_sha256=b38acadd89d1d396a0f5649aa52c539edbad07f4bc7348b27b4f4b7219dd4165 \
            ;; \
        *) \
            echo "unsupported devcontainer architecture: ${architecture}" >&2; \
            exit 2 \
            ;; \
    esac \
    && curl --fail --location --show-error --silent \
        --output /usr/local/bin/ttyd \
        "https://github.com/tsl0922/ttyd/releases/download/${TTYD_VERSION}/${ttyd_asset}" \
    && echo "${ttyd_sha256}  /usr/local/bin/ttyd" | sha256sum --check --strict - \
    && chmod 0755 /usr/local/bin/ttyd \
    && ttyd --version

USER vscode
RUN go install golang.org/x/tools/gopls@${GOPLS_VERSION} \
    && go install github.com/go-delve/delve/cmd/dlv@${DELVE_VERSION} \
    && go install golang.org/x/tools/cmd/goimports@${X_TOOLS_VERSION} \
    && go install github.com/cweill/gotests/gotests@${GOTESTS_VERSION} \
    && go install github.com/josharian/impl@${IMPL_VERSION} \
    && go install honnef.co/go/tools/cmd/staticcheck@${STATICCHECK_VERSION}
