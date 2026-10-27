FROM docker.io/cloudflare/sandbox:1.0.3 AS tools
FROM debian:trixie-slim
COPY --from=tools /usr/local/bin/sandbox-shim /usr/local/bin/sandbox-shim
RUN (apt-get update && apt-get install -y --no-install-recommends util-linux procps && rm -rf /var/lib/apt/lists/* && mkdir -p /workspace) || { echo 'Workspace image build failed; recovery: docker build deploy/machines-cloudflare' >&2; exit 1; }
CMD ["sleep", "infinity"]
