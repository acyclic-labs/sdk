# Changelog

All notable changes to Acyclic are recorded here. Every Rust crate, npm package,
and coding-agent integration in this repository shares one version and one
release commit.

## Unreleased

### Added

- Harness transport negotiation: `HandshakeResponse` advertises `TransportBinding`s, HTTP and gRPC adapters can advertise their reachable endpoints, and the shared error-code mapping is bound to `conformance/vectors/harness/error-mapping-v1.json`. TypeScript `connectHarness` resolves gRPC, gRPC-Web, WebSocket, or HTTP/SSE transparently (auto order gRPC > gRPC-Web > WebSocket > HTTP/SSE, or `ACYCLIC_TRANSPORT`).

## 0.1.0 - 2026-09-21

### Added

- Content-addressed objects, streams, inference, machine orchestration, and
  filesystem SDKs for Rust, TypeScript, React, Svelte, AI SDK, OpenCode, and Pi.
- A single `acyclic` executable and universal coding-agent plugin for Codex and
  Claude Code on Linux, macOS, and Windows.
- Native FUSE, loopback NFS, and ProjFS mounts with authenticated control-plane
  routing and fail-closed lifecycle cleanup.
- Reproducible release qualification, exact-artifact registry publication,
  aggregate checksums, SPDX SBOMs, and signed provenance attestations.

See [`release/README.md`](release/README.md) for the release and trusted-publisher
runbook.
