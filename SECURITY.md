# Security policy

## Reporting a vulnerability

Do not report vulnerabilities in public issues. Send reports to
security@acyclic.dev with affected versions, reproduction steps, and impact. We
aim to acknowledge reports within 48 hours.

## Scope

The Rust crates (filesystem, objects, stream, machines, harness, inference) and
their TypeScript/npm packages, the native (napi, WASM) bindings, and the release
and publication pipeline (provenance manifest, signed crate/package artifacts).

Of particular interest: filesystem or object-store data exposure across
sandbox/authority boundaries, native binding memory-safety issues, and
supply-chain issues in the publish/release pipeline (crate or npm package
tampering, provenance manifest bypass).

## Supported versions

The repository is currently a pre-release candidate: only the latest `main` and
the latest published `-rc.*` versions are supported. Support windows will be
documented with the first stable family release.
