# Rust-source generation architecture — provisional selection

Decision state: selected for prototype integration, not yet qualified for migration or release. See individual research reports and the goal ledger for actual test evidence.

## Selected composition

1. A standalone Rust-owned structured wire contract model with explicit field tags, type names, presence, oneofs, enum numbers, services, streaming, route and validation metadata. It emits Protobuf/descriptors rather than reading generated bindings as authority.
2. Maintained Protobuf/gRPC plugins consume those emitted artifacts for remote clients. Buf is the normal ecosystem bridge; pinned vendored protoc and local language plugins are a reproducible offline-capable path.
3. A Rust OpenAPI projection covers explicitly modeled HTTP routes. OpenAPI Generator is a downstream candidate, not the owner of protocols or complex streaming behavior.
4. Canonical Rust core behavior crosses a reviewed native/WASM boundary for embedded use. The C ABI prototype must qualify owned buffers, generation-checked handles, runtime lifetime, cancellation, and backpressure before adoption. UniFFI remains a comparison candidate rather than a universal promise.
5. Rust docs, crate-owned Markdown and typed executable scenarios produce a versioned source-bound documentation bundle. Compiled rustdoc metadata supplies resolved API inventory; conservative source scanning is visibly unqualified bootstrap output.
6. The existing Svelte website renders the bundle. Presentation code and target-runtime plumbing may remain native-language code; shared contracts, behavior policy, documentation and snippets must not be authored independently there.

## Why this composition

The reviewed OSS schema/type tools do not describe every existing protobuf tag, custom option, presence rule, RPC and streaming mode. A small Rust authoring layer is the demonstrated custom gap. Standard protobuf tooling provides stronger broad streaming-language coverage than an HTTP-only schema projection. Structured wire definitions alone cannot replace the canonical Rust state machines, and a code generator cannot safely infer all language runtime behavior from arbitrary Rust algorithms.

Cloudflare Forge (pinned in cloudflare-forge.md) successfully resolved the Actors-derived OpenAPI during the bounded comparison. It is early and TypeScript/Fern-oriented; full generator execution and transitive image licensing still need qualification. It is not selected as the core Rust-source generator. The experiment remains available as evidence and a possible downstream consumer.

## Promotion gates

- Generation must not require the active authored .proto input to recover a removed Rust field or method.
- Golden descriptor bytes and protocol handshake digests remain unchanged unless an explicit supported protocol transition is approved; semantic equality alone is not permission to replace digest bytes.
- Custom options are preserved, including unknown extension bytes; prost_types dropping extensions is not an acceptable comparison strategy.
- Generator output binds complete source content and tool versions. Git HEAD alone cannot identify an edited worktree.
- Package install, transport smoke, syntax checks and upstream language support are distinct from full SDK qualification.
- New code remains additive until compatibility, generated API and behavioral gates pass. Legacy TypeScript/proto/website sources are deleted family by family only after those gates.
- Toolchain absence remains outstanding work, not evidence that a language is infeasible.
