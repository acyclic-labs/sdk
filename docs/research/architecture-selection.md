# Rust-source generation architecture — selected stack

Decision: use the composition below for the complete SDK and documentation pipeline. The working wire, HTTP client, native/WASM and rustdoc prototypes establish the stack choice; installation, platform coverage and complete reproducible generation remain delivery gates. Selection does not certify unfinished packages.

## Selected composition

1. A standalone Rust-owned structured wire contract model with explicit field tags, type names, presence, oneofs, enum numbers, services, streaming, route and validation metadata. It emits Protobuf/descriptors rather than reading generated bindings as authority.
2. Maintained Protobuf/gRPC plugins consume those emitted artifacts for remote clients. Buf is the normal ecosystem bridge; pinned vendored protoc and local language plugins are a reproducible offline-capable path.
3. A Rust OpenAPI projection covers explicitly modeled HTTP routes. Pinned OpenAPI Generator produces broad HTTP clients from that projection. Protobuf/gRPC plugins handle RPC and streaming surfaces; canonical Rust implements shared recovery and embedded behavior.
4. Canonical Rust core behavior crosses a reviewed native/WASM boundary for embedded use. Use the versioned C ABI with cbindgen for foreign native runtimes and the existing WASM/N-API boundaries for browser and JavaScript runtimes. Owned buffers, generation-checked handles, runtime lifetime, cancellation and backpressure must qualify in real installed consumers. UniFFI is retained as comparison evidence and is not part of the universal binding layer.
5. Rust docs, crate-owned Markdown and typed executable scenarios produce a versioned source-bound documentation bundle. Compiled rustdoc metadata supplies resolved API inventory; conservative source scanning is visibly unqualified bootstrap output.
6. The existing Svelte website renders the bundle. Presentation code and target-runtime plumbing may remain native-language code; shared contracts, behavior policy, documentation and snippets must not be authored independently there.

## Why this composition

The reviewed OSS schema/type tools do not describe every existing protobuf tag, custom option, presence rule, RPC and streaming mode. A small Rust authoring layer is the demonstrated custom gap. Standard protobuf tooling provides stronger broad streaming-language coverage than an HTTP-only schema projection. Structured wire definitions alone cannot replace the canonical Rust state machines, and a code generator cannot safely infer all language runtime behavior from arbitrary Rust algorithms.

Cloudflare Forge (pinned in cloudflare-forge.md) successfully resolved the Actors-derived OpenAPI during the bounded comparison. It is early and TypeScript/Fern-oriented; full generator execution and transitive image licensing still need qualification. It is not selected as the core Rust-source generator. The experiment remains available as evidence and a possible downstream consumer.

## Final pipeline requirements

- Generation must not require the active authored .proto input to recover a removed Rust field or method.
- Golden descriptor bytes and protocol handshake digests remain unchanged unless an explicit supported protocol transition is approved; semantic equality alone is not permission to replace digest bytes.
- Custom options are preserved, including unknown extension bytes; prost_types dropping extensions is not an acceptable comparison strategy.
- Generator output binds complete source content and tool versions. Git HEAD alone cannot identify an edited worktree.
- Package install, transport smoke, syntax checks and upstream language support are distinct from full SDK qualification.
- The final tree removes independently authored shared TypeScript contracts, behavior, renderers and SDK website prose. Generated artifacts and thin presentation/runtime adapters consume Rust-owned sources. Generation emits into its artifact tree without mutating its frozen input checkout; explicit developer refresh commands may update checked-in outputs.
- Toolchain absence remains outstanding work, not evidence that a language is infeasible.

Native released-version documentation defaults to latest and keeps exact release source identity. Branch previews are separate. Legacy URL redirects and their migration ledger are not part of the selected website design.
