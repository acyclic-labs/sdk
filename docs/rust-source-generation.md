# Rust-owned SDK generation

Public executable Rust declarations own wire identities, validation, client
behavior, binding projections, examples, and documentation. Generated contracts
are outputs. A generator must not read an independently authored contract to
reconstruct those declarations.

## Generator selection

| Responsibility | Pinned candidate | Working evidence | Remaining acceptance |
| --- | --- | --- | --- |
| Rust declarations to protobuf | Protify 0.1.4, Prost 0.14.4 | Direct message encoding and schema rendering; checked digest projection and unknown enum preservation | Full Actors descriptor, options, streaming signatures, tonic generation and drift checks |
| TypeScript static types | ts-rs 12.0.1 | Seven Rust tests; generated positive and negative TS consumers for nominal identity, digest, bigint and tagged union | Installed native/browser Actors clients and complete operation coverage |
| Rust documentation input | rustdoc-types 0.60.0, Rustdoc 1.98.1, format 60 | Real public/re-export/private-path fixtures; docs and spans joined by item identity | Complete input provenance, source coverage and immutable bundles |
| API signature formatting | public-api 0.52.2 | Real generic/dynamic signatures and aliases after a checked format adapter | Production integration and removal of the custom formatter |
| Shared language bindings | UniFFI 0.31.0 | Installed Windows Python wheel calling canonical Rust Stream memory behavior, typed errors, finite reads and cancellation; Swift generated-source type checks | Actual remote backend, installed target packages and complete capability coverage |
| Browser gRPC | tonic-web-wasm-client 0.9.2, tonic 0.14.6 | Generic Actors client dependency/type compatibility | Actual WASM/browser calls and public gRPC-Web ingress |

Cloudflare Forge and OpenAPI Generator operate downstream of API contracts.
They do not extract executable Rust behavior. Independently generated foreign
transport runtimes would create another behavioral authority; public SDKs instead
call the canonical Rust runtime through maintained bindings.

Protify's proxied conversion generates infallible `From` implementations and can
normalize unknown enums. Validated domain values therefore use raw executable
wire types followed by explicit Rust `TryFrom` admission. Rejected input must
return a typed error, rather than panic or become a default value.

Protify does not capture Rust comments. A narrow typed Rustdoc join by canonical
Rust path and field name supplies documentation without defining fields or tags
again.

The public-api dependency accepts format 59. Format 60 adds nullable unstable
default metadata to functions and associated constants/types. The adapter accepts
only normal public format-60 input with that metadata absent, removes those null
fields from its formatter view, and rejects any unsupported case. The original
typed format-60 input remains the documentation authority.

## Runtime and publication

Native clients select Rust gRPC automatically. Browser clients select the
maintained Rust/WASM transport. Generic connection, authentication, TLS, timeout
and application errors do not cause fallback or replay. The current public Edge
is raw HTTP/2 gRPC: browser Actors qualification requires a separate ingress
change. Standard gRPC-Web also cannot represent Objects client-streaming uploads.

Versions cover SDK packages, executable snippets and documentation together.
Release assets contain generated SDKs, native binaries, docs and hashes; Git
contains source, pins and verification. Published version bundles are immutable;
the catalog selects the latest stable version semantically and retains previous
versions. Branch previews have separate identities. Website presentation and
deployment are outside this change.

## Acceptance

Each replacement PR includes generation, installation, applicable behavior tests
and deletion of the replaced implementation. Rust/TypeScript qualification comes
first. A language becomes supported only after its installed package passes the
applicable remote and embedded cases; generator availability alone is insufficient.

Normal CI verifies source and generated drift with bounded Rust/TypeScript checks.
Broad package/platform checks run locally or on release/manual qualification.
Initial native targets are Windows x64, Linux x64 GNU and macOS arm64. Production
deployment, registry publication and auto-merge are separate operations.
