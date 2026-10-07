# Cloudflare Forge: maintained generator fit

**Research date:** 2026-10-07  
**Source pin:** Cloudflare Forge upstream `main` as observed on this date; package versions are exact `@cloudflare/forge@0.1.0`, `@cloudflare/forge-transformer-sdk-ts@0.1.0`, `@cloudflare/fern-config@0.1.0` (private), and `fern-api@5.112.0`. The public Forge repository does not expose an immutable release pin in the material reviewed, so an adoption pin must additionally record a source commit or vendored tarball SHA-256. Cloudflare's `cf` lockfile currently vendors the two public Forge packages as `cloudflare-forge-0.1.0.tgz` and `cloudflare-forge-transformer-sdk-ts-0.1.0.tgz`.

## What works today

Forge's maintained core is a TypeScript plugin host for resolved OpenAPI 3.x. A transformer receives a `Forge`, calls `emit`, and returns `SourceFile` objects; `finalize` writes those files. The core also supports OpenAPI overlays, operation and schema introspection, and output chaining. This is a reusable downstream emitter interface, not a Rust source parser. See the [core package](https://raw.githubusercontent.com/cloudflare/forge/main/packages/forge/package.json), [Forge API](https://raw.githubusercontent.com/cloudflare/forge/main/packages/forge/forge.ts), and [upstream architecture](https://raw.githubusercontent.com/cloudflare/forge/main/AGENTS.md).

Cloudflare's current generation script is executable evidence of the language matrix. It reads one language-agnostic `fern/openapi.json`, applies Fern compatibility repairs, requires Docker, and runs `fern generate --local`. The checked-in generator groups pin these Fern images:

| Target | Generator image version | Current evidence |
| --- | --- | --- |
| TypeScript | `fernapi/fern-typescript-sdk:3.80.1` | Public `cloudflare-forge-sdk-ts` wrapper, custom runtime post-step |
| Python | `fernapi/fern-python-sdk:5.18.1` | Private workspace wrapper invokes the shared script |
| Go | `fernapi/fern-go-sdk:1.47.2` | Private workspace wrapper invokes the shared script |
| Java | `fernapi/fern-java-sdk:4.13.2` | Private workspace wrapper invokes the shared script |
| PHP | `fernapi/fern-php-sdk:2.11.1` | Private workspace wrapper invokes the shared script |
| C# | `fernapi/fern-csharp-sdk:2.73.1` | Private workspace wrapper invokes the shared script |
| Ruby | `fernapi/fern-ruby-sdk:1.15.0` | Private workspace wrapper invokes the shared script |
| Swift | `fernapi/fern-swift-sdk:0.35.16` | Private workspace wrapper invokes the shared script |
| Rust | `fernapi/fern-rust-sdk:0.42.1` | Private workspace wrapper invokes the shared script |

The matrix is real configuration that the script can request; it is not evidence that Forge itself owns nine language emitters or that each generated package is published and qualified. The non-TypeScript wrappers are private ten-line workspace adapters around Fern. The Forge README still describes the project as early and focused on the first `cf` CLI output, with additional SDK targets and docs arriving in parallel. Treat the blog's AsyncAPI, GraphQL, Cap'n Proto, Protobuf, MCP, and Cap'n Web discussion as extension direction, not current input support.

Primary sources: [generator groups and pins](https://raw.githubusercontent.com/cloudflare/forge/main/packages/cloudflare-fern-config/fern/generators.yml), [generation script](https://raw.githubusercontent.com/cloudflare/forge/main/packages/cloudflare-fern-config/scripts/generate-sdk.ts), [language wrappers](https://raw.githubusercontent.com/cloudflare/forge/main/AGENTS.md), and the [current README](https://raw.githubusercontent.com/cloudflare/forge/main/README.md).

## Fit to the Rust-owned SDK

The local authority is different. `rust/crates/sdk-docs` consumes pinned Rustdoc JSON and preserves Rust item identities, source spans, reexports, and versioned immutable documentation data. `rust/crates/sdk-generation` fixes the Rust stages, hashes the Rust/protobuf/docs source closure, and records stage and artifact hashes. Forge currently consumes OpenAPI and Fern metadata; it cannot consume Rustdoc JSON or the repository's protobuf descriptor as an authority. It therefore cannot replace `sdk-docs`, `sdk-contract-wire`, or the fixed Rust generation stages.

The only plausible integration is downstream: Rust emits a canonical, digest-bound OpenAPI projection; Forge/Fern consumes that explicit local file to produce language packages. The projection must remain derived data, and the public language facade must continue to call Rust-owned validation, transport selection, retries, cancellation, and embedded behavior. Forge's own TypeScript custom runtime is product-specific and cannot be treated as the shared Rust runtime.

Two concrete gaps must be closed before this is accepted as a reproducible stage:

1. `generate-sdk.ts` downloads the newest matching Forge OpenAPI release when `FORGE_OPENAPI_SPEC` is absent and pulls generator images by mutable tags. The script then invokes Docker containers and performs a TypeScript-only post-generation rewrite. A Rust-owned stage must supply an explicit local projection, pin the Forge source/tarball and every image by immutable digest, run with the required network inputs already staged, and record those inputs in the generation manifest.
2. The upstream `Forge.finalize` containment check uses `fullPath.startsWith(resolvedOutputDir)` ([forge.ts, lines 188-203](https://raw.githubusercontent.com/cloudflare/forge/main/packages/forge/forge.ts#L188-L203)). An output directory such as `/tmp/out` can accept a transformer path resolving to `/tmp/out-evil/...`, because the string prefix still matches. It also cleans the destination before generation and writes files concurrently. Do not expose this method directly at the immutable publication boundary; stage into a disjoint directory and apply the Rust publisher's canonical containment, duplicate-path, and atomic-write checks. This should be reported upstream before Forge is trusted with unreviewed transformer output.

## Decision

Keep Rustdoc/protobuf and `sdk-generation` as the sole authority. Keep Forge/Fern as a candidate downstream emitter for broad language coverage, with one bounded qualification per target. The qualification must use the exact Rust-derived OpenAPI projection, verify generated source and native artifact behavior against Rust conformance vectors, and record source, generator, container, toolchain, and output hashes. Planned input formats or the presence of a generator entry in `generators.yml` do not qualify a language or permit a second handwritten runtime.
