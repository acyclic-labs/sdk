# Generated SDK conformance and qualification

The SDK generation loop uses Rust to own contract metadata, descriptors,
validation, examples, and documentation inputs. Language generators are
replaceable executors. They do not become a second contract source.

## Orchestration boundary

rust/crates/sdk-generation invokes these tools in a fixed order:

1. sdk-contract-wire verifies field numbers, enum values, presence, oneofs,
   JSON names, service identities, and stream directions.
2. sdk-contract-validation compares Actors, Stream, and Workers descriptors
   against immutable deployed compatibility fixtures. Exact bytes and semantic
   compatibility are separate results; generation never rewrites those
   fixtures.
3. sdk-openapi-prototype derives an HTTP/JSON projection from Rust-owned
   descriptors and route metadata.
4. sdk-docs renders reference material and navigation from Rust comments,
   rustdoc JSON, and crate-owned Markdown.
5. sdk-examples renders executable language snippets from Rust-owned cases.
6. sdk-python exercises the pinned Python generator and package build. The
   Rust crate is preferred; the legacy script path is only an explicit
   bootstrap fallback while that crate is being integrated.

Every tool receives a request JSON document with this shape:

    {
      "schema": "acyclic.sdk.generation.request.v1",
      "operation": "generate",
      "tool": "sdk-docs",
      "source_root": "/absolute/sdk",
      "output": "/absolute/output",
      "source": {
        "revision": "<git revision>",
        "digest": "sha256:<digest>",
        "dirty": false
      },
      "contract_scope": "explicit",
      "contract_inputs": ["rust", "docs"]
    }

The tool command accepts --request PATH. It must write only below output, must
include the request source revision and digest in any manifest it emits, and
must return nonzero on a contract or generation error. The orchestrator records
missing tools as pending and failed tools as failed; it never turns an absent
tool into a passing result. `check` also regenerates into an isolated
directory and compares every regenerated artifact byte for byte. A pending
required stage makes the check pending (exit code 2), so a qualified pipeline
cannot accidentally pass because a generator has not yet been implemented.

## Common qualification protocol

Each language adapter executes the locked vectors and emits a report that binds:

- exact suite bytes and suite digest;
- source revision and package artifact digest;
- generator and runner identity;
- descriptor or protocol identity;
- sorted, unique capability names;
- one evidence digest per case in locked suite order.

Rust validates the report and emits the qualification receipt. The existing
validator in rust/crates/conformance/src/runner.rs already rejects duplicate
JSON keys, unknown fields, unbound digests, reordered cases, missing cases, and
failed or skipped cases. During migration, extend its family identity so the
same fail-closed rules apply to transport families while retaining the current
Harness receipt for compatibility.

## Remote and embedded gates

Remote clients must exercise unary calls, streaming, authentication, framing,
bounded request and response bodies, cancellation, canonical error identity,
idempotent retries, stale preconditions, pagination, and substituted response
identities. The deterministic service fixture should be shared by all
languages. Existing coverage is in the Actors, Stream, Objects, and Inference
transport tests under typescript/packages/*/test and in the Rust gRPC/HTTP
tests for those crates.

Embedded adapters must compare canonical bytes and validation outcomes with
Rust. Harness native/WASM replay, Filesystem WASM, Inference WASM, and the
canonical JSON fixtures are the initial cases. A package import smoke test is
useful evidence of installation but cannot qualify embedded behavior.

## Package and provenance evidence

An evidence file is stored as qualification/<language>.json and must use
acyclic.sdk.qualification.evidence.v1. It must identify the source revision,
the exact 64-hex contract and package artifact digests bound to the generated
manifest, and nonempty test lists for remote, embedded, docs, snippets, and
installation. Every test entry must carry a `sha256:<64-hex>` content digest
from an executed suite receipt; labels or short placeholders cannot qualify.
The inventory remains pending until the evidence is present and bound to the
current source. It merges the package-name families with the coordinated
generation-targets inventory, reports explicitly excluded HTTP-only or
not-qualifiable targets, and fails when both inventories are empty.

The package checks must install extracted archives in isolated consumers,
compile strict declarations where applicable, execute representative public
exports, verify checksums and source commit, and test extracted Rust crates
offline with their exact closure. Registry publishing, production deployment,
and main-branch merging are outside this loop.

## Reproducibility

Run generation twice from clean output directories and compare normalized
manifests, source and descriptor digests, generated text, package metadata,
snippet results, and artifact hashes. Platform-specific native binaries may be
qualified by a stable API manifest and separate binary digest; they must not be
silently restored into the source checkout. check must leave tracked source
unchanged and fail on any recorded source or artifact drift. The source digest
includes tracked and nonignored untracked author inputs, so a newly added Rust
crate cannot bypass drift detection.

## Current baseline commands

    cargo test --manifest-path rust/crates/sdk-generation/Cargo.toml --locked
    cargo test --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --locked
    cargo test --locked -p acyclic-conformance
    cargo test --workspace --all-features --locked
    bun install --frozen-lockfile
    bun run check:generated
    bun run test

The generated-language matrix should add one isolated lane per target and keep
all independent lanes occupied. Every lane retains its package archive,
checksums, source revision, report, receipt, generator identity, and test
transcript as reviewable artifacts.
