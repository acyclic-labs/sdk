# acyclic-sdk-generation

acyclic-sdk-generation is the orchestration boundary for the Rust SDK
source-of-truth migration. It does not implement language generators. It
captures one immutable source identity, writes a request envelope for each
family tool, invokes tools that are present, and records their exact outputs.

The crate remains outside the root workspace so the generation command has a
stable, independently locked build and never edits the root manifest or
lockfile as a side effect.

    cargo run --manifest-path rust/crates/sdk-generation/Cargo.toml -- generate --source-root . --output target/sdk-generation
    cargo run --manifest-path rust/crates/sdk-generation/Cargo.toml -- check --source-root . --output target/sdk-generation
    cargo run --manifest-path rust/crates/sdk-generation/Cargo.toml -- drift --source-root . --output target/sdk-generation
    cargo run --manifest-path rust/crates/sdk-generation/Cargo.toml -- inventory --source-root . --output target/sdk-generation
    cargo run --manifest-path rust/crates/sdk-generation/Cargo.toml -- qualify --source-root . --output target/sdk-generation

After the generation manifest and consumer scenario result files exist, the
Rust receipt writer creates a unified remote qualification receipt from the
actual scenario log:

    cargo run --manifest-path rust/crates/sdk-generation/Cargo.toml --bin sdk-qualification-receipt -- --source-root . --output target/sdk-generation --scenario-log target/sdk-generation/qualification/consumers/scenario-log.json --language python --tool grpcio-tools --suite remote-conformance

The scenario log uses `acyclic.sdk.rpc-scenario-log.v1` and contains the
consumer identity plus `scenarios` entries pointing at existing
`acyclic.sdk.rpc-scenario-result.v1` files. The writer computes the Git
`HEAD` revision itself, verifies it is clean, reads the generation manifest's
`source.digest` as `contract_digest`, verifies every consumer and scenario
byte hash against the artifact set, and derives `families`, RPC identities,
shapes, and exercised features from the Rust authority and result files.
There is no option to pass family names, RPCs, a status, or an exit code on
the command line. The authority model digest and the Git revision therefore
remain separate fields; a SHA-256 authority digest must never be relabeled as
`source_revision`.

The log input is intentionally small and contains no hand-authored family
inventory:

```json
{
  "schema": "acyclic.sdk.rpc-scenario-log.v1",
  "source_revision": "<git HEAD>",
  "consumer": {
    "name": "python-consumer",
    "version": "0.2.0",
    "artifact_path": "qualification/consumers/python-consumer",
    "artifact_sha256": "sha256:<consumer bytes>"
  },
  "scenarios": [
    {
      "output_path": "qualification/consumers/actors-create.json",
      "output_sha256": "sha256:<scenario result bytes>"
    }
  ]
}
```

Each result file supplies its own Rust-authority family, fully qualified RPC,
shape, transport, invocation, exit code, and checks. The writer rejects a
missing method, a duplicate, a stale hash, a nonzero exit, or a result that is
not present in the generated artifact manifest.

The repository-level `generate` and `check:generated` scripts are currently
compatibility wrappers around the pre-migration Bun projections. They are not
the Rust authority entrypoint. During the migration, package owners should
wire one public SDK command to the Rust CLI and keep the operation mapping
explicit:

```text
generate:sdk  -> node scripts/rust-sdk-generation.mjs generate --source-root . --output <output>
check:sdk     -> node scripts/rust-sdk-generation.mjs check   --source-root . --output <output>
drift:sdk     -> node scripts/rust-sdk-generation.mjs drift   --source-root . --output <output>
```

The wrapper may select the pinned Cargo executable and output directory, but
it must not add an independent contract generator or silently fall back to a
legacy projection. Legacy Bun commands can remain named migration adapters
until their outputs are emitted by a Rust stage; they must not be presented as
the source of truth for qualification. `generate` may write staged outputs,
`check` must compare regenerated staged outputs without repairing the checked
tree, and `drift` must inspect an existing generated manifest without invoking
downstream generators.

The Rust `sdk-language-producers` stage emits
`language-producers/plan.json` from every entry in
`languages/generation-targets.json`. Each plan entry carries the target's
generator name, version, source, license, pin, package ecosystem and artifact,
plus the current Rust source identity. This is a source-bound request plan;
it does not claim that a language package was built or installed. Per-language
consumer receipts remain required for qualification, and missing toolchains
stay pending rather than being converted into a successful package result.

Language owners may add an executable `producer` recipe to a target when the
producer has a deterministic staged-output interface. The recipe is JSON data,
not a shell command:

```json
{
  "producer": {
    "program": "<pinned executable>",
    "args": [
      "--source-root", "{source_root}",
      "--authority", "{wire_root}",
      "--request", "{request}",
      "--output", "{target_output}"
    ],
    "output": "generated"
  }
}
```

The Rust entrypoint expands only the documented placeholders (`source_root`,
`output_root`, `target_output`, `wire_root`, `authority_manifest`, `request`,
`target_id`, and `operation`) and invokes the executable directly. Every recipe
must bind the source root, request, and staged target output. Its stdout,
stderr, exit code, command, request, and byte-level output digest are retained
under `language-producers/`; a missing executable is pending and a successful
command without a staged output fails. `drift` never invokes a producer.
Recipes whose existing scripts write fixed paths in a checkout need a small
language-owner adapter before they can be declared here; the orchestrator does
not mutate a source checkout to accommodate them.

inventory can report pending work. generate and check fail closed when a
delegated stage fails or remains pending; check also fails on source or
artifact drift. drift is the fast, non-generating artifact gate: it verifies
the clean source identities, retained tool stdout/stderr bytes, authority
manifest, revision bindings, and every generated artifact without invoking
downstream tools. It exits 2 when the manifest is failed or pending. qualify exits with status 2 while any language is missing
complete evidence and exits 0 only when every inventoried language has
qualified remote, embedded, documentation, snippet, and installation evidence.

Each delegated tool receives requests/<tool>.json using the
acyclic.sdk.generation.request.v1 schema. The current pinned prototypes retain
their explicit native interfaces: `sdk-contract-wire generate --out DIR`,
`sdk-openapi-prototype PATH`, `sdk-docs --repo-root ROOT --output FILE
--website-output FILE --profile-manifest docs/rustdoc-profiles.json
--rustdoc-json target/rustdoc-json --strict-rustdoc-json --source-revision
REV`, and `sdk-python generate --schema-root ROOT --output DIR`. New tools
should accept `--request PATH` directly. Every tool reads the
immutable source root, writes only under the requested output directory, and
makes no registry or production deployment calls. The orchestrator hashes all
generated artifacts after the tool returns.

After wire export, the required `sdk-contract-validation` stage compares the
Rust Actors, Stream, and Workers descriptor outputs against immutable deployed
compatibility fixtures. Exact descriptor bytes and semantic compatibility are
reported separately; the fixtures are never rewritten by generation.

The request envelope's contract scope is always explicit and points at Rust
authority exports; it never selects an ambient active `.proto` tree. Missing
authority exports, unimplemented generators, and stale artifacts remain
visible as failed or pending stages rather than being reported as qualified.

Language qualification evidence is a structured receipt, not a free-form test
log. A capability may use a test entry such as
`smoke qualification/receipts/remote.json sha256:<receipt-sha256>` in its
`tests` array. The referenced path must be below the generated output's
`qualification/receipts/` directory, and its bytes must hash to the supplied
digest. The file must contain JSON with this schema and identity binding:

```json
{
  "schema": "acyclic.sdk.qualification.receipt.v1",
  "language": "rust",
  "capability": "remote",
  "source_revision": "<manifest source revision>",
  "source_revision_kind": "git-oid",
  "contract_digest": "sha256:<manifest source digest>",
  "artifact_digest": "sha256:<manifest artifact-set digest>",
  "status": "passed",
  "exit_code": 0,
  "suite": "remote-smoke",
  "assertions": 1,
  "consumer": {
    "executed": true,
    "name": "rust-consumer",
    "version": "<consumer version>",
    "source_revision": "<manifest source revision>",
    "artifact_path": "qualification/consumers/remote.bin",
    "artifact_sha256": "sha256:<consumer artifact hash>",
    "scenarios": [
      {
        "family": "actors",
        "rpc": "acyclic.actors.v1.ActorsService/List",
        "shape": "unary",
        "status": "passed",
        "output_path": "qualification/consumers/actors-list.json",
        "output_sha256": "sha256:<scenario-result hash>"
      }
    ]
  },
  "families": [
    {
      "family": "actors",
      "methods": ["list"],
      "features": ["serialization", "transport"],
      "rpc_shapes": ["unary"]
    }
  ]
}
```

Qualification accepts a capability only when every listed receipt is present,
hash-valid, identity-matched to the current generated output, and records a
passed suite with a nonzero assertion count. The `consumer` object proves that
an executable consumer actually ran and binds its portable runtime path and
SHA-256 to the generated artifact manifest. A stale executable relabeled with
the current source revision therefore fails the byte check. Every
`consumer.scenarios` entry must also be present in that same artifact manifest;
its `output_path` and `output_sha256` bind to a JSON
`acyclic.sdk.rpc-scenario-result.v1` result with `invoked: true`, zero exit
status, the exact family/RPC/shape identity, and invocation/transport checks.
This prevents a producer from qualifying a method by merely naming it in a
receipt or by relabeling an unrelated output file. The `families` array must
cover every Rust descriptor family emitted in `wire/`; each entry names the
methods and exercised features, and its method set must agree with the
scenario set. Each capability is evaluated independently. An unavailable
capability can be marked `excluded` only with a nonempty scoped `scope` value;
an unscoped exclusion remains pending. A receipt that exercises only a subset
of the Rust-authoritative methods is `partial` only when the capability has an
explicit nonempty `scope`; otherwise it remains pending.

Rust snippets in `sdk-examples-manifest.json` have a stricter receipt. A
generic `cargo test` result for the examples crate does not qualify a rendered
snippet. An executed Rust snippet must bind its rendered `snippet_path` and
`snippet_sha256`, its source snapshot path and hash, the current source
revision, a zero exit code, stdout and stderr hashes, and exact hashes for both
the compile artifact and runtime artifact under `qualification/consumers/`.
It also requires an executed locked Cargo consumer test, with a disposable
consumer manifest and lockfile, whose extracted package artifact under
`qualification/packages/` is tied to the snippet and compile hashes. The
`package_resolution` evidence binds the Cargo package name/version, extracted
tree and manifest hashes, archive bytes and archive metadata, and the lockfile
entry. This keeps a same-name/version package at another path, an altered
extracted source tree, an archive/extraction mismatch, or a registry-sourced
lock entry from qualifying. The generation gate checks those bytes directly;
missing or relabeled snippet or package evidence remains pending.

The OpenAPI stage also emits `openapi/workers-powershell-adaptation.ps1` from
the Rust `sdk-openapi` authority entrypoint. The pinned OpenAPI Generator
package supplies the PowerShell scaffolding; this Rust-owned, anchor-checked
adaptation supplies Workers protobuf bytes and uint64 behavior. Since the file
is written below the generation output, its SHA-256 and byte length are
included in `sdk-generation-manifest.json` and regenerated during `check`.
The same stage writes `openapi/stage-receipt.json`, which binds that adaptation
hash and anchor report to the Rust emitter, records the pinned OpenAPI Generator
7.25.0 qualification metadata, and records the Apache-2.0 metadata-overlay
scope with per-target license fields; it does not make a global license claim
for every template. The receipt is itself part of the hashed artifact set, so
changing the pin, license scope, anchor report, or any projection makes `check`
fail.
Receipt cleanup is limited to the staged generation output; `check` leaves the
existing output tree untouched while it regenerates under `.check/` for drift
comparison.
