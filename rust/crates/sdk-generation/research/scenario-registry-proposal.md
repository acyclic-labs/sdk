# Scenario registry and executable snippet data proposal

Date: 2026-10-07

This is the next data-layer step after sdk-docs-data.v2 package and search
records. It is intentionally limited to Rust-owned scenario discovery,
verification receipts, and qualified language projections. It does not add a
website presentation layer or claim that an unqualified language is supported.

## What is source-backed today

The foundation worktree has 22 Rust examples:

| Family | Rust example sources | Current role |
| --- | --- | --- |
| Actors | actors-http-routes.rs, conformance-certificate.rs, transport-conformance.rs | Rust-owned route output, local TLS fixture, local transport qualification |
| Filesystem | embedded_workspace.rs, filesystem-git-compat-contract.rs, filesystem-typescript-defaults.rs, filesystem-typescript-hosted-contract.rs | Embedded behavior and Rust-owned contract/default output |
| Harness | authority-id-contract.rs, child-page-contract.rs, component-label-contract.rs, conversation-page-contract.rs, custom_executor.rs, limits-contract.rs, private-directory-page-contract.rs | Rust-owned limits, identifiers, and embedded executor behavior |
| Inference | inference-typescript-defaults.rs | Rust-owned limit output |
| Inference-WASM | inference-typescript-fixed-width-metadata.rs, inference-typescript-terminal-metadata.rs | Rust-owned WASM metadata examples |
| Stream | http-conformance.rs, local-contention.rs, token-operations.rs | Local HTTP conformance, local concurrency, Rust-owned token vocabulary |
| Workers | workers-http-routes.rs, workers-module-contract.rs | Rust-owned route and module-contract output |

The word TypeScript in some example filenames describes the current consumer
of the emitted Rust values. It does not make TypeScript an input authority.
The example source, the Rust constants/contracts it invokes, and its output
digest are the authority.

These examples do not all have the same verification semantics. Some are
deterministic generators, some are local executable behavior, and some require
an endpoint or a feature. A docs bundle must retain that distinction.

## Smallest Rust-owned registry

Add one typed Rust registry in the sdk-generation crate, for example
src/scenarios.rs. It should reference existing example sources with
include_str! or a repository-relative source declaration that the launcher
hashes. The registry is Rust source, not a JSON file or a website manifest.

The first version needs only these fields:

    Scenario {
        id: &'static str,
        family: &'static str,
        title: &'static str,
        source_path: &'static str,
        mode: ScenarioMode,
        command: &'static [&'static str],
        capability: Option<&'static str>,
    }

    enum ScenarioMode {
        Compile,
        ExecuteLocal,
        ExecuteWithEndpoint,
    }

The id is stable and globally unique, such as
actors/transport-conformance or stream/token-operations. The source path must
resolve under the repository root and must be present in the generation source
closure. The command is a Cargo example invocation expressed as structured
arguments, not a shell string. capability remains optional until an explicit
Rust contract metadata source exists; the registry must not infer a capability
from a filename.

Do not add language snippet bodies to this registry. A scenario is the
Rust-owned behavior and source identity from which projections are generated.

## Verification receipt

The launcher should compile or execute each registered scenario and add a
receipt to the generation manifest:

    ScenarioReceipt {
        id,
        source_sha256,
        mode,
        target,
        rustc_version,
        cargo_command,
        status,
        stdout_sha256,
        stderr_sha256,
        artifact_revision,
    }

Compile mode runs the pinned Cargo toolchain with locked, offline dependencies
and records a successful build/check artifact. ExecuteLocal runs only examples
whose source is deterministic and self-contained. ExecuteWithEndpoint is a
manual/release qualification mode and records the endpoint fixture/service
identity; it must not run in ordinary fast CI.

The command should invoke Cargo using structured arguments:

    cargo +<pinned> check --locked --offline \
      --manifest-path rust/crates/<family>/Cargo.toml \
      --example <name>

An example that requires a feature, endpoint, or external service must declare
that in the registry and cannot silently be downgraded to Compile. The receipt
must include the exact feature list and target triple when applicable. A
failure blocks publication of that scenario as a snippet; it does not create a
partial success record.

The source digest must cover the example, the owning crate source, the manifest,
lockfile, relevant build script, and any Rust-generated contract input. Reusing
a receipt after one of those bytes changes is invalid. The output digest binds
the generated docs data and any language package artifact to the same revision.

## Rust data output

Extend the v2 data model only with records backed by the receipt:

    ScenarioRecord {
        id,
        family,
        title,
        source_path,
        mode,
        source_sha256,
        receipt_sha256,
        status
    }

The record should expose source and verification identity to the website data
consumer. It should not contain copied generated TypeScript or handwritten
language code. Existing navigation and search can index the scenario title,
family, source path, and status from this record.

For each qualified language projection, generate a separate record from the
language generator and installed-package receipt:

    ProjectionRecord {
        scenario_id,
        language,
        generator,
        package,
        package_sha256,
        source_revision,
        status
    }

A ProjectionRecord is emitted only when the target has a current source-bound
qualification receipt. The scenario registry must not list all requested
languages as if they were supported. Unqualified languages remain in the
qualification ledger and produce no supported projection record. This preserves
the distinction between an inventoried target, a generator prototype, and an
installable SDK.

## Mapping the existing examples

The first dependency-complete scenario PR should register only the examples
whose source and invocation are already unambiguous:

- Compile: route tables, contract/default emitters, token vocabulary, module
  contract, metadata emitters, and the self-contained embedded workspace.
- ExecuteLocal: local transport conformance, local contention, and other
  examples that use an in-process fixture without a service endpoint.
- ExecuteWithEndpoint: Stream http-conformance and any Actors example requiring
  the SDK test server or an external endpoint.
- Feature-qualified: local-contention currently documents a local feature and
  should carry its exact feature arguments in the registry.
- Review-required: examples that emit contract snapshots should be checked for
  stable stdout and explicit generator ownership before they become published
  snippets.

The registry should initially fail closed for examples that do not match one of
these categories. Adding a scenario requires its source path, invocation,
mode, and receipt test in the same Rust PR.

## Minimal next PR

One focused PR after the v2 data model should:

1. Add src/scenarios.rs with the 22-source inventory and explicit modes.
2. Add deterministic source-path and unique-ID validation.
3. Add the pinned compile receipt path for the Compile scenarios.
4. Add release/manual receipt handling for endpoint scenarios without adding
   them to ordinary CI.
5. Add ScenarioRecord data and include scenario/receipt hashes in the bundle
   manifest and search projection.
6. Add a fixture test proving a changed example invalidates its receipt and a
   missing or duplicate registry entry blocks generation.
7. Add ProjectionRecord emission only for language/package receipts already
   qualified against the same Rust revision.

Do not add a generic capability table in this PR. The current examples contain
real Rust-owned contract outputs, but they do not provide a uniform explicit
operation/transport metadata source for every family. Capability and transport
records should be added when each family contract exposes typed metadata; they
must not be guessed from example names or generated TypeScript.

Package install instructions have the same boundary. Cargo package identity can
be validated from Rust/Cargo metadata, but npm/Python/JVM/etc. install
instructions require generated artifact metadata and a current receipt. Emit
those instructions only from a Rust-owned package-target record tied to the
artifact manifest, not from independently maintained website text.

## Exit evidence

This scenario step is complete when a clean pinned checkout:

- discovers exactly the registered Rust examples;
- verifies every source path belongs to the Rust source closure;
- compiles every Compile scenario reproducibly;
- executes only scenarios with a declared local fixture or endpoint receipt;
- rejects stale, missing, duplicate, or source-mismatched receipts;
- includes scenario and receipt hashes in generated docs data;
- emits language projections only for current revision/package receipts; and
- regenerates the same scenario/search output byte-for-byte on a second run.

This produces the data required for a docs website to render executable examples
and qualification-aware snippets while keeping Rust contracts and examples as
the only authored source.

