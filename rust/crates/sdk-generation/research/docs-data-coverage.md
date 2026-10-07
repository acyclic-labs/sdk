# Generated docs data coverage and smallest completion PR

Date: 2026-10-07

This note defines the generated-data boundary for the Rust documentation source of truth. It covers the data producer and its inputs. The acyclic.dev presentation layer can consume the bundle later; it is outside this change.

## Current boundary

The current main revision 4f9447472ad3288ef51cd9d7e5dbc9e7d8785c41 has the sdk-docs library, but it does not yet contain the sdk-generation crate or the Actors semantic domain source. Main's release/cargo-crates.json contains ten published families:

    acyclic-inference
    acyclic-machines
    acyclic-native-runtime
    acyclic-objects
    acyclic-actors
    acyclic-workers
    acyclic-stream
    acyclic-fs
    acyclic-harness
    acyclic-plugin

The primary source-foundation worktree contains the next generation stage. Its release catalog currently has eleven entries, adding acyclic-inference-contract. That difference must remain explicit in manifests and reviews; a worktree-only family must not be presented as a main release capability.

sdk-docs is already the correct data-model boundary. It accepts typed rustdoc JSON and returns DocsData with source attestation, version/channel, navigation, families, public API items, guides, links, and source spans. write_bundle writes immutable versioned data, schemas, and a guarded sdk-docs-versions.v1 index. The generator also records source and input digests and protects latest release selection. These existing guarantees should be extended rather than replaced.

The generated-data contract must stay Rust-owned. Existing package.json files, generated TypeScript, protobuf output, and website components are validation or presentation artifacts. They cannot become input authority.

## Source coverage matrix

| Data needed by the docs site | Rust-owned source in the foundation worktree | Current producer coverage | Smallest completion change |
| --- | --- | --- | --- |
| Family and API navigation | Typed rustdoc JSON from every release catalog package; crate names and public paths | sdk-docs projects families and NavigationEntry values | Keep deterministic family, module, guide, and item ordering; include stable IDs and parent IDs in the same data bundle |
| API references | Public rustdoc items, signatures, docs, reexports, source spans, same-crate links | sdk-docs emits ApiItem and validates public/private boundaries | Add capability and wire-identity metadata only where Rust metadata exists; do not infer it from TypeScript |
| Guides | Crate root/module rustdoc Markdown plus crate-owned README.md and docs Markdown named by the Rustdoc dependency closure | sdk-docs emits crate/module guides and link maps; sdk-generation hashes tracked Markdown | Make the launcher pass the complete crate-owned guide set and reject an untracked guide; preserve source path and content digest |
| Executable examples and snippets | Each crate's examples/*.rs and Rust doctests; current inventory includes Actors, Filesystem, Harness, Inference, Inference-WASM, Stream, and Workers examples | Examples exist beside Rust but are not yet represented in DocsData | Add a Rust-owned Scenario record with ID, family, source path, operation/capability, expected artifact, and language projections; compile Rust examples and record hashes |
| Remote capabilities and transports | Rust public operation declarations, contract/protocol modules, explicit transport metadata, and generated descriptors owned by Rust | Some contract/descriptor stages exist; DocsData has no capability table | Add CapabilityRecord and TransportRecord to sdk-docs input/output; require explicit operation identity, streaming shape, cancellation/recovery support, and default transport |
| Embedded/native capabilities | Rust crate boundary metadata and native/WASM adapter declarations | Qualification receipts and adapter crates exist, but data model has no capability projection | Add BindingRecord with target, ABI/package identity, supported operations, and artifact/source digests; keep native and WASM behavior delegated to Rust |
| Package instructions | Rust generation metadata for each published family and qualified target; Cargo metadata validates crate/version | Current package manifests and READMEs contain instructions, but they are not a Rust-owned structured input | Add typed PackageRecord/InstallInstruction metadata in Rust generation input; emit package names, version, target, install command template, artifact digest, and runtime requirements. Treat package.json and README text as generated/validated output |
| Search | Family/API/guide titles, paths, signatures, docs, capability names, scenario names, and package labels already present in DocsData | No SearchIndex field | Generate a deterministic Rust SearchEntry list or token index from the final DocsData; include source IDs and normalized terms, not a second website index |
| Version catalog | Rust BuildInput version/channel/revision/source digest and VersionEntry/VersionIndex | sdk-docs already emits releases, preview, latest, data file, and data digest | Keep latest as greatest stable SemVer; make every generated data file and search/snippet artifact part of the immutable data hash |
| Language variants | Rust-owned scenario IDs and qualification records; maintained generators selected by research | Qualification research is separate and no language variant coverage should be assumed | Emit only projections backed by a qualified generator/artifact receipt, while keeping every viable language in the ledger until qualified or concretely excluded |

The current Rust examples are not a complete coverage claim. The observed source files are:

    actors/examples/actors-http-routes.rs
    actors/examples/conformance-certificate.rs
    actors/examples/transport-conformance.rs
    filesystem/examples/embedded_workspace.rs
    filesystem/examples/filesystem-git-compat-contract.rs
    filesystem/examples/filesystem-typescript-defaults.rs
    filesystem/examples/filesystem-typescript-hosted-contract.rs
    harness/examples/authority-id-contract.rs
    harness/examples/child-page-contract.rs
    harness/examples/component-label-contract.rs
    harness/examples/conversation-page-contract.rs
    harness/examples/custom_executor.rs
    harness/examples/limits-contract.rs
    harness/examples/private-directory-page-contract.rs
    inference/examples/inference-typescript-defaults.rs
    inference-wasm/examples/inference-typescript-fixed-width-metadata.rs
    inference-wasm/examples/inference-typescript-terminal-metadata.rs
    stream/examples/http-conformance.rs
    stream/examples/local-contention.rs
    stream/examples/token-operations.rs
    workers/examples/workers-http-routes.rs
    workers/examples/workers-module-contract.rs

The crate-owned README inventory also includes Actors, Filesystem, Harness, Inference, Machines, Machines-Daytona, Native Runtime, Objects, Stream, Workers, adapter crates, sdk-docs, and sdk-generation. Top-level docs currently include objects-v2-http.md, rust-generator-dependencies.md, rust-language-qualification.md, and rust-source-generation.md. The launcher must define which of these are public guides and include them in the source closure; discovery by scanning arbitrary website files would break reproducibility.

## Smallest maintained design

Extend the existing Rust data pipeline in two layers.

First, extend sdk-docs with Rust-serializable records:

    CapabilityRecord
    TransportRecord
    BindingRecord
    PackageRecord
    ScenarioRecord
    SearchEntry

DocsData should gain capabilities, bindings, packages, scenarios, and search fields under a new schema version, or equivalent additive fields with an explicit schema revision. Every record needs a stable Rust identity, family, source path or Rustdoc ID, and digest where an external artifact is referenced. Capability and transport records must carry explicit wire identity, operation shape, stream direction, cancellation/recovery behavior, and default-transport selection. A consumer should be able to render a capability table without inspecting generated TypeScript or protobuf files.

Second, extend sdk-generation's source collector and launcher. Its authoritative inputs are pinned typed rustdoc JSON, crate-owned Markdown in the verified source closure, Rust examples/doctests, Rust metadata registries or attributes for capabilities/package/scenario records, and the existing release catalog/toolchain. Cargo manifests and package manifests remain consistency checks. They do not supply independently authored website content. The launcher should:

1. resolve the exact revision and catalog;
2. collect and hash every owned input;
3. build the existing API/guide/family data;
4. compile Rust examples and validate doctest/scenario source ownership;
5. validate explicit capability, package, binding, and scenario records against the catalog;
6. generate the deterministic search entries from the completed Rust data;
7. write the versioned bundle, schemas, version index, and manifest;
8. support generate and drift-check modes with the same source closure.

No custom website parser or separate search generator is needed. sdk-docs already projects Rustdoc and Markdown. Search is a deterministic projection of the final Rust-owned data. Snippet rendering can start from Rust scenarios and artifact-qualified templates; a language projection is not emitted as supported until its generator and package receipt satisfy the qualification ledger.

## One dependency-complete minimal data PR

A single reviewable PR can make the generated-data boundary complete without touching the website UI:

1. Add the six typed records and their schemas to sdk-docs; add the fields to DocsData and include them in source/input/artifact hashing.
2. Add the Rust-owned metadata input format to sdk-generation. It should be a typed Rust module or generated Rust metadata file checked by the launcher, with explicit IDs and source spans. Do not add a handwritten TypeScript or website metadata file.
3. Add deterministic collection for crate guides, examples, scenarios, package instructions, capabilities, bindings, and search entries. Reject duplicate IDs, missing families, untracked files, stale artifact digests, or records that name a non-published package.
4. Add fixture tests for versioned bundle immutability, latest selection, search determinism, capability identity, package/source digest binding, scenario source ownership, and drift detection. Compile the existing Rust examples used as published scenarios.
5. Emit generated schemas, data, version index, and a machine-readable manifest that records revision, generator/toolchain versions, source closure, and artifact hashes. Keep website consumption as a later consumer change.

The PR is dependency-complete when a clean checkout can generate the same bundle with no hand-edited output, and drift mode fails after any Rust API, guide, capability, package, or scenario change. It does not need a Svelte route, visual redesign, redirects, or deployment to establish this data authority.

## Exact gaps and exit conditions

The data source of truth is not complete until all of these have evidence:

- sdk-generation is present on the intended integration branch and its catalog count is reconciled with main;
- every published family has a Rustdoc family, owned guide set, explicit capability/package metadata, and source closure entry;
- every published snippet has a Rust scenario ID and a compile/execute receipt tied to the same revision and package artifact;
- capabilities distinguish remote versus embedded operation and record default transport, stream shape, cancellation, recovery, errors, and explicit wire identities;
- package instructions are generated from Rust metadata and match the installable artifacts;
- search and navigation are generated from the same bundle and are deterministic;
- release and preview version entries are immutable and resolve through the stored dataFile path, with latest defaulting to the greatest stable release;
- every viable language remains represented in the qualification ledger until it has a passing artifact receipt or concrete evidence that its maintained OSS path cannot meet the contract.

A website can render this bundle with any presentation stack. That stack must not become a second source for API references, capabilities, package instructions, snippets, search records, or version identities.

