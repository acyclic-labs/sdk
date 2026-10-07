# Rust source-of-truth goal completion ledger

Snapshot: current main af6f814ccb97c3d49f93689e70b19d97d7840377 (2026-10-07).
Review rule: evidence is labeled by its actual source. Main-tree facts,
C-worktree research, and external qualification receipts are not interchangeable.

This ledger records whether the complete SDK and generated documentation-data
goal can be called. A row closes only with reproducible current-main evidence.

## Current-main facts

| Area | What is actually present on current main | State |
| --- | --- | --- |
| Rust workspace | Cargo lists the published family crates, including Actors, Filesystem, Harness, Inference, Machines, Objects, Stream, Workers, their WASM/native companions, and the plugin. | Existing Rust packages; a unified generation authority is not present in the main tree. |
| Inference authority | Main contains rust/crates/inference and rust/crates/inference-wasm, including the consolidated Rust contract, build script, descriptors, and WASM schema. | This family has a Rust-owned contract surface. |
| Actors authority | Main contains generated Actors proto/tonic sources plus grpc/http adapters. The Rust domain/contract source used in the C foundation worktree is not present on this main snapshot. | Existing generated/runtime surface; Rust semantic authority migration remains open. |
| Rust documentation | PR258 supplies Cargo-bound package/search metadata, versioned immutable bundles, public API projection and the safe-version proof. | Complete Rustdoc profile coverage and executable language snippets remain open. |
| SDK generation | Main has docs/rust-source-generation.md as an architecture record. It does not contain the sdk-generation crate or a completed all-owner generation bundle. | Open. |
| TypeScript | PR263 moves Machines primitive transport/page-size admission into Rust. Inference has a Rust-owned shared contract. PR264 merges Rust-derived Machines unions and policy projections, removing 56 authored TypeScript lines. | Broad family contract/behavior replacement remains open. |
| Website data handoff | Main documents versioned release/preview data and catalog concepts. No website/Svelte consumer tree is present in this repository snapshot. | The Rust-generated data contract is in scope; website presentation and deployment are outside this goal. |
| CI policy | PR262 supplies cheap ordinary qualification policy and cache reuse. PR259/264 ordinary runs pass without downstream language matrices. | Release/manual downstream coverage still requires the final generated pipeline. |

The main snapshot has ten published owners in the current generation inventory.
The eleven-owner generation fixtures and their partial receipts belong to the
C foundation research worktree; they are not main-tree completion evidence.

## Evidence outside current main

- Actors checkpoint `edcc1292ac2363d2ca62d24c7e9033279b32c087` is signed and verified remotely. Its semantic Rust domain now derives message and oneof shadows through the maintained Protify extension. Package/file/service registration contains no separately authored raw fields. The candidate still needs latest-main integration, exact installed package qualification, descriptor handshake verification, and independent review before merge.
- Stream PR259 points to signed `92c267c08b4cd2cac7b1347b05547f3c1dab768b`, including main `af6f814ccb97c3d49f93689e70b19d97d7840377`. Invalid absence admission, Rust-owned follow recovery, and all-target native test lint fixes are included. Final exact-revision installed package qualification and required cheap CI are pending; historical packages do not qualify this source by retagging.
- Filesystem signed candidate `f775b8465d836cf69b358f9fa9ebd8edbfa819db` is saved remotely on `codex/filesystem-rust-admission-final`. Numeric admission and finite payload types are present. Root review found a handwritten hosted payload-kind list, an unchecked generic string cast, and a possible hosted counter coverage regression. These must be resolved before admission. No final installed qualification is claimed.
- Foundation checkpoint `66e3bba580562cbe7c2498ae7dfd374646591925` is verified remotely. Full Rustdoc owner/private binding profile execution, immutable versions, snippet execution, clean regeneration and drift remain required.
- Historical Python, JVM, Go, Swift, C#, Dart and C++ receipts demonstrate maintained generator/runtime mechanisms at their recorded source revisions. They do not qualify the new production semantic producer. C++ currently qualifies only an opaque conformance bridge; a full caller-configurable typed SDK remains open. Ruby async support and PHP metadata consumption remain proven generator gaps under investigation.
- The source-bound Kani ingress proof remains one live process, with no overall terminal result. Existing cursor and bounded domain proofs do not prove generated SDK packages, documentation completeness, transport behavior, or all foreign-language runtimes. Exact filesystem float admission still requires its direct production proof.
- Release assembly fixtures include placeholder archives for unrelated packages. The hash-only TypeScript archive receipt helper does not validate archive package identity. Its receipt is not whole-release runtime qualification evidence; maintained archive validation remains a concrete pipeline gap.
## Open completion milestones

### M1: Rust-owned generation on main

Land or otherwise verify a main-tree generation entrypoint that starts from
Rust contracts and Rustdoc metadata, emits descriptors, SDK metadata, docs
inputs, snippets, and manifests, and records exact source/tool/generator hashes.
Qualify all ten current-main published owners. Resolve generated source-span
provenance, generic receiver projection, and clean regeneration/drift.

Exit evidence: a clean current-main bundle, a second clean regeneration, a
passing drift check, and a manifest covering every generated output and input.

### M2: Rust semantic exports and TypeScript replacement

Connect Rust semantic exports to the TypeScript generation bundle. Preserve
nominal IDs, bytes, u64, enums, option/presence, errors, streaming, cancellation,
recovery, and embedded behavior. Remove each replaced handwritten shared
contract/facade/behavior implementation only after its generated replacement
passes installed-consumer tests.

Exit evidence: changing a Rust declaration regenerates TypeScript declarations,
packages, snippets, and drift output without manual synchronization.

### M3: Generated documentation data for website consumption

Generate from Rust comments, Rustdoc API data, crate-owned Markdown, and
executable Rust examples:

- API references and navigation;
- capability and transport tables;
- package installation instructions;
- executable language snippets;
- search catalog and metadata;
- release/preview version catalog with immutable identities and latest selection.

Tie every data bundle and snippet to an exact Rust/SDK revision. Compile or
execute published examples against matching artifacts. The website Svelte
presentation, visual preview, and production deployment consume this generated
data but are outside this goal's implementation boundary.

Exit evidence: a clean generated data bundle with complete source coverage,
working links, version catalog integrity, and successful snippet execution.

### M4: Strongly typed installed language qualification

Qualify every viable inventoried target from the Rust source: TypeScript,
Python, Go, Kotlin/JVM, C#, Swift, C++, Ruby, PHP, Dart, and any additional
maintained target discovered by the research. Track remote and embedded
capabilities separately. A target may be marked supported only with an
installable package and applicable serialization, transport, streaming,
cancellation, recovery, typed-error, and embedded evidence. A target may be
marked excluded only with concrete maintained-tooling or capability evidence;
otherwise it remains outstanding.

Exit evidence: current-main source-bound receipts and package hashes for every
supported target, plus evidence-backed exclusions for targets not retained.

### M5: Seamless transport and platform behavior

Complete the Rust-owned automatic transport policy. Native consumers use the
best native transport, browser consumers use the browser transport, and
platform selection remains internal. Add and qualify the required browser
ingress for supported service families, including the semantic coverage needed
for streaming and Objects uploads. No consumer feature flag or platform loader
choice is part of the public API.

Exit evidence: the same Rust-owned scenarios pass for supported native hosts and
browser consumers with no platform configuration.

### M6: Low-cost release verification and final review

Keep ordinary CI to fast source, generation, drift, and focused Rust/TypeScript
checks. Run downstream language/package/platform checks on release or explicit
manual dispatch, with exact artifact reuse only when the complete source closure
and toolchain identity match. Verify local Windows, WSL/Linux, and SSH macOS
lanes. Review generated deletions, source coverage, data bundles, language
receipts, and release/workflow paths before merge.

Exit evidence: current-main release/manual rehearsal, machine-readable receipts,
and review approval for every generated replacement.

## Completion rule

The goal can be called only when M1 through M6 have current-main exit evidence.
C-worktree prototypes, historical receipts, bounded proofs, and closure
equivalence can support a milestone but cannot close an unqualified generator,
language, snippet, generated-data, or transport requirement.

