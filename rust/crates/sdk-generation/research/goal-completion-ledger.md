# Rust source-of-truth goal completion ledger

Snapshot: current main c8bef0be476cbfd5d4fa30d1f8d017fa5336d964 (2026-10-07).
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

- Actors checkpoint `edcc1292ac2363d2ca62d24c7e9033279b32c087` is signed and verified remotely. Its semantic Rust domain derives message and oneof shadows through the maintained Protify extension, without separately authored raw field registration. Current-main integration is now in `Q:/sdk/work/sdkgen-main-port-current`; candidate `f8bf8daf64fae0c04366fd1e5326053dde3d4e9e` is remotely preserved with subsequent local lint/cancellation changes. Minimal signed scope, exact installed semantic/default-transport qualification and independent review remain open. Actors descriptor consumers require identity/option/presence compatibility; Filesystem/Harness additionally have an actual descriptor handshake, which must remain compatible.
- Stream PR259 points to signed `486bd26ac1f9c8c9a88b13a70bdcf49d5637ecb1`, including main `af6f814ccb97c3d49f93689e70b19d97d7840377`. Invalid absence admission, Rust-owned follow recovery, all-target native test lint fixes and archive-content validation are included. Required cheap CI run `37630346267` passed. Final exact-revision installed package qualification is pending; historical packages do not qualify this source by retagging.
- Filesystem signed candidate `f775b8465d836cf69b358f9fa9ebd8edbfa819db` is saved remotely on `codex/filesystem-rust-admission-final`. Peer fixes replace hosted payload-kind/counter lists with Rust-derived metadata and remove an unchecked generic string projection. Current Rust, N-API and WASM counters were verified to have the same 24 fields. A finite WASM resolved-file getter, final signed freeze and exact installed qualification remain open.
- Foundation checkpoint `e0f29067aad2ac955653657f92dc516907b53fc6` is verified remotely. Nine Rustdoc profiles execute successfully, but explicit coverage of all ten published owners/private binding profiles is not yet established. The Rust-owned Machines scenario now emits an actual TypeScript consumer that compiled and executed against an installed package. Full clean generation, immutable versions and all-scenario snippet coverage remain open.
- Historical Python, JVM, Go, Swift, C#, Dart and C++ receipts demonstrate maintained generator/runtime mechanisms at their recorded source revisions. They do not qualify the new production semantic producer. C++ currently qualifies only an opaque conformance bridge; a full caller-configurable typed SDK remains open. Ruby async support and PHP metadata consumption remain proven generator gaps under investigation.
- The original source-bound Actors Kani ingress proof remains one live process, with no overall terminal result. Direct production `exact_u32_from_f64` verification passes over arbitrary IEEE-754 bit patterns; its intentionally failing fractional-acceptance control also behaved as expected. The pinned source-bound manifest and raw logs record the exact numeric theorem. These results do not prove SDK packages, documentation completeness, transport behavior or foreign-language runtimes.
- Release assembly fixtures include placeholder archives for unrelated packages and are not whole-release runtime qualification evidence. Stream's final candidate now reuses the existing archive validator before writing TypeScript qualification receipts; 26 focused archive/publication/planner tests pass. The corrected pipeline still requires merge and exact installed Stream admission.
## Open completion milestones

Stream PR259 merged at `2026-10-07T13:53:51Z`, producing main
`c8bef0be476cbfd5d4fa30d1f8d017fa5336d964`. Signed source `486bd26ac1`
passed final installed default/recovery/browser-fallback/cancellation checks,
independent review, Node-only archive tests on Windows and Linux, and required
inexpensive CI. Final archive hash is
`a6bcfccbf0bf4d30f3561008d0e937cba8b56fae01cbd3fc121ad9edbd8567c2`.
The earlier candidate statuses above are historical. Native compiler/linker,
cache and generator build-input attestation remains a follow-up; source closure
alone does not guarantee bit-identical native binaries.

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


## Current source audit: 2026-10-07 after Stream merge

The preceding status-only answer made no authoritative change. This continuation
makes progress through final Filesystem source freeze, current-main integration,
actual browser cancellation evidence, and direct-production proof results.

- Remote main remains `c8bef0be476cbfd5d4fa30d1f8d017fa5336d964`.
- Filesystem signed source `367c45cecd67b3c0375e8d373713bc3d7e13b4c5`
  is frozen on parent `f775b8465d836cf69b358f9fa9ebd8edbfa819db`.
  It derives discriminants from canonical FilePayload with maintained Strum,
  generates payload/counter metadata, and exposes a finite WASM kind getter.
  Its diff is 119 additions and 117 deletions across 20 files, including generated
  outputs. This is not a large TypeScript deletion. Installed qualification is
  source-bound to that candidate; current-main integration and fresh final
  qualification are still required before admission.
- Actual headless Chrome exercised all eight Actors operations through packaged
  WASM and gRPC-Web. Invalid identities/digest/currentHead were rejected before
  transport. In-flight abort left one Rust request active and no server abort;
  pending connection abort timed out. These are merge-blocking implementation
  defects, not missing test coverage. Native package-shape shims do not qualify
  a real installed companion. The candidate still needs the companion package.
- Kani directly proved PositiveU64 accepts exactly nonzero symbolic u64 values
  and valid subscription wire conversions preserve oneof/cursor identity.
  Both terminal exits were zero, respectively 0/100 and 0/141 failed checks.
  Evidence: qualified-prototypes/kani-domain-cutover-current-20261007/
  receipt.current-fixed.json. The source is Q producer 0093 plus its one-line
  Kani module path fix; later semantic changes require fresh proof binding.
- Nominal Kotlin configuration now selects Rust validator exports rather than
  authored Kotlin predicates. Swift nominal generation compiles, but fresh
  installed runtime qualification remains outstanding. Shared Rust CurrentHead
  marker changes must propagate to the actual main-integration producer.
- PHP protobuf runtime fails full unsigned-u64 preservation above signed max
  under the tested maintained native and pure-PHP paths. Brick Math preserves
  values alone but does not integrate with generated setters. This cohort is
  not qualified; record concrete exclusion evidence rather than narrowing the
  contract to signed integers.
- Stream build-input attestation remains WIP. Ambient RUSTC_LINKER is not proof
  of the effective Cargo linker; producer identification must reflect actual
  configuration/invocation before that follow-up is admitted.

All M1-M6 completion gates remain open. No additional merge was performed during
this audit. Source snapshots are WIP preservation, not qualification receipts.

## Independent integration review: 2026-10-07 follow-up

This iteration made progress by inspecting actual current-main candidates and
finding defects that change admission decisions; it is not a status-only wait.

- Filesystem is now integrated on main c8 in filesystem-main-port-c8 at
  `6274988e8aab33e10d721775be31e7f3a0839cd1`. The full dependency closure is
  29 files, 1,263 additions and 644 deletions, including generated bindings.
  Root found PublicWasm's special fileKind branch skips recursive readonly
  projection for all other record fields. The owner is correcting that branch;
  independent negative compilation is required before final package admission.
  WorkCounters now exposes exact bigint values rather than bounded numbers;
  installed tests must cover counters above Number.MAX_SAFE_INTEGER.
- Actors main-based candidate is sdkgen-actors-c8-minimal at a2881c65 plus WIP.
  Root inspected its actual WASM cancellation edits, maintained generator vendor,
  loader fallback, handwritten client surface, and native assembly. The owner
  must preserve upstream license/provenance and justify the focused maintained
  generator patch. Handwritten eight-operation facade/interface definitions and
  normalizeSemantic remain gaps in the fully generated end-state, not evidence
  of completion. True cancellation and CurrentHead false unrepresentability are
  still merge gates.
- Linux Stream native artifact installed checks passed with loaded artifact SHA
  1d864635e6dc227091ac9645abc7c753ab74855fc0a79bf02ef0c7883df9e21d,
  archive SHA 3792360e5e2ecf537158a3bb37682721595354a5ead7ab4b9299cf75b8574389.
  Evidence is Q:/sdk/work/stream-repro-evidence-c8. The producer includes dirty
  peer attestation edits, explicitly distinct from compiled Rust closure. No
  production platform selector changes are admitted from this prototype alone.
  Root verified ssh ivar is reachable: Darwin arm64 and Cargo available. macOS
  qualification is assigned, rather than accepting a claim that no host exists.
- Root inspected the twenty-profile Rustdoc manifest. Its source paths refer to
  rust-source-foundation, including Actors bindings absent from main c8. The
  prototype proves available-source projection only. It does not prove current
  main coverage; receipt labels/source identity and main integration need fixing.
- Python's latest installed cohort is source 03bb, explicitly historical. Current
  main-based nominal/readonly producer qualification remains outstanding.
- Stream linker attestation fixed ambient RUSTC_LINKER misuse but effective
  Cargo configuration includes inherited/global inputs. Ad hoc repository-only
  rejection cannot establish the final linker invocation. Review remains open.

Remote checkpoints verified at the start of this iteration were foundation
8542364f7ee2983092b6196f92165cf3f0a39d39 and Actors language WIP
5f8b7061465cd632d643e90d682c5d5195d9fe5b. No merge is claimed by this review.

## Runtime review progress: 2026-10-07 connection cancellation

This goal iteration made progress through source-bound runtime probes and
qualification evidence. The goal remains incomplete; no additional merge occurred.

Root exercised the actual current Actors TypeScript source with a stalled injected
binding. The first probe reproduced shared cancellation: aborting caller one
failed caller two despite its signal remaining active. After the owner's waiter
accounting fix, the same bounded probe passed: caller two remained pending.
A second probe then reproduced a distinct retry race. After the final waiter
aborts, a new call still attaches to the aborted pending connection until its
underlying cleanup rejects. The observed connection count was one rather than
two, and the unaborted new caller received the old cleanup failure. Source hashes
and terminal results are recorded in the current candidate's
research/qualified-prototypes/actors-root-review-20261007. The owner is correcting
cache eviction before underlying abort, with identity guards for old completions.
These probes cover the production TypeScript connection state machine; they are
not transport qualification receipts.

Independent headless-browser tests now observe in-flight server cleanup
(active zero, aborted one), finite connection abort, and pre-abort no-network.
The native runtime transport getter and fallback identity need exact-source,
rebuilt-package verification. The broad same-suffix loader regexp still accepts
foreign dependency paths; the missing own module must be identified exactly.

macOS Stream research assembly on ivar passed actual native loading and stalled
TLS cancellation, with archive SHA
b331e6e14c729f371a514efd16037d67a8d4e400e1afa6a41fe0ed5982b47862.
Its tools were rustc 1.96.0 and Bun 1.3.14; repository pins are Rust 1.98.1 and
Bun 1.4.2. This evidence is a working prototype, not final reproducible producer
qualification. A fresh maintained-entrypoint build under pinned tools is assigned.
The maintained Cargo RUSTC_WRAPPER mechanism was prototyped successfully to capture
actual compiler/linker arguments without reimplementing Cargo config precedence;
minimal integration is assigned.

Filesystem numeric helper and crate-root source hashes exactly match the selected
Kani source inventory. That proves helper-source identity; it does not certify the
dirty full candidate or old package artifacts. Fresh c8 package assembly remains
a gate. Separate isolated Linux/macOS qualification is assigned.

Current Actors c8 review WIP was signed, pushed, and remote-verified as
115e983f0305459e27540c41656c9cc85a85ad27 on
codex/actors-c8-review-wip-checkpoint-20261007. Historical language cohorts are
retained as evidence, and agents have been redirected to the actual main-based
producer rather than relabeling old results.

## Source authority and package review: 2026-10-07

Current-main Filesystem metadata reports ten public acyclic Cargo owners and six
native/WASM binding packages. The twenty-profile prototype refers to PRIMARY
source (eleven owners and nine bindings), so it cannot be used as a current-main
coverage receipt. The docs-data port is assigned to derive inventory from its
actual source tree and refresh receipts after the new Actors bindings merge.

Actors current-main candidate now has the semantic unit CurrentHeadMarker with
fallible bool ingress and infallible true lowering. Generated SubscriptionStart
TypeScript preserves the literal true in the currentHead variant. Compatibility,
all binding generation, and exact-source Kani checks remain admission gates.
Root reviewed exact optional-loader identity handling and requested platform-
appropriate case/URL normalization. Historical PRIMARY native qualification
metadata is being removed from product source rather than represented as current.

Root reproduced a new packaging failure using the actual generated NAPI loader
with a simulated Linux selector. With no Linux artifact installed, the loader
throws a generic Error without MODULE_NOT_FOUND at the top; its cause chain has
missing own WASI/Linux artifacts. The TypeScript fallback's narrow top-level
module predicate therefore does not provide cross-platform fallback. Probe:
Actors current candidate research/qualified-prototypes/actors-root-review-20261007/
missing-platform-loader.mjs. This proves loader error shape, not Linux runtime
qualification. Actual installed Linux/macOS default paths are assigned.

Stream's documented Cargo wrapper capture prototype is now implemented and
root independently ran all five focused tests successfully. The capture records
actual rustc arguments instead of guessing Cargo configuration precedence.
Review found a further unverified path: cached Cargo builds can emit no compiler
invocation, while the reader requires one. A narrowly forced fresh product link
or fully bound safe capture reuse must be verified without cleaning dependency
caches or increasing ordinary CI cost. Actual pinned platform package generation
remains outstanding. The follow-up currently adds about 334 production script
lines, so code-minimality review also remains open.

Filesystem Windows regeneration was observed live through rustc PID 65236; the
source/outputs remain dirty until the owner freezes the corrected tree. Isolated
Linux/macOS recipes and fixtures are prepared. No current installed package
qualification or additional merge is claimed in this iteration.

Remote Actors review WIP b75c841225de17676c2b241e1d83b563f7206a67 is verified.
All language qualification must follow the actual main-based producer; historical
cohorts remain evidence only. Actors alone does not close other service-family
streaming, recovery, embedded, documentation, or language requirements.

## Filesystem frozen source and remote preservation — 2026-10-07

Verified Filesystem source HEAD 7cef9472f2fb69aa5c2cdf9cf394774025ee7985,
tree c63ad0c27e8fa430326f0b47bc797de3235175ad, signed and pushed on
codex/filesystem-main-port-c8. Remote main remains c8bef0be476cbfd5d4fa30d1f8d017fa5336d964.
Fresh package SHA-256 143AF0B1BE845410E2047A8FF11C457FCCE3EBA592CBF1F08B7B6D7753ED1C1D;
Windows native binding SHA-256 9A2AFD21C5A6CE593731A4B5A98DEB31E4E7796B219A3CC3980BF1E1AD774498.
Independent installed 36-case WASM/N-API matrix passed with unchanged hashes and
exact generated counter keys. Receipt remains diagnostic because untracked
research makes its source state dirty; clean-source admission and Linux/macOS
qualification remain outstanding. Manual ABI enum projections remain an authority
gap; canonical optional OSS derives are being compared with extending existing
metadata generation in an isolated prototype. The authored TypeScript source
change is 118 added /147 removed lines (29 net removed), not a bulk deletion.

Draft Filesystem PR creation attempted via gh GraphQL and REST. Both APIs returned
server failures; latest REST response HTTP500 empty body, request
D189:2B9331:5B701A:5FE1F3:6AC66238. Subsequent PR list shows no PR for the branch.
PR title/body are preserved at Q:/sdk/work/filesystem-c8-pr-body.md. No new merge.

Verified remote unqualified WIP checkpoints:
- Docs current-main port: b488143ad07a3d1e00dbcb439dfd536b449d8e95 on codex/docs-c8-source-wip-checkpoint-20261007.
- Stream actual compiler invocation capture: d766aded1fc3195b14eb599153c34328b2c0556f on codex/stream-build-inputs-wip-checkpoint-20261007.

Actors source advanced to75404e290d0da674d91c43e643fccaa6ba536bff and now explicitly
exports a NAPI transport getter. Installed84a508 artifact still lacked that getter;
rebuilding and requalification are required rather than transferring the old receipt.
Formal expected-revision solver remains verified live; compile-only c8 audit is
not a completed theorem. All16 useful subagent slots are assigned. Goal remains active.
