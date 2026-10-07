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

GitHub recovered: draft PR267 https://github.com/acyclic-labs/sdk/pull/267 created and attached. Verified head7cef9472f2fb69aa5c2cdf9cf394774025ee7985 and isDraft=true. Initial statusCheckRollup/run list empty; no CI result claimed. No merge.

## Canonical binding reuse and cross-platform review — 2026-10-07

Previous goal turn progressed: created draft PR267 and pushed source checkpoints.
This iteration verified all16agents active, PR267 exact7cef stilldraft/no reportedCI.
All3Filesystemcommits have valid signatures. Actors c8..779 commits are unsigned;
a signedcoherent review tree and exactsource artifact rebuild remain required.

Filesystem canonical reuse prototype bd1b19d0e0821d57809835452714fe53137aa430 is
signed and remotelyverified on codex/filesystem-enum-prototype. Root challenged
external-type failures as insufficient evidence of an OSS gap. Subsequent probes
proved canonical optional Tsify/serde/NAPI derives, including Strum discriminant
passthrough. Maintained NAPI3.10.5 generateTypeDef merges allcore/bindingmetadata:
canonical FileKind/FilePayloadKind declarations appear once without a custom
renderer. Owner is integrating this into PR267;7cef package is historical.

Actors review confirmed actualNode fileURL WASM initialization failure and Linux
GNU/musl mismatch. Owner fixed both atb320; reviewer qualified installed779 package:
WSL all8grpcweb/realabort active0, macOS normaldefaultgrpcweb, exactlibcselector,
Windows nativegetter and all8, semantic nestedoptional/oneof andreadonlytypes.
Facade operations nowdescriptor-derived. Root further found publicbrandedinputs
stillrequirecasts despitehiddenNAPIvalidators; Rust-backed publicfactories are
assigned. Custom29lineNodefacadegeneration is being moved toRustentrypoint to
match approved architecture. Oldsource/artifactreceipts remainboundoriginalheads.

Docs CLI review found absolute manifest paths and overscoped sourcewalker,
potentialstaleRustdoc acceptedatunchangedcrateversion, immutablepreviewrewrites.
Relocation/sourcebinding/previewrelease fixes assigned; no reproducibility claim.
Stream reviewer found actualcapturedargv manifests similarlyembedabsolute paths;
normalizedpublishedidentity/externalrawdiagnostics nowimplemented and6focusedtests
pass, but actualsignedallplatform cold/cache/relocation artifacts remain pending.

Recovery artifacts uploaded to existingunpublished draftbackup-rust-sdk-docs-20261005:
asset619087767 acyclic-fs-0.2.0.tgz1982214bytes remoteAPI SHA256143af0b1be845410e2047a8ff11c457fcce3eba592cbf1f08b7b6d7753ed1c1d;
asset619087758 fs-native-archive-current-c8-independent-20261007b.tgz9514990bytes remoteAPI SHA25610c70e8f15b9ebef52ed58c384484906bcca7a54470e4d9dd5a375d208991b27;
asset619087764 filesystem-7cef-backup-20261007.json explicitlydiagnosticWindowscohort.
Draftstatusverifiedtrue; no release/registrypublication. DiskcurrentlyC229.9GiB/Q303GiBfree.
No newmerge. Goal active, broadlanguage/streaming/docs/sourcecoverage stillincomplete.

Root independently found and reviewer reproduced a public TypeScript brand-construction gap: installed package exports no runtime ActorId, CodeSha256 or PositiveU64 constructors; internal import is blocked; no-cast consumer fails. Native and browser invalid inputs are correctly Rust-rejected without network, but public Rust-backed constructors remain a merge gate. Actors WIP is verified remotely at 098e93f31884e45f5d44e096175f073fd88e977d. Kotlin now has real macOS JVM qualification; Swift Linux remains viable and outstanding, not excluded merely for missing tools.

### Signed Filesystem cutover and independent generation audit — 2026-10-07

PR267 current signed head d09267fec3474572a8222327f109963b4dd5a38b unifies canonical core enums through maintained Tsify/NAPI derives. Root independently verified signatures, remote PR head and package/native hashes. GitHub CI run37651963273 failed native workspace compilation because TypeScript feature unification enables wasm-only Tsify derives; policy also reports duplicate cfg_attr. Filesystem owner is correcting these locally before a new signed freeze. No merge performed.

Root preserved exact d092 package in the existing unpublished recovery draft: asset619261343 fs-d09267fec3-0.2.0.tgz, remote digest120719fd97406bc82aba87cf9874a8b66b97ace098ba58029a3414aaa5ab7936. Native recovery archive asset619261377 fs-native-d09267fec3-win32-x64.tgz, remote digest58b4b2470d98d73e7c02d3e423a545e0d0b738f84b75957adf022b3851c84f95. Raw native file4b7b870930da5505f98b93a6da750aed1597b89326b747bb87aaac886533d157. Root verified remote asset state uploaded and API SHA256 values.

Docs source WIP signed remote checkpoint054933436f0e2b408feb6bad1a097ef2e8c73991; Stream producer WIP343703e867a599bf1de18931ac06840e6d835634. These are preservation checkpoints, not qualification claims. Actors latest1ac5b398644d9d7056a95cd579c6a5315dd98db8 preserves no-argument JSON route contract; root executed the Rust exporter and observed all8 routes. Signed final Actors review/package freeze remains outstanding.

Independent docs review reproduced stale Rustdoc acceptance during generation, manifest identity tamper acceptance, release scenario bypass, source inventory omissions, and Unix rename overwrite risk. Owners are correcting the actual CLI; fixture determinism alone does not establish finished source authority. Goal remains active. All16 child slots replenished with useful current-source work.

### Actors signed review PR and docs end-to-end gaps — 2026-10-07

Root pushed signed Actors1269511d636b8ceeea1604a23919a9c1448241fa, verified exact remote branch codex/actors-c8-minimal-signed, created and attached draft PR268 https://github.com/acyclic-labs/sdk/pull/268. CI37657895411 plan passed; gate/policy observed live. No merge. Current main is foreign mergecd6ab86bf9f5fbfa8eeb416b6604de379df67903; compatibility has been locally tested but Filesystem PR267 now reports conflicts requiring a main integration preserving peer work.

Docs WIP remote checkpoint3a76f23457c5a0481c80c11826f26c947aa635f4 includes CLI fixes and source-closed scenario receipts. Reviewer verified stale Rustdoc/source attestation rejection before generation, strict manifest/revision handling, release scenario bypass rejection, and concurrent no-clobber publication. Both endpoint scenarios executed authenticated local fixtures. Remaining root findings: the CLI still requires manually supplied rustdoc directory/sidecars rather than rebuilding all profiles through the Rust entrypoint; profiles hard-code default features and empty feature sets instead of binding actual producer configuration. SourceState and plugin closure fixes/tests remain under review. No whole-generation completion claim.

Stream production5347821b1d5570ba15f29d00d74cdd761bb95399 is signed and remotely verified; its parent is the historical original Stream source. Independent review found plain Windows flag escaping, encoded-linker normalization and early error environment restoration issues. Validated uncommitted fixes were discarded during a peer worktree switch; reviewer is recovering into a separate worktree and signed remote patch. Shared checkouts must never discard peer changes. Root preserved the prior Stream WIP checkpoint343703e867a599bf1de18931ac06840e6d835634 before this switch.

### Native feature fix and Cargo flag precedence review — 2026-10-07

Root verified Filesystem signedba8d1fa931912e69a9c6e591423552b58fe57557 remotePR267 fixes nativefeature Tsify gating; PR stillconflicts withmain andrequiresisolatedintegration. Numeric sourcechangedMAXcast tof64::from, so formalownerrequalifiesnewsourcehash ratherthanretaggingprior51276receipt.

Actors signedworktree919a04d83a265d90f1bce7ede2dfdbfa301829a9 locallyformats7ownedRustfiles; remotePR268 wasstill126951 atobservation. Independent maintained npm Node20.19.0 runtime qualification validates defaultnative loader andRustconstructors, binarySHA6E3A39787E667D50487F7335C85636C2823A53E636D73C2C841D45DA4E57906. FullNode20operations/fallback/cancellation is assigned; loader-onlyevidence is scoped. Root requestednominal CodeSha256 mutableUint8Array typing audit againstRustprivateimmutablebytes.

Stream ownercurrentmainsignedfollowup30bdc7715c3f68579a24ca9e80b94fce7567c905 is887netadds infourfiles andunderindependentminimalityreview. Root directlyprobed deterministicRustflags with bothplain andencodedflags andconfirmed bothwerecombined. Cargoofficial documentation saysencoded>plain>targetconfig>buildconfig aremutuallyexclusivesources: https://doc.rust-lang.org/cargo/reference/config.html#buildrustflags. Ownerandreviewerarecorrectingactualprecedence includingemptyencoded/configretention beforefreeze.

Docs CLI5activeintegrationtests nowPASS stalerustdoc,sourceState,pluginclosure,release-skip,concurrentpublication. Profileaccuracy audit remainsopen: externalreceipts mustnotfabricate defaultFeatures true andemptyfeatures; generatedprofiles needCargo capability matrix coverage withminimalboundedfeatureprofiles. Allgoalrequirementsremainincompleteuntilactualqualifiedsourcecutoversandlanguagesfinish.

## 2026-10-07 current-main Filesystem integration

- Root integration now owns `C:/Users/varun/.codex/worktrees/filesystem-main-review/sdk`, main `cd6ab86bf9f5fbfa8eeb416b6604de379df67903` plus Filesystem candidate `ba8d1fa931912e69a9c6e591423552b58fe57557`, pending merge commit.
- Resolved authored conflicts by preserving main's extracted operation-window adapter and optional lease commit arguments while retaining Rust-owned exact-u32 boundary types. Maintained WASM and N-API generation completed successfully from the combined source. Native typescript+napi-types feature check and formatting passed.
- Combined typecheck identified main's operation-window adapter still importing deleted TypeScript rebase validation. Assigned Rust core positive-limit validation before durable transitions plus adapter deletion to Filesystem owner. Final binaries must be regenerated after this fix. Full Rust tests are running; no integration merge qualification claimed.
- Exact integrated numeric.rs and lib.rs production hashes match the ba8 theorem inventory; formal owner recorded source binding without claiming a new solver run.
- Stream independent clean Windows build proved RUSTC_WORKSPACE_WRAPPER avoids dependency command-line overflow while preserving existing RUSTC_WRAPPER. Cached binary nondeterminism and missing effective implicit-linker/profile provenance remain unresolved production blockers.
- Docs independent parser/mutation suite now has 9 passing tests; full default CLI profile/catalog generation still needs completed end-to-end evidence. Generated versioned data remains SDK-only.
- Actors signed 919a04d source remains unpushed at observation; mutable byte type aliases remain blocker. Maintained Windows/Linux/macOS companion assemblies exist, but final source/package qualification must follow the generic readonly projection fix.
- JVM Stream source and three-platform artifacts/receipts now preserved in the existing unpublished GitHub recovery draft with verified API digests; no registry publication/deployment.
- Goal remains active. No new merge performed in this iteration.
### Integrated Filesystem source frozen remotely

- Signed merge `ced6f3ed8413723ba1e399855f7aff9853e79b30` preserves both current main and ba8 history. Signed generated-output update `c2e2d8ef80ab88a989c1f5955e6f35881f0274cd` is clean tracked and verified on remote PR267 and codex/filesystem-main-review-integration.
- Maintained native/WASM generation after Rust operation-limit fix passed. Native ABI and TypeScript adapter passed using pinned Bun 1.4.2; TypeScript build and type fixtures passed. The source delta removes 30 net authored TS lines relative to maincd6.
- Full pre-final-guard filesystem run passed 1,149 library tests plus every available integration suite (36 library and one integration test explicitly ignored by their own environment gates). Final source operation-window suite passed all 16 tests, including zero-limit no-close/no-recovery-claim.
- The Rust pure reconciliation-limits predicate is now directly proven over all three symbolic u32 fields, with a failing negative control. Stateful mutation safety is a regression test claim, not a mathematical proof claim.
- Initial c2 CI37666962372 failed because GitHub retained old PR basec8, including main's Web Flow signedcd6 in SSH-only PR verification. Refreshed PR base tocd6 and reopened existing draft; no signature policy was weakened. Fresh CI37667870393 plan, policy, gate, and SDK Qualification jobs passed at observation; final record job still live. Windows hosted job skipped; Windows local ABI/adapter checks already passed.
- Final source-specific clean installed release artifacts and validation are assigned to fs_attestation. Separate signed validator tool commit4633ad6178e9ae296da1f8969f9edb476bad9340 must be pushed and recorded independently of SDK source; research runner stays outside product PR.
- Actors readonly exported type and constructor matrix now passes at d69057b2b248fcf930aaf962bbaadd0d52297731, with Node20 native and realHTTP1 gRPC-web all-eight/cancellation behavior; old919 native/WASM provenance is still insufficient until new source-bound rebuild.
- No new merge to main performed. Goal remains active; all16 useful subagent slots remain assigned.
### 2026-10-07 exact Filesystem package remote preservation

- Source: c2e2d8ef80ab88a989c1f5955e6f35881f0274cd, PR267 remains open/draft pending final native release qualification.
- Maintained Bun package archive: Q:/sdk/work/fs-package-c2e2d8ef80-20261007/acyclic-labs-fs-0.2.0.tgz; SHA256 5a539bdf2ce8db210dacbbafb5f8666395aba9dc3e31b0b01de061a285a70d43.
- Installed externally in Q:/sdk/work/fs-installed-c2e2d8ef80-root-20261007: public memory consumer and workspace composition completed exit 0.
- Uploaded to existing unpublished recovery draft404134409, asset619534879. GitHub API state uploaded, size2046477, digest sha256:5a539bdf2ce8db210dacbbafb5f8666395aba9dc3e31b0b01de061a285a70d43. This is preservation, not a registry or public release.
- Fresh exact-head CI37667870393 completed success. Earlier same-head obsolete-base event37666962372 remains failed in the PR rollup; do not claim all rollup checks green.
- Actors signed af8c7845d639ea2eaa2787c7b29d7d8b4c70789a now delegates native companion metadata/artifacts to maintained NAPI-RS3.10.5, deleting34 custom lines. Final platform qualification and post-Filesystem-main integration remain outstanding.
### 2026-10-07 refreshed main integration

- Main advanced to424bfe3d8419035e54048d74c2504a1416f798aa through peer PR269. Its Harness discovery and Filesystem retained-generation changes are preserved.
- Filesystem signed merge1e94392e9d1d34772132197480c69081b593d4d4 is pushed to both PR267 source and review integration refs. Parents c2e2d8ef80 and424bfe3d. Rust source merged, WASM regeneration passed. Full TypeScript noEmit and formatting passed after repairing the retained-generation validator import and its required label. Native declaration regeneration remains live session76031; final installed release qualification must bind this new source, not oldc2 artifacts.
- Actors signed integration2e2f53e2c9eb90fd4f84d5ef63e01a894eab2bb2 is remotely verified on codex/actors-main-integration. It includes74b1d6c7 plus current main and Filesystem1e943. Cargo's maintained offline workspace update resolved package-addition conflicts by adding the same8 main dependencies, without updating existing external versions. Final minimality scope and integrated-source packages remain outstanding.
- Oldc2 numeric admission: Windows and Linux each18WASM+18NAPI cases passed with unchanged source/artifacts. These are historical source-bound results. macOS c2 build failed disk space; owners are removing only completed reproducible caches after preserving artifacts, never foreign/live targets.
- Stream signed1e5856a2 marker ownership fix passed14tests and independent ordinary fresh/repeat/relocated five-file equality; differing owner/reviewer native hashes remain under recipe review before a broad reproducibility assertion.
- Docs full54 profile execution exposed a missing feature-only FilesystemS3Adapter catalog join. Regression authored; actual final output is still unqualified. Do not reduce profile coverage to make generation pass.

### 2026-10-07 Actors minimality and final-source qualification

- Signed Actors integration fcd86db81ba5076abf0a03758ff20593e90aab71 removes the optional UniFFI crate and dependency graph from the Rust/TypeScript migration; previous source remains in remote history for later language qualification. The 505-line proof module is replaced by direct PositiveU64, ActorLimits, and enum-mapping harnesses. The change is 102 insertions and 1,295 deletions across 14 files.
- Signed c551f225e4 integrates the generated public readonly semantic namespace. Signed ec0496fea5 corrects the bridge scope documentation and proof module name. All are pushed on codex/actors-main-integration; no main merge was performed. The separately enumerated readonly export list is being replaced with shared executable Rust type metadata before the final freeze.
- Local Actors Rust library run passed all 76 tests after scope pruning. Descriptor compatibility is running against the integrated checkout. Root TypeScript validation found missing @types/node in this checkout, not a qualified source result; matching dependencies and external no-cast consumer qualification remain required.
- Current Filesystem1e package SHA256 is 595a6c41ac8e7308303a4786b9b8cb3f4f6f629209ebe71a388d58877368076b, externally installed public memory/workspace consumer passed, and API-verified recovery-draft asset619612441 preserves the package. Fresh CI37674208593 completed success. Exact-source package WASM preflight passed; native release admission and Linux/macOS receipts remain outstanding. Root native declaration producer76031 remains live, with progress observed; no historical c2 artifact was retagged.
- Stream recipe source66bea4293b441744c95fc31ff43096e45c324d7f is signed and pushed, with 14 focused producer tests passing. It pins CARGO_INCREMENTAL=0, Cargo-compatible CARGO_PROFILE_RELEASE_INCREMENTAL=false, and maintained NAPI --locked, restoring caller environment afterward. Exact-source artifact reproducibility remains outstanding.
- Docs feature-profile union and immutable release provenance implementations exist but updated runtime output is not yet qualified. Q:/ copies alone are local preservation, not remote evidence. No website merge or deployment is authorized in this scope.
- Goal remains active; all 16 useful subagent responsibilities are assigned and completed agents receive bounded follow-up work.

### 2026-10-07 final package preservation and semantic admission review

- Actors integration signed bb8f10e07a7b14aa6ab7819618052b69f670d65b is verified on remote. Its maintained ts-rs visitor discovers nested public types from Rust metadata; generation uses Config::default rather than caller environment. Exact integrated source passed 77 library tests and 12 descriptor compatibility tests. TypeScript source typecheck passed with matching dependencies; this is distinct from final installed package qualification.
- Actors Windows installed consumer at signed follow-up 4883fda939acc7eba2bbe4cbd4668a7717648182 passed all eight real TLS operations, Rust-backed nominal constructors, readonly negative compilation and cancellation with server abort. This follow-up is not yet integrated or the final source freeze. Direct public wire-to-semantic Binding and SubscriptionSpec conversions still bypass existing constructor predicates; maintained macro post-conversion validation is assigned before final qualification.
- Filesystem PR267 remains open/draft at exact signed 1e94392e9d1d34772132197480c69081b593d4d4. Fresh CI37674208593 completed success with Windows hosted build skipped. Root native declaration producer76031 completed successfully without declaration drift. Final Windows and Linux admission each passed 18 WASM and 18 N-API cases with unchanged source/artifact hashes. macOS remains unqualified after a disk-space build failure.
- Recovery draft404134409 API reports uploaded exact-source parent package digest595a6c41ac8e7308303a4786b9b8cb3f4f6f629209ebe71a388d58877368076b, Windows companion digest aed619b91f54b69800176d07262e2875e5af13ada34ad764a0ebec0f59cfefce and Linux companion digest ffd00d759377336a05bfee5632ed950fd98fdd3844daa42c29cf331449f797d6. Corresponding Windows/Linux receipt digests are 8df665043f4a79bdd6e473543c0f6f8a3439d09f6a505d17993d3187b89ef12d and 2241185997c5566a3599617853591f4de5ee9633cfe113595b17033256e52b87. No registry publication.
- Stream signed bb313bb18fb8c51fa695868280445421664eb533 contains atomic bundle publication and nonfatal post-commit backup cleanup. Owner reports 16 producer and 38 qualification tests plus Windows external TLS cancellation guard; final signed independent review and other platform lanes remain outstanding. Four-file branch adds substantial custom build tooling, so minimality and reuse of Rust generation facilities require explicit review before merge.
- Harness bfb90df1cf4f1d05382eacc1a11a83d4045248ea permits async factory retries after failed WASM fetch, but independent review reproduced valid synchronous policy helpers misclassifying initialization failure as invalid input. Complete automatic async initialization at every Rust-backed public boundary is assigned; this revision is not merge-ready.
- Docs owned source is saved in signed remote checkpoint455d4b7a84e9d4afcb283bee0d7e63d23f60de94 on codex/docs-source-checkpoint-20261007-r3. Release manifest uses the existing sdk-docs version encoder instead of a separately authored copy. Updated full profile output and release-manifest runtime checks remain pending; the checkpoint is preservation evidence only.
- All 16 child agents are running useful bounded work. No new merge occurred. Goal remains active, including qualified additional language cohorts and generated versioned docs data.

### 2026-10-07 isolated review PRs and integrated admission hook

- Stream draft PR270 https://github.com/acyclic-labs/sdk/pull/270 is created and attached. Its four-file diff is independently reviewable. Fresh/cache native bytes match, but diagnostic-only rustc --diagnostic-width=79 changes published manifests; normalization preserving raw capture is assigned. CI37684958000 also correctly rejects unsigned ancestor42a033d5; signed tip alone is insufficient. Repair signed ancestry without weakening policy and rebuild exact-source metadata before final qualification.
- Actors loader is integrated as signed c413560567. Maintained container validation hook942b0fb001 and borrowed-predicate refinement05255453d9534b12f846bcbcc0cee1058ac8ccf9 are signed, clean and verified remotely. Exact integrated source passes 78 library tests, 12 descriptor compatibility tests and TypeScript noEmit. Direct request wire ingress still needs canonical cross-field validation; native nominal fallback and optional scalar shape findings also remain assigned. No final package freeze yet.
- Filesystem formal proof draft PR271 https://github.com/acyclic-labs/sdk/pull/271 is created and attached, stacked on PR267 source branch. Its five files add cfg-gated direct production harnesses and proof instructions; no solver receipts or raw logs enter product code. Both pure-function proofs and expected-failing controls have been independently audited. Retarget after the prerequisite merge; no auto-merge.
- No new main merge. Full goal remains active.

### 2026-10-07 request admission and fully signed Stream base

- Actors signed integration d59a58792e56fb5aed0d2da08f10d0cd803255c5 adds request post-conversion admission checks for create/update/add, invoking existing canonical validators without recursive conversion. Constructors use the same ingress path. Root and independent review each observed 79 library tests passing; root also observed 12 descriptor compatibility tests passing. The worktree is clean and pushed. Native-export failure handling and optional scalar property alignment remain pending before the final package freeze.
- Root created codex/stream-signed-atomic-base at signed acd50808df3dc746dad24f65e144db0db35269a0 with the exact bb313 source tree and signed parent66bea429. Diff between old and new trees is empty; every commit in main-to-new-base ancestry verifies authorized signature. This combines the unsigned atomic-publication commit and its cleanup follow-up into one signed commit, leaving the original remote branch/worktree untouched. Final owner fixes must build on this signed base and regenerate source-revision metadata; old package receipts must not be retagged.
- Main remains424bfe3d8419035e54048d74c2504a1416f798aa at API observation. No new main merge. All16 useful subagents are active and the goal remains incomplete.

### 2026-10-07 public package qualification gates

- Final macOS1e94392 admission passed, joining Windows/Linux with matching clean383-file source closure, package/WASM hashes and24work-counter fields. Darwin companion archive bde53635abc115fec5eb4559f4ac5e0037030132097e4f2827a016f5b546a8fb and receipt8021dc113d280eb96591245974077d7280802956a09286e5fe92dd9eb2353816 are API-verified uploaded on recovery draft404134409.
- Broader semantic audit found public openNativeFs dynamically imports a .node file, rejected by both Node24 and Bun1.4.2. Direct createRequire companion admission harnesses remain valid for their scoped numeric/ABI claims but do not prove the public loader. Owner is fixing the public loader and qualifying actual installed entrypoints before merge.
- Signed Filesystem test-only followup b7930c0ceade7d42196f1756bdaa394b9078bd39 corrects four stale number assertions to bigint and initializes the direct-WASM fixture. All16 wasm-adapter tests pass with90expectations. Prior1e artifacts/receipts retain their original identities; changed test-source closure is not silently retagged. PR267 stays draft pending public-native fix and final qualification.
- Actors signed0bc28708c972bef878c1e45a255ad64fab24f570 integrates fail-closed selected-native nominal exports and exact optional scalar projection. Non-oneof EXPLICIT absent fields become own undefined properties; oneof and legacy-required fields are excluded. Root TypeScript check and3focused runtime/presence tests pass. Rust sources remain identical to d59's79library/12descriptor qualified source. Final external package qualification is assigned.
- Harness signedbb932f2a implements async policy/client and identity boundaries through Rust. Browser runner produced no result before hanging; source typecheck still needs exact maintained Objects declarations. No browser qualification or merge claim.
- Docs foreground run7 session37987/PID84768 was authoritatively live at root observation. Eleven current CLI regression tests pass; complete54-profile generation and drift remain pending.
- No new merge. Goal remains active.

### 2026-10-07 final public-loader freeze and minimality review

- Filesystem PR267 signed remote head84260e46a7558e3b693a4f003810b811031b60ce: createRequire installed native loader, bigint cache limit preserved, ObjectCacheOptions derived from Rust native declaration. Root TypeScript compilation and 17 tests/95 expectations pass. New package7a6225a14e7d80ff3cd963591f7564b979e3cbb5dc98b2f6c1d5b111d0650caf passed external installed Node24/Bun1.4.2 Windows and WSL Linux; 314 native producer inputs unchanged. macOS public consumer remains outstanding due Tailscale authentication; no three-platform loader claim.
- Stream PR270 now signed c8f5466f0a28918ff9e6f6594fb5781ed9d52e51, cheap CI37688920922 passed. Three-platform cold/repeat bundle identity and TLS cancellation receipts complete; independent final review pending. No root merge yet.
- Actors root9924150476d8ac61bd0f3c73789af57781d65832 fixes MODULE_NOT_FOUND absent optional-companion fallback. Source compilation passed; installed package baseline qualification active. Independent minimality audit identified duplicate maintained NAPI-RS runtime selector; isolated deletion/refactor assigned before final merge candidate.
- Current main4e3ed22bdc2137a93cccdd66a3cbb815acf897cc includes peer PR253 Harness recovery changes. Harness owner must integrate preserving peer semantics and finish async consumer test migration.
- Docs and Rust-owned example WIP remotely preserved signed60c0c67583ebbc100e7102f10628612f6dd434d7 branchcodex/docs-source-checkpoint-20261007-r4. Foreground docs run7 PID84768 verified live CPU583.25; generation incomplete.
- Goal remains incomplete. Broad source authority, deletion, languages, docs and proofs gates are not replaced by these bounded results.

### Filesystem merge completed — 2026-10-07
PR267 merged by root using authorized admin squash after current-main cheap qualification37690208862 passed. GitHub verified MERGED at21:37:49UTC, squashcommit a70ba20573f8806e848bd3ef26b3273d3b6a63e8, reviewed signed integrationhead cf17845441baf464eac76359e3eeaa3bcaef87ac. Exact Filesystem source matches qualified84260. Final public-installed Node/Bun Windows/Linux/macOS passed loader and pending-operation cancellation with committed-baseline recovery; receipt API digest324772ef8ae55de57be07b17cb867644bbf4b542dcc11ba1c73fed5d7a985610 verified. Net authored Filesystem TS src lines reduced54. No package publishing/deployment. Proof-only PR271 restacked signed5294c2d9ad9b4dbf7e7142ad8fb8afee1f89b1ce onnewmain,5files112addedlines; originalbbproofbranchpreservedremote. Broadgoal stillactive/incomplete; Actors maintainedloader97094 reviewpending, Streamf6 guardfix current-source requalificationpending, docsPID84768 live, Harness currentmain integrationpending.


### Current merge gates and authenticated Mac lane — 2026-10-07
- Root verified authenticated ssh ivar returns Darwin. No foreign caches or live processes removed.
- Actors signed dbaa865580 integrates test-only failed WASM initialization retry on production77edee6170 and is pushed remotely. Exact maintained loader provenance review passed; permanent fatal native cases still need Bun execution coverage. Current client removes50 lines against its previous implementation, while the full Actors migration still adds authored TypeScript overall. Final three-platform installed qualification is pending.
- Filesystem proof PR271 af1ecde13 passed cheap CI37691809979. Merge remains held for the documented numeric harness hash mismatch: current git blob59635972 versus recordedD196; pinned solver-source equivalence or an exact-source rerun is required before correcting the evidence.
- Stream PR270 current-main integrationc71234494 preserves all four intended product files byte-for-byte from qualifiedf6, with no Filesystem/Harness rollback. Windows/Linux/Mac f6 reproducibility and installed tests passed. Root workspace manifest/lock changed at integration, so dependency-input equivalence or fresh c712 qualification is required; f6 receipts retain their identity.
- Harness signedc14d5b2d9 consumer migration passed238 tests and is remote; current-main integration and package provenance preservation are assigned before PR.
- Docs run7 PID84768 remains live at CPU2207.31, all54 rustdoc receipts exist but no aggregate generation manifest. Seventeen Rust scenarios pass independently; skip-scenarios run7 does not itself qualify those executions.
- Goal remains active and incomplete. Sixteen useful child responsibilities remain assigned. No additional main merge, publication or deployment in this checkpoint.

### Reviewable Actors PR refresh and package assembly fix — 2026-10-07
- Root preserved stale PR268 headd69057b2 remotely on codex/actors-pr268-pre-minimality-20261007, then exact-lease updated the draft source to reviewed integration. Current signed3e6bd8c232 includes the parent-archive fix: retain maintained generated/native loader and manifest, exclude only native binaries. Owner's genuine assembled Windows archive exposed the omission; previous manually assembled fixtures were insufficient packaging evidence. Fresh actual Node/Bun platform qualification is assigned.
- PR268 title/body now describe the Rust/TypeScript-only integration and actual pending installed gates. No merge claim. Rootchanges are pushed on both integration and PRsource refs.
- Streamc712 cheap CI37693059734 is allgreen. Independent review proves the reachable320 dependency/version nodes matchf6, but current whole-closure provenance rejects oldarchives after rootmanifest/lock changes. Fresh c712 qualification requested instead of adding custom equivalence machinery solely for a merge.
- Docsrun7 PID84768 remains live CPU2354.67 and emits preview/version/profile datasets. Aggregate manifest/drift remain pending. Generated language snippet compilation must be distinguished from running Rust examples that emit snippets.
- Filesystem proof evidence audit identified historical negative controls as deliberately false acceptance assertions against unchanged production, not production mutations. Proof documentation and PRclaim correction are assigned before merge.
- Broadgoal remainsactive; no additional merge in this checkpoint.

### Exact installed Actors qualification and remaining admission gaps — 2026-10-08
- PR271 proof-only change merged as 8b547b48ca8d112dce73543aae71a73a3c1fc339 after correcting negative-control scope and preserving canonical source equivalence evidence on codex/filesystem-proof-equivalence-audit-20261007 (efbc3413894cfaf28bae579404490f1aaede4817). Historical pinned solver runs prove the two pure production admission functions only; no new solver run or state-machine proof claimed.
- Actors PR268 signed6775f1a8774d8f915dd1a1367058b697291da936 now packages maintained native loaders, requires complete WASM assets, uses locked native Cargo builds, and recognizes exact Mac Bun importer provenance. Actual fresh Windows/macOS parent+companion archives pass installed Node/Bun native/fallback/retry tests and all eight TLS RPCs, bearer rejection, server-observed cancellation and wide-u64 serialization. Installed readonly/nominal negative TypeScript compilation passes. Remote draft404134409 transport archive SHA2561198ad0739b5f6b347bad4d26563e67272e3bab04af2f0f8f687f7ecbda09da4; Linux remains outstanding. Product Rust unchanged from qualified79 library/12 descriptor tests.
- Root discovered retained Actors cross-family grpc/browser qualification scripts still import removed Actors HTTP/gRPC facades/routes. These need migration and genuine rendered browser execution before final merge; fresh native transport evidence does not cover them.
- Harness PR272 head5b284bce52f69f63b0e09ca21603aff636113679 packageD5E5 is correctly preserved remotely but independent review blocks merge: currentmain5d2336 modifies Harness and qualification, and runtime provider identity admission must snapshot mutable input before the async Rust initialization boundary. Owner assigned fixes, fresh package and tests; no merge claim.
- Docs collector fixes signed45c6a267272904f3f4f1dd3fbdebbd1ec3e902b3 are remote. Old aggregate run7 manifest incorrectly included profile scratch builds, skipped executable scenarios, and does not qualify generated snippets. Updated generation/drift qualification remains active.
- Stream PR270 needs genuine Linux NAPI installed TLS/cancellation evidence; a mistakenly produced Linux Filesystem FUSE receipt is explicitly insufficient for this gate. No foreign WSL processes killed or worktree refs reset.
- Goal remains active/incomplete. Current runtime supports root plus three useful workers; saturate those slots and retain queued language/source-authority work. No registry publication, production deployment or website merge.

- Docs run8 now completed: 19 emitted scenarios, 553 source inputs, 126 published artifacts, no profile scratch builds, 18 rustdoc profiles/sidecars. Positive/restored drift passes; edited source and generated artifact each exit1. Source hash11fb1e9afbb919f10f1d55d5025289dfeacd539c1ebb752f176cf5f881db1242. Executable language-snippet qualification remains separate and incomplete.
