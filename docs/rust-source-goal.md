# Rust SDK and documentation source goal

Status: active. This ledger is specific to the Rust-source migration, not the historical SDK_CONTRACT_GOAL.md.

## Boundaries

- SDK branch: `codex/rust-sdk-docs-source`, isolated worktree based on `5beef008f88cd51a434260b36b79776bb95e605f`.
- Website branch: `codex/rust-sdk-docs-source`, isolated worktree based on `68fea323412df5461b9c54475d97b2a48ca9c14e`.
- Never merge to main, enable auto-merge, publish registry artifacts, or deploy production in this loop.
- Keep available subagent slots occupied with explicitly owned independent work; replenish finished tasks.
- Preserve existing protocol identities, descriptor handshake digests, archived contracts, release claims, and user changes.

## Source ownership

Rust owns behavior and authored contract metadata, including wire field numbers, presence, oneofs, enum numbers, service/RPC identities, HTTP routes, validation, capabilities, and errors. Generated Protobuf, OpenAPI, language SDKs, docs bundles, and snippets are projections. Thin UI adapters and target-runtime plumbing may remain in their native language but must not independently author shared contracts or semantics.

Crate-owned Markdown included by rustdoc is allowed for guides. Website SDK content must consume the generated source-bound bundle. References, previews, and release availability remain explicit.

## Milestones

1. Research pinned OSS tools and execute bounded Actors, Stream, and embedded prototypes.
2. Select the minimal qualifying toolchain based on actual evidence; document gaps that justify custom Rust metadata tooling.
3. Establish Rust-owned contract export, compatibility verification, one generation command, and docs/scenario bundle.
4. Qualify installable generated packages and migrate existing shared TypeScript behavior incrementally.
5. Replace website SDK content with generated content, preserve routes, and verify rendered previews.
6. Expand every viable OSS language target, record evidence, and run clean reproducibility/conformance/package checks.

## Completion rule

Do not mark this goal complete while a viable target, migrated API, shared TypeScript implementation, independent SDK website content, or required compatibility/installation/snippet/preview check remains outstanding. Prototype or smoke results are not full language qualification.

## Iteration 1 — 2026-10-03

- Created and isolated both worktrees; activated the goal loop.
- Dispatched all 16 subagents to independent research/audit tasks, then replenished completed slots with owned prototype implementations.
- Research identifies a necessary Rust contract-authoring layer: existing OSS type/OpenAPI tools do not encode the complete Protobuf tags/options/services/streaming model.
- Cloudflare Forge is a downstream OpenAPI candidate. Its actual implementation and transitive generator licensing must be verified; roadmap targets are not qualification evidence.
- Current implementation work is deliberately additive until protocol and generated-package gates pass. Legacy sources are not removed prematurely.

## Iteration 2 — prototype integration and hard gates

- Existing Actors, Workers, and Stream Rust baseline: 52 tests passed (one pre-existing ignored Stream test).
- Actors contract now emits protobuf and source-info-free descriptors directly from Rust. Golden/protoc equivalence, tag mutation, and output drift tests passed. Existing runtime/handshake descriptor bytes remain unchanged; normalized descriptor hashes are not interchangeable with deployed digests.
- Actors OpenAPI consumes the Rust model, validates with pinned OpenAPI Generator 7.25.0, and has six passing tests including HTTP loopback error/uint64/oneof coverage. Two normalized Python wheels matched SHA-256. Operation descriptions and model-owned routes remain integration work.
- Resolved rustdoc JSON generated using pinned Rust 1.98.1 in a docs-only experimental invocation. Strict bundle generation covers 19 public crate families; default-host empty graphs and feature/target completeness remain explicit gates.
- Python transport unary/stream/cancellation and JVM transport plus independent installed consumer tests passed. These are bounded transport prototypes, not complete family/language qualification. Packages must consume Rust-emitted schemas before migration.
- Embedded MemoryStream C ABI exists; handle lifetime, buffer ownership, foreign consumers, and ABI layout remain under independent review and implementation.
- Central runner review identified fail-open risks: missing regenerated files, fake qualification hashes, and incomplete language inventory. Owners are fixing these before integration acceptance.
- TypeScript generated metadata still requires Rust model migration and parity checks. Legacy shared TypeScript and website SDK content are still present and therefore the goal is incomplete.
- All 16 subagent slots replenished with implementation, provenance, extra-language, compatibility, and independent review work. No merges, registry publishing, or production deployment.

Next bounded milestones: wire Stream export; strict bundle website preview; exact artifact-set drift checks; Rust-schema package consumers; safe embedded foreign consumers; reproducible package evidence; source-bound executable snippets.

## Iteration 3 — source authority, evidence, and rendered review

- Rendered Actors preview checked at 1280x900 and 390x844. It has correct unqualified diagnostics and no measured horizontal overflow, but missing summaries, mangled scanner names, duplicate names, and breadcrumb/navigation gaps mean it is not accepted as pristine docs. Resolved graph projection and grouping are in progress.
- Added website route backlog: 51 page patterns, 35 legacy content modules. Dynamic route expansions still need enumeration. Existing dev-only production 404 guard retained.
- C++ foreign consumer built/linked actual Rust release library using clang/CMake/Ninja and passed first ABI smoke. Broader stale-handle, release, close-order, cancellation/concurrency and layout checks remain pending.
- Independent review reproduced fake qualification evidence acceptance, empty-inventory success risk, missing regenerated-artifact bypass, untracked-source provenance omission, and empty-descriptor compatibility success. Owners are implementing fail-closed regression gates; these outputs must not be promoted until retested.
- Rust model work now split into Actors, Stream, and Workers with exclusive module ownership. Package lanes are changing canonical-proto bootstrap inputs to explicit Rust-emitted schema inputs.
- Compact website projection is moving from its initial TypeScript helper into the Rust docs command, consistent with removal of TypeScript generation authority.
- Added ignored language-local portable-toolchain paths; downloaded runtimes are local qualification tools, not committed artifacts or source hash inputs.
- Goal remains active. All 16 subagent slots occupied. No main merges, registry publication, or production deployment.

## Iteration 4 — broader Rust models and fresh evidence

- Refilled all 16 agent slots with owned model, exporter, SDK, embedded, documentation, website and package tasks. Objects v2 routes and descriptor/presence/map/streaming tests are now in the Rust model; Workers, Filesystem/Harness, Inference and Machines integration is continuing.
- Independently rebuilt the compatibility validator and reran negative gates. Empty descriptors, duplicate identities, unknown descriptor fields, reordered repeated options and untracked-source identity tests passed. The identified local generation binary still accepted fabricated qualification evidence; this receipt is failed and supersedes the earlier passed receipt. Source fixes and binary freshness remain under active integration.
- Negative-gate receipts now identify the exact executed validator, generation binary and harness hashes. Temporary cleanup verifies resolved containment before deletion.
- Further qualification review found syntactically valid test hashes do not establish actual executed test receipts, and source/artifact manifest bindings need verification against disk. Those are assigned integration gates, not accepted evidence.
- Rustdoc profiles, true target-specific graphs and source-bound graph freshness remain work. The Stream website quickstart requires semantic Markdown rendering and a dedicated Rust-owned guide before it can replace the existing content faithfully.
- No merge to main, auto-merge, registry publication or production deployment. The goal remains active and broad language, behavior, documentation and removal requirements remain outstanding.

Next bounded milestones: integrate all family models without changing deployed descriptor bytes; fix source-bound qualification receipts; compile the unified generation stages; exercise multilingual clients against a bounded canonical Rust fixture server; render and verify generated guide/reference pages; produce isolated reviewable checkpoints after checks pass.

Fresh bounded verification: current protocol validator suite passed all 22 tests; hash-identified receipt at research/acceptance/protocol-validator.receipt.json. This does not qualify model integration or language packages.

Research evidence: pinned Utoipa/Schemars/Specta/Typeshare emitters executed in research/metadata/rust-metadata-prototypes (2 tests passed). A research agent initially wrote these new files to the original checkout; only its verified authored files were moved to the isolated worktree, hashes matched, and targeted original-checkout status is clean. No original-checkout commits were made. Full outputs and limitations are in docs/research/rust-metadata-prototypes.md.

Fresh verification updates: explicitly identified generation binary bec0d712... passed strengthened negative harness with valid initial inventory; 3 current ABI cancellation tests passed. Stream preview bundle c0b414... passes desktop/mobile wrapping and copyable code checks, but dropped paragraph tail and H1-to-H3 hierarchy remain failed visual/content gates. research/acceptance receipts record bounded scope; no broad qualification.

## Iteration 5 — isolated integration and broader consumers

- Recorded pinned OSS research in reviewable commit fc5ae608. Implementation remains under active integration in isolated SDK and website worktrees; no merge, publication or deployment.
- All 16 agent slots remain occupied. Refilled completed research, model and review tasks with full-family generation, package installation, source-bound conformance and documentation migration tasks.
- Fresh bounded embedded evidence includes 13 C ABI tests with UniFFI, 2 Filesystem/Harness binding tests, C/Python consumers, generated UniFFI polling consumer and a WASM Node consumer. Native N-API and complete package staging remain pending; disk-related build failures are being addressed using owned reusable caches.
- Python/Go, JVM/.NET, Dart, Scala and TypeScript lanes have executed bounded package or fixture checks. They are not broad language qualification; complete family/capability matrices, streaming recovery and reproducible source-bound receipts remain required.
- Root independently verified the latest Stream quickstart bundle d94f3a7c... at 1280x900 and 390x844: no horizontal overflow, one H1, consecutive H2 sections, paragraph tail preserved and code text intact. Exact StreamProvider link destination exists. Registry-looking install instructions were replaced with explicit source-workspace instructions. Reference still uses visibly unqualified fallback analysis; strict compiler graph coverage remains outstanding.
- Shared wire changes reported from an incorrect checkout are being reconciled by their owner into the actual isolated source, preserving independently owned Protocol and family modules. Integration is not accepted until tests run from that exact source.
- Qualification workflow scaffolding currently has incomplete platform execution and profile generation; assigned correction before acceptance. Product Rust build-script contract authority is under independent audit.

Next bounded milestones: complete exact-source wire exports and drift tests; execute the unified generation entrypoint; stage local dependencies into installable packages; produce compiler-qualified docs profiles; migrate remaining SDK topics and snippets; checkpoint verified implementation and run clean-checkout generation.

## Iteration 6 — compiler graphs and full-family bindings

- All nine Rust contract families now pass the owners' locked compatibility suite; root independent rerun is pending current product-binding edits. Raw custom options, archived handshake identities, map documentation and signed integer semantics are preserved.
- Python, Go, JVM, .NET, Ruby and Dart have generated bindings for all nine families. Installed clients have executed bounded Actors/Stream fixtures; these results do not qualify unexecuted families or recovery capabilities. JVM Harness accessor adaptation preserves its wire identity and adds reflection/serialization checks.
- Compiler-derived rustdoc evidence now includes 24 source-bound graphs across four complete profiles and 19 package families, with 5,835 source links audited. Website import and multilingual example integration remain active work.
- Native N-API, C, Python ctypes, generated UniFFI Python and WASM Node consumers passed against staged Rust libraries. Full distribution and remaining shared TypeScript behavior replacement remain outstanding.
- Root browser verified guide-to-reference navigation and reference search (StreamProvider filters 146 symbols to one). The preview still displayed the older fallback bundle during this check; it is not accepted as the final pristine reference.
- Ended a source-write pause that exceeded its bounded capture window. Clean generation is being verified using an isolated captured checkout while all 16 agents continue useful implementation. Live-source churn must fail closed rather than produce mixed artifacts.
- No merge to main, auto-merge, registry publishing or production deployment. Goal remains active.

Next bounded milestones: make product Rust builds consume model descriptors by default; replace qualified shared TS algorithms with Rust boundaries; render and execute expanded language scenarios; import strict docs projection and test language switching; validate portable source-bound artifact receipts; record an implementation checkpoint and run clean-checkout generation.

Fresh root integration evidence for iteration 6: full sdk-contract-wire suite passed 66 tests across 15 suites and formatting passed. A focused options/validator checkpoint is cc5e2eee9374bd83a75dfa0690f2824cfbd7e30d. Clean verification snapshot 6365b7b5 from that origin generated 95 artifacts and passed drift; its docs command still permits fallback, compatibility stage checks Actors only, and product artifact commands require integration. These are recorded outstanding gates, not full qualification. The website now suppresses mismatched and unexecuted example projections; mobile quickstart/reference widths pass after wrapping fixes. Current compiler-only graphs and expanded examples need a complete, matching strict bundle. Verified temporary Cargo caches are excluded from source snapshots. No merge or publishing.

## Iteration 7 — exact snippets and semantic contract documentation

- All 16 subagent slots are replenished with explicit implementation, package-consumer, source-identity and independent review ownership. The goal remains active.
- Protocol source generation now emits its wire fields from the Rust descriptor model and its comments from Rust-owned message/field descriptions. Root independently ran the focused Protocol suite: 5 passed, including immutable deployed dependency parity and authored-proto drift rejection. Family-specific negotiation policies are described without asserting uniform validation behavior.
- Filesystem/Harness owners supplied semantic documentation tables covering 175 messages and 686 fields; product code-generation overlays and pristine compiler references still require exact-source integration.
- Exact rendered Rust consumers and stronger package/source/runtime receipts are under integration. Scenario tests alone cannot qualify published snippets. The current Go Actors snippet failed real Rust validation because required limits were omitted; the failure is retained and its renderer is being corrected.
- Website checkpoint fc631c8 keeps unqualified language projections pending and rejects stale source bundles. Remaining route migration and Rust-generated navigation require verification and reviewable checkpoints.
- Java and Perl OpenAPI consumers passed bounded canonical bytes, decimal uint64 and error checks. Swift, C++ and other viable inventory targets remain outstanding; missing local dependencies are not final capability exclusions.
- Shared build storage is being coordinated using existing targets. Fresh strict compiler profile completeness, all-family compatibility stages, source/input versus generated-output identities, clean reproducibility and final removal requirements remain outstanding.
- No merge to main, auto-merge, registry publication or production deployment.
Fresh root verification: after correcting two outdated assertion scopes, the complete locked offline wire suite passed 72 tests (24 unit, 48 integration across 12 suites). Bounded receipt: research/acceptance/wire-suite-latest.receipt.json; log SHA-256 a790b0fc44523bc3988825fd5629ec2f3982e08b420c65a37c7d85254f422ce3. This does not qualify subsequent concurrent edits, clean final generation, package consumers or final website coverage.

## Iteration 8 — recovery and current acceptance gates

- Kept all 16 agent slots occupied, replenishing completed wire, package, website and receipt review tasks. The active goal remains incomplete.
- Checkpoint b94d31ff82bff6736adc89854bd622294e5de506 integrates model-owned wire bindings and product descriptor inputs. Later enum-value/oneof documentation and source-tracking changes remain under verification and focused checkpointing.
- A source audit found four Rust files consisting entirely of NUL bytes. Preserved the exact damaged bytes under Q:/sdk/work/rust-source-corruption-evidence. Recovered sdk-docs from a hash-verified isolated verification snapshot and three Inference/Machines build scripts from exact b94 Git blobs, using unchanged-hash guards. The cause is not established. Later documentation compiler and build-overlay edits still require owner reconciliation.
- Root independently ran the recovered sdk-docs locked offline suite: 12 tests passed. Its source hash and log are recorded in research/acceptance/docs-recovery.receipt.json. This qualifies the recovered compiler tests only, not full strict compiler graph generation.
- Root independently ran native PHP Protobuf/gRPC uint64 golden checks: all nine vectors passed. Broader current-source PHP package, snippet and recovery qualification remains in its owned lane.
- Receipt review reports 24 generation tests and production forgery gates passing. Fresh exact-source rendered snippets remain required; compiled source identity must not replace live source recomputation when checking mutations.
- The original SDK checkout has no tracked changes. Reviewable work remains in isolated SDK and website branches. No main merge, auto-merge, registry publication or production deployment.

Next bounded milestones: reconcile recovered source with later owner changes; reject corrupted authored inputs before snapshots; separate compiled and live example source identities; rebuild exact package consumers; produce all strict docs profiles from one captured revision; verify the refreshed website and record installable artifact instructions.
