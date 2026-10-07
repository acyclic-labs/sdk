# 2026-10-07 qualification iteration

Main remains 90d2aff6cb870245c8db320809d711e7e74c2004 (PR251). No main merge in this iteration.

PR254 head 1cdebbfcb3c4c19217a0c1b18ad3e9c96d1b562f has passing plan, policy, gate, record, SDK Qualification and Greptile. Full nested context validation preserves the output context revision. Actual wasm32 check passed with this behavior; subsequent changes were formatting only.

Local pinned Rust 1.98.1 evidence:
- Harness: 184/184 tests after identity-aware result/event/cancellation routing and fail-closed legacy defaults.
- Objects: 26/26 tests after validated metadata/object-info/range wrappers.
- Stream NAPI: 5/5 tests after serialized reads and asynchronous close; awaited close releases the transport cursor.
- SDKdocs: 24/24 tests before the forthcoming generated-source provenance API change, including inherited methods/associated members, alias links and explicit re-export precedence.
- SDKgeneration binary unit tests: 4/4.
- Staging: 8/8 Node tests, including actual junction rejection.
- Native builder: 6/6 Node tests; wrapper: 7/7 Bun tests. No native rebuild claimed by these tests.
- Existing ten-harness Kani receipt: all 17 source input hashes and accepted log hash remain current.

Clean qualification checkout beca729ca71803c30cb90886e27a77ec2e596020 built real Rustdoc JSON and dep-info lanes for all eleven published crate owners. Generation failed after those builds because generated Cargo OUT_DIR source spans are outside the repository. No completed bundle or successful drift check is claimed. The partial output is retained. The next fix is exact paired generated-source provenance and truthful bundled source paths, preserving public item coverage and rejection of arbitrary external spans.

The first retry rejected a reused partial output directory; the subsequent fresh output reached the provenance failure above. Later retries must use a new output and the same warm caches, after a clean signed source snapshot.

Next milestones: verify generated-source provenance and full clean generation/drift, qualify installed Actors TypeScript against that bundle, finish release-only full qualification wiring, update PR252 and restack PR254, obtain required review and merge dependency-complete PRs normally. No registry publishing, production deployment, auto-merge or website merge.

## Generation provenance and qualification follow-up

Root verified Actors 66/66, Harness 186/186, Objects 28/28, and qualification planner 19/19. Ten Kani harnesses were rerun after the TypeScript exporter-only change; current receipt source closure and accepted log hash were independently verified. SDKdocs generated-source projection tests passed 25/25. Stream declarations were regenerated through maintained N-API tooling and now expose asynchronous close.

Generated-source provenance now maps actual JSON-lane compiler outputs to stable bundled paths, checks paired compiler lanes, hashes before/after projection, and keeps drift side-effect free. Independent review added comparison of current generated logical paths/hashes to existing bundle artifacts. Full generation remains unqualified: the release fixture found relative dep-info source paths incorrectly resolved against the cache rather than compiler checkout cwd; owner is repairing this concrete failure. Preview fixture passed. No completed all-eleven-family bundle or installed TypeScript qualification is claimed.

Release publisher review fixed the exact caller job-name predicate. PR252 remains review-required; PR254 checks pass. No coordinator merge has occurred in this loop. All pending work remains active; website consumption and publishing are outside this loop.

Standalone SDKdocs recheck passed 25/25; docs-only three-file update pushed to PR252 head 61b3d50bc59c5e4299d3ef0d93361e3460a1a5c2. PR254 restacked without force to 3628d7b0b3b0c3a35dbe19b56f75bc154965cb28. No main merge. Objects latest multipart cohort passes 31/31; Stream lifecycle 5/5; staging8/8; WSL preflight passes.

Release fixture handle85390 remains live, compiling its fresh private Rustdoc cache. Full clean generator build26984 terminated because Windows held the SDKgen executable open for that fixture. Direct use of the existing binary correctly rejected the clean snapshot's differing compiled-source digest; it produced no valid bundle. Wait for that exact live fixture before rebuilding the generator from clean qualification snapshot103006. Do not replace or restart live work based on quiet output.
## Current local qualification results

Objects typed request cohort passes 34/34 Rust library tests. The clean all-eleven-owner generation at snapshot 103006a55886ff811151da91ca5cb137b1be18e1 built every Rustdoc JSON and dep-info lane, then failed public API projection for generic receiver T::commit_workspace_fork. Generic receiver support and an all-eleven-owner warm corpus regression check are being qualified; no complete generation bundle or drift pass is claimed.

Release fixture handle85390 terminated on a missing Workers README in its synthetic checkout. The fixture now includes that source and reuses a stable sibling cache; its recheck is pending. Generator build34330 is terminal, so neither prior build remains live.

Current PR254 b8beb24d89e5f69ce5510bcf658aa6d5e7ac6ade Linux qualification used native Bun1.3.14 and built Rust WASM successfully. Its TypeScript suite reached 17 passes and one failing high-level handle test (identity length differs in nested context validation). This is a qualification failure, not a current installed-package receipt. Diagnosis is in progress before changing fixture or behavior.

PR252 head61b3d50bc59c5e4299d3ef0d93361e3460a1a5c2 and PR254 remain open; no coordinator merge has occurred. The required-review gate is still present. Goal remains active.
## Main integration and current proofs

Remote main advanced to 2654baf2bfa8295109a00909a96b1778f858af03 through another task's PR255. Coordinator has not merged to main. PR252 was synchronized onto that main in signed head d2cc44136bbce1e3f7035475deededc6edd7c8ba; its textual conflict is resolved, preserving all five shared checksum-verified tool-install call sites. GitHub now reports mergeable/review-required and schedules the cheap core gate. A preflight test fixture needs a real fake-gitleaks tar archive after this integration; that fixture-only correction passes locally.

PR254 signed head e501424f618fa53cf8ad72ec72b232f17ce8ae3e reimplements the nested context fix in main's consolidated acyclic-inference crate. It preserves deletion of the duplicate inference-contract crate and existing WASM delegation. The current consolidated Rust library suite passes 15/15. Full installed-package requalification against this new head remains pending. Earlier exact head3374173 TypeScript18/18, offline package consumer, and extracted Rust8/8 completed, with package hashes and a clean source independently verified; the outer WSL wrapper lost its exit variable, so that wrapper's exit status is not a successful gate receipt.

Objects typed facade and responses pass36/36; all13 routes delegate to existing canonical providers and response validators, and the three boolean routes no longer expose generated wire responses. Stream NAPI passes6/6, including a real TCP/TLS pending-read close test which observes server-side stream drop.

Kani frozen-source rerun: eleven successfully verified pure-domain harnesses, zero failures. Root independently verifies all17 source input hashes and accepted log SHA256 9C14023472F8335BFEFBD6771E35CE07A97254E0D2E438E5AFD97741359FADE8. Accepted log is preserved in research/proofs/runner-validation-11-roundtrip-frozen.log. Receipt summary metadata is being corrected from the old literal ten-harness wording.

SDKgen fixture reaches generation but remains unqualified end-to-end: two generated-code Rustdoc JSON inputs changed when a cache was reused across fixture roots. The fixture now namespaces those caches; its fresh compilation then hit a Windows linker input-file failure, under investigation. No all-eleven-family generation/drift success is claimed.
Current accepted eleven-harness proof receipt now has dynamic 11/0/11 counters and matching policy metadata. Root rechecks every source hash and the final accepted log SHA256 57CF9B40CC7117DDFCACAD15D6B18F224B678C4EA59BEED536103142FA56D401; research/proofs/runner-validation-11-roundtrip-policy11.log preserves this final evidence. The earlier frozen log remains historical.

PR252 lateste870be9e48f6fcebeb5efcd18b2b9482aeca393b passes all cheap core CI jobs (plan/gate/policy/record/SDK Qualification), with Windows skipped. Review is still required; no coordinator main merge. PR254 latestbac48c3432038bed318103f5ebeeed62d3fe6e6c remains the three-file consolidated repair. Its local package retry handle19376 uses an explicit native Linux tool path after handle85247 terminated at missing wasm-bindgen resolution.

SDKdocs current local follow-up passes26/26, with one ignored corpus test; the new nonignored fixture verifies local array receiver links through full projection. Standalone maintained UniFFI0.31 adapter compiles with Rust-owned nominal constructors and connect/inspect cancellation. Native Filesystem strict numeric admission is implemented across all u32 inputs; its first compilation found denied low-level unsafe hooks, now narrowed to documented maintained N-API conversion hooks and awaiting recheck.

## Completed package and binding gates

Native Linux installed Inference qualification for bac48c3432038bed318103f5ebeeed62d3fe6e6c completed with exit zero (handle19376). WASM release build, TypeScript19/19, isolated npm-tarball installation and validator smoke test, Rust crate packaging, and extracted crate15/15 passed. SOURCE_COMMIT matches the exact revision; two independent reviewers verified both SHA256SUMS entries and the qualification checkout remained clean. Artifacts and log are under Q:/sdk/work/inference-installed-bac48c3-linux-nativepath-20261007. Windows Bun path coverage remains separate.

Filesystem NAPI Rust tests passed12/12 after narrowing the unsafe conversion hooks (handle74779). Actual installed JavaScript boundary admission and generated declaration checks are next; Rust tests alone do not qualify that boundary.

Remote checkpoint push completed: codex/rust-foundation-checkpoint-20261006 points to 0fb47f3d0e6476ce97be07959393ef69663b67f3. Subsequent changes require a new checkpoint. No coordinator main merge has occurred. PR252 still needs an approving review and thread resolution; its latest three source-collision/read-consistency/version-path findings are being checked before merge. PR254 package qualification is complete but remains stacked on PR252.

Actual Windows Filesystem N-API qualification now passes through the built current DLL: invalid finite/integer/range classes reject in planExtents, open.maximumEntries, and createVolume.limits.maximumPathBytes; u32::MAX is accepted by open.maximumEntries. The generated declaration checks retain number. This follows Rust12/12, rather than using Rust-only tests as a proxy for JS conversion.

The standalone Actors adapter builds with maintained UniFFI0.31 and generates Kotlin, Swift, and Python outputs in Q:/sdk/work/actors-uniffi-current-prototype. It exports the canonical private-CA connector with explicit cancellation. Native DLL SHA256 F04A0403781A67916B941307E67BC660C5CCDE0B6098F3F4ED6C960DC88FA77A; adapter source SHA256 A7E0399E381BA2A2B1D8D07587E81CF2C6245FAC3E04277E04331179DC2BC8CF. Installed foreign-language runtime qualification is pending; this is an inspect-only slice, not a complete Actors SDK.

The shared Actors TLS/HTTP fixture refactor passes existing conformance: Node38, Bun38, Rust15 authenticated methods, including denied auth and Rust response-size bounds. New language probes reuse that sole fixture rather than implementing a second protocol server.

Full SDKgen fixture59685 is terminal after approximately920 seconds: generation completed all eleven old-baseline owners, but the final restored-output drift assertion failed. Ten rustdoc JSON artifacts matched; Workers alone contains unstable OUT_DIR span filenames and path-prefix/separator variations despite identical generated source bytes. Replay1090 confirms the cause. Persisted rustdoc source identities must be normalized without weakening source attestation or drift checking; reproducible all-owner generation remains incomplete. Current-main integration separately has ten owners because the duplicate Inference contract crate was removed.

Metadata qualification now passes after replacing the obsolete global Python source/word ban with a precise ban on the historically removed plugin/packaging/pypi implementation. Proof tooling, research, and future maintained Python bindings remain permitted. No supported Python package is claimed by that inventory repair.

## Inference artifact reuse boundary for clean candidate 9bc

The clean qualification candidate was `9bc668a053480a916d7e6e3972a80e2a5735ab1e`. An external bounded input-closure comparison against the previously qualified Inference source `bac48c3432038bed318103f5ebeeed62d3fe6e6c` covered 71 inputs: workspace manifests from `cargo metadata --no-deps`, the transitive `ensure-rust-target.sh` build helper, Rust crates and descriptors, protobuf and generated contract inputs, TypeScript sources and metadata, lockfiles, toolchain manifests, and package gate scripts. Generated `dist`, WASM, `node_modules`, and TypeScript build-info outputs were excluded as gate outputs; unrelated docs and workflows were outside scope.

The comparison result is `Q:/sdk/work/inference-package-input-equivalence-bac48c3-expanded.json`, with `equivalent=true`, 71 reference files, 71 candidate files, and zero differences. Comparator SHA-256 is `A9B8FB1AB6C8B07CC5E9592B42E9DED3D06113CD88C411627F93F6F8D7F39447`; result SHA-256 is `D4942DD61E41E63610C30EC9CC5FBFE83806B76AEA5C98E8649C9D8A8FC4C4A2`.

This establishes bounded source-input equivalence sufficient to inherit the existing `bac48c3` package artifact evidence for unchanged inputs. It does not retag the artifact: its `SOURCE_COMMIT` remains `bac48c3432038bed318103f5ebeeed62d3fe6e6c`, and the comparison is not an installed-package qualification receipt for `9bc668a053480a916d7e6e3972a80e2a5735ab1e`. Any change outside this closure, a workspace-membership change, or a toolchain/registry/environment difference requires a fresh exact-head gate.
