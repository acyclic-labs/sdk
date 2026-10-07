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
