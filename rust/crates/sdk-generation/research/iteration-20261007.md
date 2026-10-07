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
