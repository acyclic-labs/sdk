Cases launch the shipping GraphCoder binary with an ordinary provider catalog and isolated settings. Build GraphCoder and harness-mock-model and install scripts/graphcoder-pty dependencies before timing. Run `node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL [CASE ...]`.

Search seeds `.codex/search.json` under the selected settings root (`CODEX_HOME/search.json` otherwise). Fields are `provider` (`exa` or `tinyfish`), `api_keys` (provider id to key), and optional `endpoint`. The case points the ordinary HTTP adapter at the shared mock namespace; there is no search CLI flag or alternate executor.

`scripts/qualify-graphcoder-lane.sh` runs focused proofs and the full parallel suite. `E2E_WALL` includes seeding, assertions and cleanup, excluding builds and dependency installation. A red matrix remains a failure even below ten seconds. Cargo's target directory defaults to `target` when `CARGO_TARGET_DIR` is unset.

`pty-diff-folder-move` and `pty-diff-folder-replace` keep the live diff open during native shell changes, then explicitly resume and verify the same descendant removals and additions without redispatch.

`pty-diff-large-restoration` restores the interior of a 1 MiB file while a small added file remains visible, then verifies the retained diff after explicit resume. The filesystem content-equality regression separately forces distinct range-write payload layouts under the unchanged item and byte budgets.
