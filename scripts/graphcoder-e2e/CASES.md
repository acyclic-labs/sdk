Cases launch the shipping GraphCoder binary with an ordinary provider catalog and isolated settings. Build GraphCoder and harness-mock-model and install scripts/graphcoder-pty dependencies before timing. Run `node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL [CASE ...]`.

`topology-blackboard` selects the embedded role file through native `spawn`.
Its authored SDK conversation waits for one child to publish a session-shared
board, then a sibling reads and extends that original publication. Two edits
use the same original version: the second must report a conflict. The
coordinator reads the unchanged contributions after both producers finish.
A second GraphCoder process reopens the same session and delegates a reader
that reads the retained board through its own admitted role.
`node scripts/qualify-topology-presets.mjs GRAPHCODER MOCK_MODEL FRESH_ABSOLUTE_OUTPUT`
checks the same conversation with a generic worker, private sibling volumes,
and an omitted edit version. Each must fail the unchanged assertions before
the original role, sharing and version condition pass together.
`topology-hierarchy` joins an original grandchild result into an independent
reviewer's task and returns that reviewer's actual terminal answer. The same
qualifier rejects a generic lead, a substituted leaf and a substituted review
before the original contributions pass together.
The qualifier retains its modified `cases/` and `coverage.json` input under
the fresh output directory. `E2E_CASES_ROOT` selects that tree in the ordinary
runner; checked-in inputs are read only, including if qualification is killed.

Search seeds `.codex/search.json` under the selected settings root (`CODEX_HOME/search.json` otherwise). Fields are `provider` (`exa` or `tinyfish`), `api_keys` (provider id to key), and optional `endpoint`. The case points the ordinary HTTP adapter at the shared mock namespace; there is no search CLI flag or alternate executor.

`pty-search-cancel-resume` waits for the acknowledged configured search HTTP request, cancels its held body with Escape and reopens the retained cancellation without redispatch. `node scripts/qualify-web-lifecycle.mjs /absolute/graphcoder` separately follows an actual search result into `url_fetch`, checks exact UTF-8 bytes and source URLs, completes with a citation and replays with provider/search/fetch dispatch disabled.

`node scripts/qualify-fetch-cancellation.mjs /absolute/graphcoder` holds a real GET response body, cancels it through Escape, observes the HTTP exchange retire and explicitly resumes the stopped task with its prompt and cancellation notice retained. Search and fetch cancellation both require the idle STOPPED view and exit 2, with no additional provider or tool request. `scripts/qualify-web-cancellation-negative.sh` disables the production executor's cancellation contract; both shipping cancellation tests must fail, then pass after restoration.

`scripts/qualify-graphcoder-lane.sh` runs focused proofs and the full parallel suite. `E2E_WALL` includes seeding, assertions and cleanup, excluding builds and dependency installation. A red matrix remains a failure even below ten seconds. Cargo's target directory defaults to `target` when `CARGO_TARGET_DIR` is unset.

`pty-diff-folder-move` and `pty-diff-folder-replace` keep the live diff open during native shell changes, then explicitly resume and verify the same descendant removals and additions without redispatch.

`pty-diff-large-restoration` restores the interior of a 1 MiB file while a small added file remains visible, then verifies the retained diff after explicit resume. The filesystem content-equality regression separately forces distinct range-write payload layouts under the unchanged item and byte budgets.
