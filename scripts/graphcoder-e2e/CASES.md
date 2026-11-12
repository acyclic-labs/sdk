Cases launch the shipping GraphCoder binary with an ordinary provider catalog and isolated settings. Build GraphCoder and harness-mock-model and install scripts/graphcoder-pty dependencies before timing. Run `node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL [CASE ...]`.

`session-attached-cancel-query` keeps one root running across detach/reattach,
opens History, then requires Ctrl-C to cancel that root without closing the
panel first. Its idle-query control closes History without cancelling a turn.

A workspace file may use `{ "source": "capture.mjs" }` to seed an asset from
this support directory. Programs executed by confined tools belong in the
workspace fixture; the controller's checkout grants no implicit read authority.

Executable assets use `{ "binary_env": "E2E_HOOK_BINARY" }`. Build the
`harness-hook-fixture` binary from `harness-mock-model` and set that variable to
its Cargo-reported executable before running hook cases. The loader copies its
bytes into the workspace with executable permissions; hook commands use that
workspace-relative copy. The shared Node test runner prepares this artifact for
the hook protocol test automatically.

`E2E_FILESYSTEM_BINARY` similarly selects `harness-filesystem-fixture`. Its
workspace-local copy reads a JSON operation plan for file writes, byte-range
fills, rename and removal, without requiring an interpreter.

`workspace.metadata` seeds root `mode` and `modified` (Unix seconds) from the
fixture owner before binding the workspace. The root-metadata case then creates
and removes a temporary file during the turn, changing the directory timestamp
through ordinary filesystem operations before its diff and merge assertions.

A `service` input owns the server separately from terminal clients. A later
`{ "mode": "service", "kill": true }` kills that owner and waits for its
native close event. A following `service` input restarts it with the original
owner arguments, so recovery cases preserve configuration across the restart.

`when.new_contains` lists alternative complete text groups. A group must appear
in the current committed frame and have been absent from the previous accepted
action. Paging cases use it to observe each patch without assuming path order.

Interactive sessions edit isolated volume forks. Cases that assert edited files
in the original checkout issue `/merge` and wait for its receipt before exiting.
Rendering and resumed-diff assertions precede that merge; the file assertions
continue to verify the original checkout, without an implicit harness merge.

`node scripts/qualify-topology-modes.mjs GRAPHCODER MOCK_MODEL FRESH_OUTPUT`
runs forced single and team cases plus a generated swarm with 257 siblings
and depth 65. Team workers settle and retire across independent root turns before the
third spawn is refused, proving a total reservation rather than an open-child
limit. The wire oracle checks absent worker/single spawn declarations and the
root's unchanged declarations after exhaustion. Each case also runs under a
different forced topology and must fail the same original assertions.

`child-completion-merge` writes through a worker's real shell, waits for its
completion, then verifies the file and retained parent content through the
parent's shell. Neither model calls merge. Its primary and physical variants
explicitly select native and physical mounting. `topology-owner-policy`
checks delegation stays disabled in a detached serving owner, rejects a
different topology on attachment before any model call, and resumes under the
original policy.

Completed child work is already published into its parent. `child-discard`
therefore checks that discarding the child's branch reference retains that
published work; discarding a reference does not reverse a completed merge.

`pty-tree-sidebar` creates a real worker, authors its private `ui/` row files,
toggles the automatically visible tree, fuzzy-matches its title, attaches,
and observes a live rename.
Parent and attachment-history navigation must return to the original views
without dispatching another model request. The shared Explorer's 10,000-child
control separately checks the row golden and request counts across a page and
sibling boundary, including an absent identity between pages.

`pty-code-highlight` streams a Rust fence, checks real cell foregrounds and a
code background band, retains the OSC 8 destination, and switches the same
settled answer through light and dark with `/theme`. Styling is asserted at the
settled frame, so lost highlighting fails directly. The shared cell oracle has
negative controls for unchanged text without syntax colour or background.

`tui-render-work` runs the native TUI library's input/delta paint pipeline at
80×50 and 120×40 with 100 and 10,000 retained blocks. Row visits and wrapped
text bytes must be equal for each history size. The same oracle rejects
archive scanning and historical rewrapping. PTY cases also report
`first_paint_ms`: elapsed time from the runner's terminal-launch request to the
first completed frame containing GraphCoder, including transport, terminal
negotiation and emulator decoding. This is an observation, never a latency
assertion or a claim that executor construction alone measures product startup.

`pty-conversation` submits its first prompt and terminal-query replies together on the first terminal output bytes, before admitting a rendered frame, then checks both conversational turns. `pty-cancel-preparation` holds the acknowledged token-count request, observes two distinct thinking highlights, then cancels before any model dispatch and requires the animation to stop. Resume cases use the public text/JSONL and explicit terminal-resume contracts.

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

`pty-audit-table` requires both source locations from an over-width audit cell
and the final guarantee column to remain visible through normal transcript wrapping.
