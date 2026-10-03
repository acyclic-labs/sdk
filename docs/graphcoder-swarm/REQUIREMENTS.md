# Locked acceptance matrix: Harness-first coding swarm

All rows are required. PENDING is not a pass. Model scripts are mocks;
filesystem operations, subprocesses, journals and terminal interaction must be real.

| ID | Contract | Verification | Status |
|---|---|---|---|
| INPUT-01 | Canonical ordered requests and content manifests | model_input manifest tests | TESTED, dispatch integration pending |
| INPUT-02 | Complete tool exchanges at fork boundary | fork_requires_complete_matching_tool_exchange | TESTED |
| INPUT-03 | Reject aggregate overflow without truncation | aggregate_limit_does_not_silently_truncate | TESTED |
| INPUT-04 | No hidden retrieval, summaries or UI/sibling input | provider-capture E2E | PENDING |
| INPUT-05 | Validate tool arguments/results and referenced content | schema/corruption E2E | PENDING |
| FORK-01 | Byte-perfect inherited model prefix | prefix mutation tests | TESTED, runtime integration pending |
| FORK-02 | Recursive pinned prefixes survive restart | recursive_prefix_survives_persistence_with_suffix_only_context | TESTED, full host restart pending |
| FORK-03 | Fork notification/task/new scratch are suffix only | production fork E2E | PENDING |
| FORK-04 | Shared completed boundary for parallel fork batch | concurrent fork E2E | PENDING |
| FORK-05 | Parent changes do not change child prefix | pinned runtime E2E | PENDING |
| FORK-06 | No cloned processes or unresolved effects | fork admission negatives | PENDING |
| WORK-01 | Real isolated root/child/grandchild workspaces | recursive swarm E2E | PENDING |
| WORK-02 | Direct-parent merge/discard, one level upward | facade authorization E2E | PENDING |
| WORK-03 | Conflict continuation/abort/rebase and discard | conflict E2E | PENDING |
| WORK-04 | Checkout untouched before exact root approval | writeback E2E | PENDING |
| WORK-05 | Preserve concurrent user edits | reconciliation E2E | PENDING |
| COMMS-01 | Durable scoped delivery identities and order | message recovery E2E | PENDING |
| COMMS-02 | Durable waits, deadlines and cancellation | wait recovery E2E | PENDING |
| LIMIT-01 | Active/total/depth/step/output/time budgets | swarm limit tests | PENDING |
| EFFECT-01 | Admission before dispatch, observed outcome before success | journal fault E2E | PENDING |
| EFFECT-02 | Stable identities and safe retry guarantees | effect recovery E2E | PENDING |
| EFFECT-03 | Unknown command outcomes never rerun automatically | subprocess crash E2E | PENDING |
| EFFECT-04 | Exact host command approval, explicit environment | process policy E2E | PENDING |
| EFFECT-05 | Stale writers/generations cannot mutate | fencing E2E | PENDING |
| HOST-01 | Durable local composition, reopen/resume | host lifecycle E2E | PENDING |
| CLI-01 | Interactive and headless public interface | PTY/installed artifact E2E | PENDING |
| CLI-02 | Input/tree/activity/messages/approval/cancel/diff | terminal scenario E2E | PENDING |
| LOAD-01 | Lazy pages/content/diffs and no eager worker startup | instrumented load tests | PENDING |
| BIND-01 | Rust-owned public bindings, native/WASM agreement | generation and parity gates | PENDING |
| QUAL-01 | SDK conformance, regressions and package consumers | qualification lanes | PENDING |
| QUAL-02 | Source/suite/descriptor/artifact evidence identity | qualification receipt | PENDING |

## Baseline
Existing Harness library suite: 176 passed before dispatch-path changes.
After canonical admission integration: 181 passed.
Filesystems, installed artifacts and terminal are not qualified by those results.
