# Locked acceptance matrix: Harness-first coding swarm

All rows are required. PENDING is not a pass. Model scripts are mocks;
filesystem operations, subprocesses, journals and terminal interaction must be real.

| ID | Contract | Verification | Status |
|---|---|---|---|
| INPUT-01 | Canonical ordered requests and content manifests | model_input manifest tests | TESTED, actual provider capture matches persisted manifest |
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
| HOST-01 | Durable local composition, reopen/resume | reopen_recovers_completed_turn_without_dispatch | TESTED single-agent lifecycle; swarm resume pending |
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

## Publication recovery subcontracts

These refine EFFECT-01/EFFECT-02 and FORK-04; the parent rows remain pending
until concrete recursive fork activation and fault scenarios pass.

| ID | Contract | Verification | Evidence/status |
|---|---|---|---|
| EFFECT-01-A | Persist completed-batch publication admission before dispatch and result before next request | completed_batch_publication_blocks_next_request_until_reconciled | checkpoint-publication.json; TESTED stock executor |
| EFFECT-02-A | Reconcile original admission; repeat dispatch only for idempotent guarantee | batch_publication_recovery_preserves_admission_and_retry_guarantee | checkpoint-publication.json; TESTED three guarantees |
| EFFECT-02-B | Reject altered completed boundary on replay without dispatch | batch_publication_recovery_preserves_admission_and_retry_guarantee | checkpoint-publication.json; TESTED |
| FORK-04-A | Typed fork adapter activates children only after the shared completed boundary | concrete swarm fault E2E | PENDING |

## Authoritative exchange subcontracts

These supplement the original required rows; they do not qualify the full swarm.

| ID | Contract | Verification | Evidence/status |
|---|---|---|---|
| HISTORY-01 | Store completed text, calls, successful results and rejection feedback in exact order | native_forks_capture_completed_authoritative_exchange_and_exact_model_prefix | checkpoint-history.json; TESTED native durable storage |
| HISTORY-02 | Restore canonical text bytes and original text message representation | pinned_model_text_preserves_bytes_and_rejects_overflow_corruption_and_reinterpretation | checkpoint-history.json; TESTED |
| HISTORY-03 | Retain intermediate assistant text once across follow-up turns | native_forks_capture_completed_authoritative_exchange_and_exact_model_prefix | checkpoint-history.json; TESTED |
| FORK-01-A | Two actual sibling workspace forks receive the same completed model prefix bytes | native_forks_capture_completed_authoritative_exchange_and_exact_model_prefix | checkpoint-history.json; TESTED fixture through production executor and SDK fork providers |
| EFFECT-05-A | Refuse stale completed conversation before writing publication artifacts | stale_completed_boundary_is_refused_before_publication_files_are_written | checkpoint-history.json; TESTED native generation unchanged |
