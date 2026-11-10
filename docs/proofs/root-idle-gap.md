# Root wait evidence: contract-deep-read-d511

The complete source capture contains seven agents: one root and six direct
children. The reported 189.879-second stall is a benchmark reconstruction
artifact, not an observed root wait. All 93 model calls have been joined to
real proxy intervals; the longest interval between those calls anywhere in
the swarm is 13.509 seconds. Five children produced nonempty answers and
settled; the sixth ran seven model calls and failed during an edit. The
benchmark's `no_output_agents=6` does not measure either text output or actual
completion. No production wake fix is justified by these findings. The
private-file custody failure is separate from the reported idle gap.

Source directory:
`Q:/swarmbench-native/runs/20261109T215923Z-contract-deep-read-swarm-XS-d511/`.
Operation: `swarmbench-016e77796db04a31a7fd51ee83bcfa59`.
Root: `harness/agents/4bfa6c245d43432b9f2ea29505d61e66`.

## Committed timestamps

These times come from `Record.committed_at_micros` in the base64 SessionFrame
values in `graphcoder.jsonl`, not filesystem mtimes or reconstructed transcript
end times. Status deadlines come from the encoded StatusRecord value. Line
numbers are one-based.

| Event | UTC, 2026-11-09 | JSONL line |
| --- | --- | --- |
| Initial root idle status | 21:59:32.139 | 3 |
| Root waiting on base and amendments 01, 02, 03 | 22:00:13.976 | 446 |
| First subsequent committed record: wait result file creation | 22:01:11.665 | 450 |
| First wait tool_completed, step 12, execution sequence 104 | result above | 452 |
| Subsequent root fs tool result | 22:01:13.779 | 489 |
| Reviewer spawn tool result | 22:02:13.909 | 1202 |
| Root running status | 22:02:16.877 | 1236 |
| Root waiting on independent-review | 22:02:23.298 | 1277 |
| First subsequent committed record: reviewer wait result creation | 22:03:30.833 | 1281 |
| Reviewer wait tool_completed, step 33, execution sequence 277 | result above | 1283 |
| Amendment-02-fix spawn tool result | 22:03:38.785 | 1324 |
| Root running status | 22:03:43.273 | 1360 |
| Root waiting on amendment-02-fix | 22:03:45.156 | 1362 |

The completed waits explicitly name `timeout_ms: 200000`. Their deadlines are
22:03:24.062 and 22:05:36.884, respectively. Their results return all requested
children as settled/idle, well before either deadline. Therefore neither return
was caused by its fixed timeout. The two intervals with no committed root
records are 57.689 and 67.535 seconds. The root tools stream is not an activity
trace for every child.

Root model latency in `usage.jsonl` sums to 85.443662269 seconds. The native
adapter packs each agent's calls end to end starting at 21:59:32.034. The
root has the largest cumulative latency, so the final synthetic leaf ends
at 22:00:57.477. `autopm/forensics.py` then counts the tail through
22:04:07.356 as a 189.879-second global stall. This exactly reproduces
`analysis.json`, including millisecond truncation. It erases tools, child
spawn offsets and waits from the timeline; it does not establish inactivity.
The longest SDK model interval is 17.774505692 seconds, and the longest proxy
interval is 17.427 seconds.

The final waiting deadline is 22:07:03.279. The reconstructed root transcript
ends at 22:04:07.356 with the separately owned custody failure. Its waiting
status and tools watermark are the final observation boundary, not evidence of
a wait whose deadline fired. Reconstructed child `ended_at` values cannot
establish wall-clock ordering here: the reviewer is assigned 22:00:32.875,
which precedes its committed spawn result at 22:02:13.909.
In fact, all six child end times equal the same 21:59:32.034 origin plus
that child's sum of `usage.latency_ns`. For example, the reviewer's eight
model calls sum to 60.841298223 seconds. Those reconstructed times omit spawn
offsets and time outside model calls.

## Completion mechanism and attribution boundary

At both the reported revision e542fa51 and the investigated checkout, a deferred
tool wait becomes `Wait::Until`. The parent commits `Awaited` to each pending
child. The child's settlement commits `ChildSettled` to the parent's inbox;
the same actor transition contributes its activation. Consuming the last needed
completion calls `settle_awaited`, satisfies the condition, and resumes the turn
in that delivery. A child already idle when its registration arrives also sends
the completion. No status change or filesystem watermark is required.

The first events following the two observed root-record gaps are the wait
result creations listed above. The implementation resumes those calls on
completion delivery, and their results and deadlines agree with that path.
The source capture is complete as exported. Its child execution projections
do not export actor checkpoint or parent inbox commit timestamps. Therefore
the exact last-child settlement-to-delivery latency is unknown, rather than
190 seconds. The timing and output artifacts can nevertheless be diagnosed
from this capture; no different or eighteen-agent capture is required.

## Actual per-agent timeline

All times below are UTC on 2026-11-09. The 214 agent-tagged trace spans identify
seven agents, 93 model calls and 113 tools, but their timing is synthetic
(`detail.limits.timing`, `native_notes`).
[root-idle-gap-calls.csv](root-idle-gap-calls.csv) lists every model start/end
using `proxy_calls.jsonl`'s request-receipt `ts` plus `ms`, grouped by agent
and zero-based SDK step. It retains the proxy line, response ID and separate
SDK latency. These are proxy boundaries, not actor commit timestamps.

To reproduce the join, read `harbor-jobs/.../agent/transcripts/agents.jsonl`,
then concatenate each execution payload's base64 chunks by `(sequence, field)`
in offset order. For each usage `(agent_path, dispatch.step)`, find that
step's model `completed.metadata.id` and join it to proxy `request_id`.
Every one of the 93 joins is unique, consumes a different proxy row and agrees
on both input and output token counts. Do not use the null
`usage.provider_request_id` or nearest-time/token guesses. The `trace.json`
agent turn's `attrs.native_id` maps each native path to its trace agent ID.

Spawn times below are commits creating the spawn result file, immediately
before its execution `tool_completed` projection in `graphcoder.jsonl`.
They establish successful spawn publication, rather than the exact earlier
actor-creation instant. Child IDs are suffixes of the root's `/children/` path.

| Name / trace ID | Native child ID | Spawn result commit | Proxy first start / last end | Calls | Completion evidence |
| --- | --- | --- | --- | ---: | --- |
| root / root_021987bd0335 | root 4bfa6c245d43432b9f2ea29505d61e66 | initial status 21:59:32.139 | 21:59:32.322 / 22:03:41.528 | 36 | aborted at 22:04:07.356 |
| base / dlg_978199ee0f64 | e8bab483b1db18c68a2556cc4e849813 | 21:59:49.369 (line 202) | 21:59:53.776 / 22:00:43.167 | 10 | nonempty message seq 140; idle confirmed by first wait |
| amendment-01 / dlg_b72d27155c86 | 8a27f124ff0a62e0102b1d8ad5d7f617 | 21:59:53.450 (235) | 21:59:58.528 / 22:00:42.880 | 9 | nonempty message seq 168; idle confirmed by first wait |
| amendment-02 / dlg_7925378958d8 | 3a3d0043ef8b7b0a11274be4166ac359 | 21:59:58.273 (267) | 22:00:01.842 / 22:01:08.735 | 12 | nonempty message seq 171; idle confirmed by first wait |
| amendment-03 / dlg_6c1dda611306 | e851d57bfa790cd42659a05e366dd28a | 22:00:01.533 (299) | 22:00:04.732 / 22:00:59.932 | 11 | nonempty message seq 169; idle confirmed by first wait |
| independent-review / dlg_273a12a1a04d | a7fa2b44da467d074c7e11f1b5def96d | 22:02:13.909 (1202) | 22:02:17.683 / 22:03:17.843 | 8 | nonempty message seq 289; idle confirmed by second wait |
| amendment-02-fix / dlg_6642e696e59c | ff77767d1d3a10ea94ea1f5ead77d786 | 22:03:38.785 (1324) | 22:03:44.121 / 22:04:05.816 | 7 | no final answer or completion; last event seq 63 is edit `tool_started` |

The five nonempty `message_completed` payloads have respectively 279, 362,
253, 258 and 883 characters. These are model-message completions, not separate
actor settlement records. The parent wait results prove the first four
settlements occurred by 22:01:11.665 and the review settlement occurred by
22:03:30.833; each returns the actual answer and `phase: idle` with no running
children. Exact child settlement commit times are not exported. The final
child's provider `completed` at step 6 only completes a model call requesting
an edit; its `message_completed.text` is empty and is not a completed turn.

| Root wait | Model emitting wait finished at proxy | Waiting status commit | Return result commit | What happened |
| --- | --- | --- | --- | --- |
| step 12, execution 103/104: four documents | 22:00:13.519 | 22:00:13.976 | 22:01:11.665 | amendment-02's final proxy response ended 22:01:08.735; all four answers returned |
| step 33, execution 276/277: review | 22:02:22.879 | 22:02:23.298 | 22:03:30.833 | review final proxy response ended 22:03:17.843; its answer returned |
| step 35, model payload 289: amendment-02-fix | 22:03:41.528 | 22:03:45.156 | absent | child edit failed; process aborted 22:04:07.356, before deadline |

The root was waiting on working children in the first two intervals, and on
the subsequently failing edit child in the last. It was not parked for
190 seconds after a child completion. In the synthetic interval labelled
idle, the real run contains document work, two wait returns, root validation,
composition and pricing, an independent review, and a repair turn.

Do not interpret the review's 12.990 seconds between proxy EOF and root result
as wake latency. The last review call's SDK latency is 14.337766 seconds while
the proxy duration is 3.743 seconds; the SDK clock explicitly includes
original journal backpressure between streamed chunks and excludes the terminal
commit (`executor/model_usage.rs`, `dispatch.rs`). Proxy EOF is not completion
publication. The comparable final document call is 4.906540 seconds in the SDK
versus 2.559 seconds at the proxy. This capture cannot split the residual into
terminal commit, settlement, delivery and result-publication costs. It provides
no evidence of a late/lost completion wake. The longest gap across actual proxy
intervals is 22:03:17.843–22:03:31.352 (13.509 seconds), not a global inactivity
measurement; it also omits non-model work.

## Why all six children were labelled no-output

`autopm/agents.py:analyze` initializes missing `agent.turn.attrs.changed_paths`
to zero and treats absence of `workspace.publish` spans as no publication.
The GraphCoder adapter emits neither observation. This forces every child
defaulted to succeeded into `no_output_agents`, regardless of answers, file
writes or messages. It is not a test of whether children ran turns.

There are two additional adapter defects in `harness/native/graphcoder.py`:
`_messages` extracts tools but ignores model `message_completed` payloads, so
all child summaries are null; `parse` leaves every child's `NativeAgent.status`
at its default succeeded, including the edit child whose turn never settled.
The first four document agents wrote their assigned JSON files, ran validators
and sent messages; those files appear in the collected artifacts. The review
produced a substantive failure report that caused the repair spawn. The sixth
ran a turn but did not finish it. None of these facts means six completed empty
turns or six children that never ran.

`spawn_root_share=1.0` is accurate: all six children were spawned by the root,
as required by the workload. It says nothing about completion or wake latency.

## Sibling comparison

These are different host adapters, not five more GraphCoder executions. Their
native timing is not the GraphCoder synthetic model packing. The stall column
below is their existing leaf-span analysis, not a settlement-to-wake measure.
All five lack `changed_paths` and `workspace.publish` observations too.

| Suffix | Harness | Agents | Wall seconds | Reported longest stall seconds | Reported no-output children | Answer evidence |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| d511 | GraphCoder e542fa51 | 7 | 275.322 | 189.879, synthetic artifact | 6/6 | 5 final answers; repair interrupted |
| b165 | SDK-hosted OpenCode | 18 | 911.333 | 12.562 | 17/17 | 14 child summaries present; not seventeen proven empty results |
| 424f | SDK-hosted Codex | 5 | 286.584 | 6.659 | 4/4 | all 4 have nonempty answers |
| 1e57 | SDK-hosted Pi | 5 | 170.529 | 0.801 | 4/4 | all 4 have nonempty answers |
| 5889 | SDK-hosted Claude Code | 5 | 122.448 | 1.557 | 4/4 | all 4 have nonempty answers |
| ead0 | standalone OpenCode | 5 | 102.082 | 0.505 | 4/4 | all 4 have nonempty answers |

The eighteen-agent run is b165, not d511. The no-output classification defect
is shared across adapters; the ~190-second synthetic stall is specific to the
GraphCoder reconstruction in this comparison. The four five-agent siblings
are negative controls for interpreting no-output as absence of text output.

## Required swarmbench changes

1. In `harness/native/graphcoder.py`, join completed model payload IDs to the
   proxy records for wall-clock intervals. Preserve SDK dispatch identity,
   latency and timing provenance separately. Never convert cumulative durations
   into wall-clock agent/tool spans. When proxy or commit timestamps are
   unavailable, keep them unknown. In `autopm/forensics.py`, suppress stalls,
   wrap-up, peak concurrency and timing conclusions based on synthetic spans.
2. Decode child `message_completed` payloads into messages and summaries.
   Distinguish provider-call completion, answer completion, idle settlement,
   failure and interrupted/unknown outcome. Do not default unobserved child
   settlement to succeeded, or set `ended_at` to accumulated model duration.
3. In `autopm/agents.py`, represent missing change/publication telemetry as
   unknown. Only classify no file/publication output when those capabilities
   were observed completely, and report text/message output separately. A
   read-only reviewer with an answer is useful output. Apply this capability
   check across all native adapters; absence of spans is not evidence of zero.
4. For a settlement-to-wake measurement, export the original child settlement
   commit and parent inbox `ChildSettled` append/consumption identity and time,
   plus wait registration/result commits. Do not substitute provider EOF or
   reconstructed transcript end. Existing transcript projections do not contain
   these timestamps; exporting them is instrumentation, not repairing this
   complete capture.

Deterministic benchmark controls should use delayed child spawns with real
proxy intervals and nonempty child messages: they must not generate a tail
stall or no-output findings. Negative controls should use genuinely missing
timing (unknown), a recorded completed empty turn with observed zero changes
(no-output), and an interrupted tool call (not succeeded). Reanalyze these six
runs after the adapter and capability fixes. No benchmark files are changed
in this SDK PR.

## Deterministic control

`agent_model::awaiting::completion_burst_wakes_root_on_last_delivery` exercises
the real AgentHost, actor delivery, Streams commits and follow at a fixed
ControlledClock timestamp. Eighteen children finish at that same timestamp.
With only seventeen completions, the root must retain its original 200-second
deadline and must not resume. The final completion must resume it exactly once
at that completion's exclusive inbox cursor, without advancing time or
delivering another event. This is a protocol regression guard, not a claim
that a missed wake was reproduced in the supplied run.
