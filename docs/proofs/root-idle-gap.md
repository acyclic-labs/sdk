# Root wait evidence: contract-deep-read-d511

The supplied capture does not contain a contiguous 190-second inactivity gap.
It contains one root and six direct children, recorded in
`transcripts/agents.jsonl`, rather than 18 children. No production latency fix
is justified by this capture. The private-file custody failure is outside this
investigation.

Evidence directory: `Q:/lanes/overhaul/swarm-evidence/contract-deep-read-d511/`.
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

Root model latency in `usage.jsonl` sums to 85.443662269 seconds. Subtracting
that from the approximately 275-second run gives approximately 190 seconds
(69%). This arithmetic is consistent with classifying all time outside root
model calls as idle; it does not establish inactivity. That time also includes
tools and waits while children work. The longest individual model latency in
the entire capture is 17.774505692 seconds.

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
This capture does not include the child actor checkpoints or parent inbox
deliveries needed to measure the delay from the last child's actual settlement
commit. It cannot establish an exact missed-wake cause, or identify a first
event after a nonexistent 190-second gap. A capture of the reported 18-agent
run, including those completion deliveries, is needed for that attribution.

## Deterministic control

`agent_model::awaiting::completion_burst_wakes_root_on_last_delivery` exercises
the real AgentHost, actor delivery, Streams commits and follow at a fixed
ControlledClock timestamp. Eighteen children finish at that same timestamp.
With only seventeen completions, the root must retain its original 200-second
deadline and must not resume. The final completion must resume it exactly once
at that completion's exclusive inbox cursor, without advancing time or
delivering another event. This is a protocol regression guard, not a claim
that a missed wake was reproduced in the supplied run.
