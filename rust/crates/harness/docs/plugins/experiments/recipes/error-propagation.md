# Error propagation

Use the [shared admission, custody and analysis contract](../README.md).
Bind a diamond: seed producer → two independent quantity assessors → join
assessor. All three assessors receive `../tasks/quantity.md`. The seed producer
publishes one selected input:

| Arm | Seed input |
| --- | --- |
| correct-seed | `../inputs/correct-proposal.txt` |
| wrong-seed | `../inputs/wrong-proposal.txt` |

Both payloads have identical framing. Hold graph, authentic producer, evidence,
model and delivery order fixed. Each branch observes its own seed input; the
join waits separately for the original committed decision from each branch.
Every producer's loss ends its corresponding wait through the shared call
failure path. No extra model step checks readiness.

Score with `../scorers/quantity.json`. Primary outcome: adoption of 52 at the
join. Report adoption at each reachable node and edges with both the original
producer decision and recipient input recorded. Stream the original edge table
once offline: O(1) work per edge, O(E) total, without a per-arrival reachability
scan. Correlated answers alone do not prove error transmission.

Negative controls: correct seed remains recoverable as 47; input on an ungranted
stream produces no delivery edge; losing one predecessor aborts the join with
the producer's original typed cause. Do not turn loss into an alarm retry,
partial join or a silent omission.

| Arm | Assigned | Join adoption | Node adoption | Failure rate | USD/run |
| --- | --- | --- | --- | --- | --- |
| correct-seed | 0 | — | — | — | Unknown |
| wrong-seed | 0 | — | — | — | Unknown |

Not run. The authored seed itself requires no model call.
