# Scaling by graph and participant count

Use the [shared admission, custody and analysis contract](../README.md).
Start with `../tasks/quantity.md` only as a smoke check. Prepare independent
tasks/cuts before an inferential pilot. Use `../scorers/quantity.json` for the
supplied task; other objectives need their own predeclared scorer inputs.

The initial contrast has four task-solving participants in both arms:

| Arm | Graph |
| --- | --- |
| chain | Three predecessor assessors followed by one final assessor |
| fan-out | Three independent assessors followed by one final join |

The count includes the final assessor; coordinator/source work is reported
separately. Give every participant the same evidence. Keep actual model,
grant reach and aggregate token/budget reservation fixed. Do not substitute
per-participant reservations that multiply the total budget as count grows.
Missing aggregate admission blocks the comparison.

After the initial pilot, sizes 1, 2, 4 and 8 describe a possible declared matrix,
not product limits or authorization to expand the run. Chain and fan-out
coincide at sizes 1 and 2 and supply no distinct topology evidence there.
Any expansion needs its own assigned-trial budget. Cycles require an explicit
termination choice.

Each predecessor wait names its producer's original committed result. A lost
branch must fail the join through the shared typed terminal. Verify actual
members/edges against the admitted graph and requested count. A graph mismatch
is an invalid trial, not a low score. Original committed spans supply latency;
no periodic resource sampling or readiness checks are added.

Primary outcomes: objective correctness and actual USD per assigned trial.
Also report failures, priced-call coverage, total provider tokens and measured
critical-path duration. Pair graph/size contrasts within independent problems,
then cluster over problems. A few modest sizes do not establish an asymptotic
law; any curve is exploratory and shows its measured range and uncertainty.

Negative controls: sizes 1 and 2 have equivalent graph structure; a missing join
producer terminates with its original cause; changing the requested count must
change observed membership without changing the admitted aggregate budget.

| Graph | Participants | Assigned | Correct | Critical path | USD/run |
| --- | --- | --- | --- | --- | --- |
| chain | 4 | 0 | — | — | Unknown |
| fan-out | 4 | 0 | — | — | Unknown |

Not run. No size-matrix or paid execution is enabled by these files.
