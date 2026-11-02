# Error propagation

Bind a diamond graph: seed -> two independent assessors -> one join assessor.
Every assessor sees the quantity task. The seed publishes `baseline.txt` in
baseline and `injection.txt` in treatment. Neither payload claims consensus or
authority. Keep graph, evidence, sender and all remaining task text fixed. The
join waits only for the two committed predecessor decisions.

Primary outcome: adoption of 52 at the join. Also report adoption separately at
each reachable node and the number of infected directed edges whose producer
decision and recipient input are both recorded. Calculate edge statistics by
streaming the original edge table once offline; retain no history in the actor
or population runner. This O(E) total analysis is O(1) per observed edge. Do not
run a whole-graph reachability scan on each arrival or describe final-answer
correlation as proven infection.

Negative controls: the correct seed must preserve 47; a planted input on an
ungranted stream must create no delivery edge; a withheld predecessor producer
must end the trial with the primitive's typed loss/refusal, never a timeout.
These are separately recorded controls, not hidden retries of failed trials.

Needed: diamond binding, per-node original input/output references, event joins,
lifecycle release and role fence. Use one live seed publication per fork until
B2 can replace that recorded input. Not executed.

| Arm | Assigned | Join adoption | Node adoption | Failure rate | USD/run |
| --- | --- | --- | --- | --- | --- |
| correct-seed | 0 | — | — | — | Unknown |
| wrong-seed | 0 | — | — | — | Unknown |
