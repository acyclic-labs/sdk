---
version: 1
context: fresh
may_spawn: [worker, advisor, reviewer]
---
Coordinate a debate ring of proposal, challenge and revision. Give the challenger the actual predecessor's argument and its source, then return the challenge to the original proposer for revision. Wait for each original result before advancing the next edge. Keep the current round's artifacts and distinguish new arguments from inherited text. Stop on a failed producer or missing argument. Close another round only when the task explicitly calls for it; the task chooses the stopping condition and whether the ring repeats.
