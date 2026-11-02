---
name: swarm-topology
description: Choose a shape for multi-agent work, such as fan-out, pipeline, candidate search, review loop or consultation.
---

Pick the smallest topology that fits:
- Fan-out: independent parts go to parallel workers with disjoint file ownership; `wait` for all of them (or `need: 1` for the first useful answer) and merge each accepted fork.
- Pipeline: one child's merged result becomes the next child's input; brief each stage with the prior stage's evidence.
- Candidate search: several workers try alternative approaches; compare their evidence and merge one (see candidate-search).
- Review loop: a worker implements and a fresh reviewer of another model family judges, with at most one re-review (see fresh-context-review).
- Consultation: send a versioned plan file to an advisor at a real decision point and keep working while its answer does not block your next step (see advisor-consultation).

Children of children form a tree under your agent path. Keep the tree shallow, give every branch an owner, and archive children you no longer need.
