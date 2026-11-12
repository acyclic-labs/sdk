---
name: candidate-search
description: Explore independent attempts in forks, compare their evidence as files, and merge a model-chosen or human-chosen result.
---

Choose when to explore and how many attempts the task warrants, within the user's reservations. The harness does not choose a width, depth, scoring rule or winner. A parent may repeat this procedure inside any candidate to explore a deeper tree.

Use spawn to fork independent attempts from the settled conversation and workspace. Give each candidate its goal, constraints and the checks it should run. Keep implementation changes in the candidate's project volume. Keep evidence in private `results/<fork>/tests.json` and `results/<fork>/logs/`, using ordinary file tools. Record the command, exit status, observations and references to retained output; distinguish an unrun check from a pass. Use wait to receive settled results, never a sleep or a readiness probe.

Compare the actual changes with `fs diff agents/<ref>` and read the evidence. Write the comparison to `volume:///private/ui/compare.md`: describe each candidate, its evidence and remaining uncertainty. A model may choose based on the task's objective and this evidence. A parent may choose instead, or the human may choose through an ordinary input fence:

````markdown
```input pick-fork id=winner
- candidate-a
- candidate-b
- candidate-c
```
````

Publish or link that comparison from `ui/index.md` when asking the human, then end the turn to wait. The reply is the ordinary `{ui,generation,answers,text}` prompt. Bind the selection to the displayed candidate list and evidence; free-form text is always available. A reply is a request to the agent, never executable markdown or authority to bypass policy. If the selection is missing, ambiguous or names another candidate, ask for clarification rather than guessing.

Merge the selected candidate with `fs merge agents/<ref>`. If the merge fails or reports conflicts, retain every candidate and repair or report the conflict before proceeding. After a successful merge, archive the unselected candidates and wait for their closed state so they cannot receive further model calls. Retain their evidence. Run the covering checks on the merged result and report the selected branch, rationale and verification. At each level of a deeper tree, use the same procedure; merge grandchildren into their parent before comparing that parent with its siblings.
