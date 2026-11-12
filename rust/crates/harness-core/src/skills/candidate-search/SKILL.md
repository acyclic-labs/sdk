---
name: candidate-search
description: Explore independent attempts in forks, compare their evidence as files, and merge a model-chosen or human-chosen result.
---

Choose when to explore and how many attempts the task warrants, within the user's reservations. The harness does not choose a width, depth, scoring rule or winner. A parent may repeat this procedure inside any candidate to explore a deeper tree.

Use spawn to fork independent attempts from the settled conversation and workspace. Give each candidate its goal, constraints and the checks it should run. Keep implementation changes in the candidate's project volume. Publish evidence with ordinary file tools under `volume:///shared/results/<comparison>/<fork>/tests.json` and `logs/`. Use a unique comparison directory so later attempts retain earlier evidence. The shared volume lets the parent read each result and log directly before choosing; private agent files are not a cross-agent results channel. Record the command, exit status, observations and references to retained output; distinguish an unrun check from a pass. Use wait to receive settled results, never a sleep or a readiness probe.

Use `fs diff agents/<ref>` for the change summary; it does not expose file contents. Read every candidate's shared tests.json and relevant logs before comparing or choosing. Write the comparison to `volume:///private/ui/compare.md`: describe each candidate, its evidence and remaining uncertainty. A model may choose based on the task's objective and this evidence. A parent may choose instead, or the human may choose through an ordinary input fence:

````markdown
```input pick-fork id=winner
- candidate-a
- candidate-b
- candidate-c
```
````

Tell the human to open `/view compare.md` to display that file, then end the turn to wait. The reply is the ordinary `{ui,generation,answers,text}` prompt. Bind the selection to the displayed candidate list and evidence; free-form text is always available. A reply is a request to the agent, never executable markdown or authority to bypass policy. If the selection is missing, ambiguous or names another candidate, ask for clarification rather than guessing.

Merge the selected candidate with `fs merge agents/<ref>`. If the merge fails or reports conflicts, retain every candidate and repair or report the conflict before proceeding. After a successful merge, archive the unselected candidates and wait for their closed state so they cannot receive further model calls. Retain their evidence. Run the covering checks on the merged result and report the selected branch, rationale and verification. At each level of a deeper tree, use the same procedure; merge grandchildren into their parent before comparing that parent with its siblings.
