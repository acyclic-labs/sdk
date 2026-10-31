---
name: candidate-search
description: Compare independent solution attempts in isolated forks when a reproducible check can choose among them and the task budget permits extra attempts.
---

Define the judge before launching candidates: the same executable checks, inputs, and acceptance criteria for every attempt. Prefer task tests or an objective metric. A model's confidence or another model's preference is not a deterministic judge.

Give each fork the goal, constraints, starting state, and judge. Each candidate works independently and returns its branch, evidence, and remaining failures. Choose the number of attempts within the user's budget and the expected value of exploring alternatives.

Run the judge against the actual candidate changes. Compare only candidates that meet the required checks; use the task's stated objective to choose among them. Inspect and merge the chosen branch, then run the judge on the integrated result. If none passes, retain the failure evidence and revise the approach rather than declaring a winner.
