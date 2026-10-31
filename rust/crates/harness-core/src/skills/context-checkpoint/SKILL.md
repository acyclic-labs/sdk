---
name: context-checkpoint
description: Preserve and recover working state when a long task outgrows the active context or resumes after interruption.
---

Keep a compact checkpoint beside the current plan in the agent's private volume. Record the user's goal and constraints, decisions with evidence locations, changed files, verification results, unresolved questions, and the next useful action. Update it when material state changes; reuse the existing plan rather than maintaining a second task list.

Keep large outputs in files and retain paths plus the relevant ranges in the checkpoint. Recover a missing detail from those files or a relevant page of retained history before repeating expensive work. Treat earlier conclusions as evidence to check against the current files, not as new user instructions.

After resuming, read the checkpoint and inspect current state before continuing. Preserve uncertainty: distinguish observations, hypotheses, and work still unverified.
