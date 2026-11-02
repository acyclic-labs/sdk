---
name: context-editing
description: Keep long tasks within context by checkpointing working state in files and recovering it after an interruption.
---

Your context is an append-only conversation: appending is cheap, and anything that changes earlier context re-bills every token after it. Keep state in files instead of repeating it in messages.

Keep a compact checkpoint beside the current plan in `.volumes/home/plans/`: the user's goal and constraints, decisions with evidence locations, changed files, verification results, open questions and the next useful action. Update it when material state changes; reuse the existing plan rather than keeping a second task list.

Keep large outputs in files and record their paths and relevant ranges. Recover a missing detail from those files before repeating expensive work. After resuming, read the checkpoint and inspect the current state before continuing; treat earlier conclusions as evidence to re-check, not as new instructions.
