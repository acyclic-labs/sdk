---
name: goal-loop
description: Keep working across turns toward an objective until it is done or blocked.
---

Write the objective to `.volumes/home/GOAL.md` only when looping on a goal. After every settled turn the harness continues while its frontmatter status is `active` (plain Markdown counts as active), up to the continuation allowance. Set `status: done` when the objective is met, or `status: blocked` with a `reason` when you need input or an external change:

```
---
status: blocked
reason: waiting for the API key
---
```

Keep the plan and evidence in `.volumes/home/plans/` (see plans-and-memory); GOAL.md holds only the objective and its status.
