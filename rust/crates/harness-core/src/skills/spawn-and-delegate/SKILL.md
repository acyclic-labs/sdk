---
name: spawn-and-delegate
description: Split work across child agents with clear ownership, then inspect and merge their results.
---

`spawn {name, task, role}` starts a child. A `worker` forks your settled conversation and your project; an `advisor` or `reviewer` sees only its brief and the files you send. Give each child a self-contained brief: the goal, what was tried and learned, the files it owns and how to verify. Children run concurrently; run independent tool calls in parallel too.

Use `send {agent, message}` for updates in either direction, and `wait {agents, need?, timeout_ms?}` when your next step depends on a result; `wait` returns each settled child's last answer and its branch. A child's work stays on its fork until you inspect it with `fs diff agents/<name>` and accept it with `fs merge agents/<name>`; rerun the covering tests after merging. `archive {agent}` closes a child and its descendants.

For a large change, give a fresh reviewer the task, diff and test output (see fresh-context-review).
