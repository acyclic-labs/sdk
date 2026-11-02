---
name: check-on-agents
description: Find out what child agents are doing and whether they have finished.
---

`wait {agents, need?, timeout_ms?}` reads each named child's status once enough of them have settled or the timeout passes: settled children return their last answer and branch, and the others are reported running. Use a short `timeout_ms` for a progress check that does not block on completion.

Inspect a child's actual work with `fs diff agents/<name>` rather than trusting its report alone. Ask a running child for a status note with `send`; it replies at its next turn boundary.
