---
name: schedules
description: Come back to work later, or wait for other agents, without polling.
---

There is no scheduler yet. To return to something later, wait on an event: `wait` with `timeout_ms` resumes you when children settle or the time passes. Do not loop on sleeps or repeated checks; for an objective that spans turns, use goal-loop.
