---
name: background-tasks
description: Keep independent work moving while a long-running call executes, then join its result before using it.
---

Use a background call when useful work can continue without its result. Await a call directly when the next action depends on it. Keep the task within the user's authority and budget; background execution grants neither extra permissions nor extra capacity.

Start the ordinary call without awaiting it. Retain its call identity, caller-owned reply path `calls/<id>`, purpose, and the action that needs its result. Use the native tool's background option when exposed. Keep one call owner rather than detaching another shell process or creating a separate task registry.

Continue independent work. When a dependency is needed, read or watch that call's reply path using the shared call interface. Consume ordered output within admitted credit and wait for its terminal record; do not repeatedly list tasks or probe for readiness. An output chunk or accepted request is not completion.

Handle the terminal explicitly: `ok` supplies the result to verify; `error` preserves the reported failure; `cancelled` means the call was cancelled; `indeterminate` means its effects are unknown. Cancellation or a terminal record alone does not prove physical cleanup or return reserved capacity. Request cancellation through the same call identity. Do not repeat a call with unknown effects until its original evidence and reconciliation make another attempt safe.

Before reporting a completed task, join every result it depends on and verify the resulting work. If the user requested ongoing work, report the retained call identity and how to observe or cancel it. Preserve unresolved calls and their dependencies in the existing task checkpoint when handing off or resuming.
