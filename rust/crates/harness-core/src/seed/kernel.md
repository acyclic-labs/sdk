---
version: 1
---
# Kernel rules

This file is read-only. It describes the fixed harness loop; nothing here is configured.

## Loop order

1. A turn starts when input arrives: a user message, a message from another agent, or a goal continuation.
2. Each step renders the request (system prompt, discovered instructions and skills, then the conversation) and makes one model call.
3. Every tool call in the reply is admitted against the agent's grants and approval policy, then executed. Independent calls run in parallel.
4. Each tool result is appended to the conversation, and the next step begins.
5. The turn settles when a reply has no tool calls. With an active GOAL.md, the goal check may start another turn.

## Outcome contract

- Every tool call ends exactly once: ok, error, cancelled or indeterminate.
- A refused action is a tool failure, `tool execution failed: <reason>`. The run continues; the action did not happen.
- An external effect happens at most once. When its outcome cannot be known, the call is indeterminate, never silently retried.

## No-stuck rules

- A failed model call settles the turn with a notice.
- Cancel stops the open calls and settles the agent idle, whatever its files contain.
- A malformed optional file turns its feature off; it never blocks a step.

## Cache cost model

- The request prefix is cached by the provider. Appending is cheap.
- Editing earlier context re-bills every token after the edit.
- Tool declarations and the system prompt are part of the prefix, so changing them re-bills the whole conversation.

## Layers

Every path an agent reads through `.volumes/effective/` resolves over the layers, highest first: `home` (the agent's own `.volumes/home/`), then `seed` (the harness defaults in `.volumes/seed/`). `sys/` is not layered.

| Rule | Files | Effect |
|---|---|---|
| replace | every file that does not compose | the highest layer holding the file wins whole |
| compose | `agent.ts`, `plugins.ts`, `plugins/<name>/provider.ts` | each layer's module default-exports a value, replacing the result below it, or a function `(base) => value` of that result; they are folded from the lowest layer up |
| off | a file holding only `off: true` | removes the lower layers' file |

The layer stack's identity is the hash of its ordered layer generations; an unchanged hash means nothing to reread.

## Settings are code

Settings files are TypeScript modules, typed by `sys/types/config.d.ts`. They may `import` other files of the effective tree (`./x.ts`, `./x.json`) or `sys/` statically; they have no clock, randomness, network, tools or `import()`, and a small instruction budget. The result must be plain data. `policy.ts` exports plain data, never a function.

The kernel reads `agent.ts` (model, effort, sampling) when a session starts. Each version of your `agent.ts` is recorded as an admission: `applied`, or `invalid`/`rejected` with the error, in which case the defaults below your file stay in force. A `policy.ts` your issuer wrote binds you and is read-only to you; one you wrote is advisory.

## Read-only paths

- `.volumes/seed/` ships with the harness. Override a file by writing the same path under `.volumes/home/`.
- `sys/kernel.md` is this file.
- `sys/types/config.d.ts` declares the settings and their records.
- `sys/native/<family>/` holds the exact tool declarations each model family was trained on.
