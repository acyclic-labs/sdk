# Harness integrations and extensions

This guide defines the planned integration boundary for `acyclic-harness`. It is
Rust-owned product documentation; it does not claim that a CLI, plugin package,
lifecycle hook, compatibility version, or hosted integration is currently
available.

## Integrations and harness extensions

An integration connects an existing product such as Claude Code, Codex, Pi,
OpenCode, or DSH to supported Acyclic capabilities. A harness extension supplies
components inside the open-source Harness crate. They can coexist, but they have
different installation, lifecycle, and compatibility contracts.

## Choose the smallest integration

| Approach | Useful for | What it does not imply |
| --- | --- | --- |
| Direct service client | Use a Workspace, Machine, or Inference Context from an existing program | Adopting the Acyclic agent loop |
| Tool or protocol adapter | Expose typed tools, artifacts, and supported questions to an existing agent | Access to all of that agent's internal state |
| Task adapter | Run an external agent and observe its results as a harness task | Recovery of hidden tools or process-local promises |
| Native harness component | Replace a model, loop, context builder, or executor through public traits | Source compatibility with another product's extension API |

## Negotiate capabilities per host

An adapter reports tool execution, streaming, questions, approval routing,
cancellation, retained state, and fork support separately. An unavailable hook
produces an unsupported result or a caller-selected simpler workflow. It does
not establish that a full session fork occurred.

A Filesystem fork can isolate another agent's edits even when that agent cannot
fork its conversation or live process. Promotion still uses the existing
Filesystem `JoinPlan` and expected-generation checks. Tool approval remains
bound to the actual invocation under the host's effective policy.

## Wrap an opaque agent

When only process input and output are exposed, the adapter observes one opaque
invocation. It can report progress and retained artifacts, but cannot promise
exactly-once hidden effects, internal tool recovery, or arbitrary conversation
or process cloning. A lost connection may leave the invocation indeterminate.

Prefer an explicit typed tool or task integration when the host supports it. Reuse
one task identity and reconcile unknown execution rather than launching another
external agent after every connection failure. This page records the intended
contract; release availability requires a separately qualified integration.

## Related contract guides

Use the [Filesystem branches and joins](../../filesystem/docs/topics.md) and [Harness filesystem](filesystem.md) guides for the promotion and tool-approval boundaries. Recovery profiles and design references remain planned documentation until their host integrations are qualified.
