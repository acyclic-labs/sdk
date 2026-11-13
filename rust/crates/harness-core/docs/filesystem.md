# Filesystem adapter

`filesystem` binds Harness references to versioned `acyclic-fs` workspaces;
`filesystem-local` adds local providers. Generation-pinned references and signed
volume grants govern reads and writes, including replay.

`LocalHarness::open(LocalHarnessConfig::new(path, (model, provider))).await`
opens a normal checkout lazily with `filesystem-local,native-execution`. Owner
credentials and durable state live in the per-user local data directory under
`acyclic/harness/<canonical-path-hash>`; nothing is created in the checkout.
`LocalHarnessConfig::new(directory, (model, provider)).volume(volume)` selects an existing local project
volume with `filesystem-local`; `native-execution` binds shell/fs to a persistent
private checkout of that volume. Both selections create an owner issuer, signed
tool grants, durable task host, execution journals and private scratch.

`tool(tool)` and `tools(iter)` add custom tools; `without_default_tools()` disables
only the default set. Duplicate contracts fail registration. The default coding set comes from `tools::defaults()`.
`run(prompt)` admits and drives a stock turn; `submit(operation, prompt)` and
`resume(task)` expose durable admission and recovery separately. Reopening the
same project retains the owner and admitted work.
The local preset admits each request against the model's window using provider
token accounting: one `context.pressure` notice at the threshold, then a
mechanical backstop that evicts the oldest whole exchanges into a fixed marker,
with no model call. An explicit `compaction(policy)` override keeps its chosen
reserve and threshold; unsupported accounting or incompatible budgets fail
before dispatch.

`native-execution` enables approved native processes. Approval pins the request
and volume manifest; the host supplies credentials and publication permits.
Recovery uses retained journals and receipts, and preserves uncertain outcomes.

See the [adapter API](https://docs.rs/acyclic-harness/latest/acyclic_harness/filesystem/).

The model-visible command tool is `shell` on every platform. It runs bash on
Linux/macOS and PowerShell 7 (or Windows PowerShell 5.1) on Windows; the
environment context records that command language. Hook matchers use `shell` only.
A hook command's `shell` field continues to select its interpreter.

Shell lifetime is unbounded when `timeout_ms` is omitted. A positive lifetime
selects the original process deadline; background handoff never restarts it.
Cancellation and authenticated scope expiry still stop the original process tree.

Native checkout execution accepts `background: true`. Anthropic maps
`run_in_background`, Gemini maps `is_background`, and the OpenAI argv interface
uses `background`. Admission returns after the original capture owner starts;
the command survives the admitting turn. The result names its private output
volume and `tools/<turn>/<operation>` directory. `request.json` is immutable,
`started.json` records admission once, `stdout/` and `stderr/` contain complete
ordered chunks, and `result.json` is published once after cleanup and workspace
capture. Inline output is a bounded preview with complete spill.

Writing the returned `cancel` path stops only that job through a native filesystem
notification. Archive stops the agent's jobs through its committed inbox event.
Runtime loss drops their stop guards; process custody fences a crashed host's
remaining trees before recovery. Recovery observes the original admission and
never launches the command again. Concurrent commands share the selected native
projection; filesystem publication is serialized by that projection's owner.
Commands writing the same paths have the host filesystem's ordinary concurrency
semantics. Machine backends without a managed job owner refuse background mode
before dispatch.

Directory queries bound candidate names consumed before filtering by current policy.
An empty visible page with `next` is progress, not exhaustion. Pass that opaque
continuation to the same directory; it retains the full authenticated opening
generation and never grants read authority. Every page checks current policy.
`examined` counts those candidates, including hidden names, so upstream queries
charge sparse pages too. SDK seek and witness work remains in its work receipt.
Continuations encrypt hidden progress through the shared Auth cursor primitive;
they retain no server-side query state and expire when the process key retires.
File entry versions remain immutable read addresses; directory continuation uses
`next`, independently of which entries fit the caller's row and byte credit.
