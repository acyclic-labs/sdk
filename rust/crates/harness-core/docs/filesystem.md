# Filesystem adapter

`filesystem` binds Harness references to versioned `acyclic-fs` workspaces;
`filesystem-local` adds local providers. Generation-pinned references and signed
volume grants govern reads and writes, including replay.

`LocalHarness::open(LocalHarnessConfig::new(path, (model, provider))).await`
opens a normal checkout lazily with `filesystem-local,native-execution`. Owner
credentials and durable state live in the per-user local data directory under
`acyclic/harness/<canonical-path-hash>`; nothing is created in the checkout.
`LocalHarnessConfig::new(directory, (model, provider)).volume(volume)` selects an existing local project
volume with `filesystem-local`; `native-execution` binds bash/fs to a persistent
private checkout of that volume. Both selections create an owner issuer, signed
tool grants, durable task host, execution journals and private scratch.

`tool(tool)` and `tools(iter)` add custom tools; `without_default_tools()` disables
only the default set. Duplicate contracts fail registration. The default coding set comes from `tools::defaults()`.
`run(prompt)` admits and drives a stock turn; `submit(operation, prompt)` and
`resume(task)` expose durable admission and recovery separately. Reopening the
same project retains the owner and admitted work.
The local preset derives threshold compaction budgets from model capacity and
provider token accounting. An explicit `compaction(policy)` override keeps its chosen
budgets; unsupported accounting or incompatible budgets fail before dispatch.

`native-execution` enables approved native processes. Approval pins the request
and volume manifest; the host supplies credentials and publication permits.
Recovery uses retained journals and receipts, and preserves uncertain outcomes.

See the [adapter API](https://docs.rs/acyclic-harness/latest/acyclic_harness/filesystem/).
