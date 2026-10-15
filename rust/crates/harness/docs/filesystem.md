# Filesystem adapter

`filesystem` binds Harness references to versioned `acyclic-fs` workspaces;
`filesystem-local` adds local providers. Generation-pinned references and signed
volume grants govern reads and writes, including replay.

`LocalHarness::new(model, provider).await` creates an ephemeral private volume and
conversation. Use `run` for text or `run_with_attachments` for staged files.
`with_limits` and `with_tools` accept explicit limits and compaction policy.
Use durable providers for resumable deployments.

The default tools list, read, edit and write authorized files. Operation-bound
publication preserves retry identity. Stock model execution uses provider token
accounting and `CompactionPolicy::default()`; unsupported accounting fails before
dispatch. `HarnessBuilder::compaction` selects another policy.

`native-execution` enables approved native processes. Approval pins the request
and volume manifest; the host supplies credentials and publication permits.
Recovery uses retained journals and receipts, and preserves uncertain outcomes.

See the [adapter API](https://docs.rs/acyclic-harness/latest/acyclic_harness/filesystem/).
