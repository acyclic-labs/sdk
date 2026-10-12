# Pi integration

Pinned host: `@earendil-works/pi-coding-agent` 1.1.0.

```sh
acyclic install pi
```

| Tool | Arguments | Native operation |
| --- | --- | --- |
| `acyclic_spawn` | `id`, `prompt`, optional workspace `source`, optional resource `root`, optional `context` (`scratch` by default, or explicit `native_cut`) | `SpawnPrepare`, actual SDK child session serialization, then `SpawnCommit` |
| `acyclic_send` | `id`, `message` | `TaskSend`, one original mailbox publication |
| `acyclic_steer` | `id`, `message` | `TaskSteer`, original native task steering |
| `acyclic_wait` | `id` | `TaskWait`, at most 120 passive nonblocking observations over 30 seconds; only an actual terminal outcome completes the wait |
| `acyclic_inspect` / `acyclic_diff` | `id`, optional resource `root`, optional `directory` (relative, directory root by default), optional `cursor`, optional `maximum_entries` (default 64, maximum 256) | `TaskInspect` / `TaskDiff`, authenticated pinned-source directory pages |
| `acyclic_import` | `source`, `destination`, optional resource `root`, `paths`, `confirm` | `TaskImport`, selected source reads and authorized destination writes |
| `acyclic_writeback` | `source`, optional resource `root`, `paths`, `confirm` | One `TaskWriteback` prepare; real native UI approval or outside-model host consent; headless `TaskWritebackStatus` passively observes the same original SDK operation |
| `acyclic_model` | optional paired `provider`, `model` | Actual Pi model registry lookup and `pi.setModel`; omitted arguments observe the native selected model |
| `acyclic_context` | none | Actual `ctx.getContextUsage` and selected SDK session/leaf identity, not Harness accounting |
