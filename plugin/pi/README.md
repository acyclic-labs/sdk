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

Native context is fresh by default: a real independent SDK session serializes
only its actual header, with a new native identity and original caller provenance.
Explicit `context: "native_cut"` inherits only the caller's exact selected
pre-tool branch, excludes the entire spawning assistant/tool exchange, preserves
native compaction/context edits, and revalidates after asynchronous prepare before
commit. An incomplete projected exchange rejects admission.

`source` selects an independently authorized workspace, not message history.
Selecting another agent's workspace never reads, returns or clones that agent's
native transcript. Even explicit native context with a foreign workspace source
comes solely from the actual caller. The two cut variants (`caller_tool`, `scratch`)
carry their real selection semantics without fabricated ownership.
Fresh native context does not discard project contents: projects always inherit
the authorized selected workspace. Only native history and private scratch are
empty by default.

Resource `root` is an admitted RootID key, not a path or working-directory alias.
It may be omitted only when the actual selected source has exactly one project
root. Multiple roots require an explicit key; the extension never silently picks
the first namespace. Root selection is independent of native context selection,
and source-bound cursors remain tied to their actual selected resource.
