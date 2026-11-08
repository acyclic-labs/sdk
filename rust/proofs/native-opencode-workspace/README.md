# Native OpenCode workspace admission

Set `ACYCLIC_TEST_BINARY`, `E2E_GRAPHCODER`, `E2E_MOCK_MODEL` and `E2E_OPENCODE` to absolute
executables built from the selected source and native OpenCode 1.19.5. Run:

```sh
node --test --test-concurrency=1 plugin/tests/hosts/opencode-workspace.test.mjs
```

The shipping installer stages the native extension in a fixture profile. The
real native CLI consumes the original llama.cpp text response retained by
the inference conformance suite. Its completed session ID must match the
shared workspace's original `NativeSession` record. Process retirement precedes
the final shared capture. The profile explicitly selects OPENCODE_CONFIG_DIR;
its native provider config lives there rather than in the workspace. The
shipping deterministic model producer then drives the installed native MCP
spawn/wait path and a child write tool. The child file must exist in its settled
captured view, and its native session must export from the same trial data store.
Native debug paths are recorded without reading authentication files.

Apply `root-admission.patch` to omit the adapter's actual root admission. The
same control must fail with `native OpenCode session was not recorded through
shared workspace`. Reverse the patch and require the unchanged control to pass.
No compiler or substitute native process is involved in this omission.

Apply `helper-exports.patch` and rebuild `acyclic` to retain the former shared
helper exports in the installed extension. OpenCode treats each export as a
plugin initializer; the same native control must refuse this broken closure.
Restore the composer, rebuild, and require the original control to pass. The
default installation uses `.js`, matching native plugin discovery.

This control does not establish authentication refresh, generation-based cwd
reuse, detached writer quiescence, merge/resume or full platform acceptance.
