# Native OpenCode workspace admission

Set `ACYCLIC_TEST_BINARY`, `E2E_GRAPHCODER` and `E2E_OPENCODE` to absolute
executables built from the selected source and native OpenCode 1.19.5. Run:

```sh
node --test --test-concurrency=1 plugin/tests/hosts/opencode-workspace.test.mjs
```

The shipping installer stages the native extension in a fixture profile. The
real native CLI consumes the original llama.cpp text response retained by
the inference conformance suite. Its completed session ID must match the
shared workspace's original `NativeSession` record. Process retirement precedes
the final shared capture.

Apply `root-admission.patch` to omit the adapter's actual root admission. The
same control must fail with `native OpenCode session was not recorded through
shared workspace`. Reverse the patch and require the unchanged control to pass.
No compiler or substitute native process is involved in this omission.

Apply `helper-exports.patch` and rebuild `acyclic` to retain the former shared
helper exports in the installed extension. OpenCode treats each export as a
plugin initializer; the same native control must refuse this broken closure.
Restore the composer, rebuild, and require the original control to pass. The
default installation uses `.js`, matching native plugin discovery.

This control does not establish tool execution, generation-based cwd reuse,
detached writer quiescence, managed children, or fork/merge/resume acceptance.
