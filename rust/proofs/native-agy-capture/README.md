# Native capture and writing children

`plugin/tests/hosts/agy-capture-admission.test.mjs` installs the shipping package
into an exclusive Antigravity 1.5.2 profile. It replaces the actual admitted
mount directory, requires native capture refusal with zero provider requests,
restores that same directory and requires the original authentication-refused
request. It then uses the shipping deterministic model producer to drive a real
native file tool, two managed children and native model-issued MCP spawn/wait
to a writing child. Its native file tool gets the absolute cwd from the original
native request using the shared model producer's single-line binding. Missing or
ambiguous paths fail instead of guessing a view generation. Reopen the settled
task before inspecting its captured file; the completed process's cwd is disposable.
The native profile is inspected after settlement. No model is purchased.
Workspace, profile, executable and package paths
contain spaces.

Run the existing control with matching absolute executables:

```sh
ACYCLIC_TEST_BINARY=/absolute/acyclic \
E2E_GRAPHCODER=/absolute/graphcoder E2E_AGY=/absolute/antigravity \
E2E_MOCK_MODEL=/absolute/harness-mock-model \
node --test --test-concurrency=1 plugin/tests/hosts/agy-capture-admission.test.mjs
```

`stop-continues.patch` restores the actual refusal disposition that caused the
native host to repeat the failed admission. Rebuild `acyclic-plugin` after
applying it and use that executable with the same control. The original native
error-step event rejects re-entry and joins physical process retirement before
directory restoration; no timeout or polling establishes this failure.

The fix changes the existing Stop response. It adds no exchange, allocation
site, syscall, durability barrier, lock, task, wait or stored error state.
Admission still uses the one shared capture writer. Final root capture remains
the original shared launcher's responsibility. This control does not qualify ADC authentication,
detached writers, sandboxed command writes, stable cwd across generations, resume,
or the platform matrix.
