# Shipping acceptance cases

Build `graphcoder` and `harness-mock-model` on the target OS ahead of timing.
Windows MSVC executables must be cross-built on bb2; run Windows suites serially.
The Windows `owned-process.cs` host must also be cross-compiled on bb2 and
its absolute executable supplied as `E2E_PROCESS_HOST`. External acceptance
commands run under a Linux subreaper, a macOS session or a Windows Job Object. Normal
exit and suite cancellation drain their service descendants before returning.
POSIX cleanup requires Python 3 and macOS `ps`; exit waits use Linux pidfds or
macOS kqueue notifications. Separate service process groups stay owned. Shipping GraphCoder processes retain their own recovery
oracle so an external-test cleanup boundary cannot hide orphan-fencing failures.
Install `scripts/graphcoder-pty` with its frozen Bun lockfile and lifecycle scripts disabled.

```sh
node --test scripts/graphcoder-e2e/*.test.mjs
node scripts/graphcoder-e2e/coverage.mjs
node scripts/graphcoder-e2e/serial.mjs /absolute/graphcoder /absolute/harness-mock-model
```

Use the serial matrix on every OS to avoid concurrent native mounts and PTYs
competing inside a shared deadline. On Windows run it under the fleet's
`Global\AcyclicWindowsTestRun` mutex. Each case suite retains
the ten-second limit, including its filesystem variants. The aggregate receipt
requires every case and variant exactly once; total matrix time is recorded
separately. Run Node helper tests with `--test-concurrency=1` on Windows.

`coverage.json` maps every A1–A26 target to real case identities and names the
owner of remaining scope gaps. Missing rows, unknown identities, duplicate
identities, unmapped cases and recorded gaps fail qualification. A mapped row
does not assert that every part of its target has been proven.

Supplying case IDs after the two executables runs a diagnostic selection. Its
receipt has `qualification: false`; a successful selection never qualifies the
complete platform. The full run requires both passing cases and coverage.
Known product failures retain their assertions and fail normally.

Installed-host and component acceptance cases consume prebuilt executables:

| Environment variable | Artifact |
| --- | --- |
| `E2E_PLUGIN_TEST_BINARY` | `acyclic-plugin` integration test executable for `plugin/tests/e2e.rs` |
| `E2E_PLUGIN_BINARY` | Matching normal `acyclic` executable |
| `E2E_PROVIDER_TEST_BINARY` | `acyclic-harness-providers` library test executable |
| `E2E_BUN` | Absolute Bun executable, with the pinned Pi test dependencies prepared |

The installed Codex and Claude tests select their actual hosts using their
existing fixture contracts (`ACYCLIC_E2E_CODEX` / `ACYCLIC_E2E_CLAUDE`, or PATH).
They must use fixture settings. The runner supplies isolated HOME, USERPROFILE,
CODEX_HOME and application-data directories and removes provider API key
variables. Do not supply real credentials. Missing binaries, empty selections,
ignored tests and skipped tests fail acceptance. The alloy component case does
not qualify mixed-provider shipping reasoning; that gap remains in the manifest.

The orphan case waits for an acknowledged HTTP barrier from a real native-tool
descendant, kills the shipping owner and reopens the workspace. It checks both
process identities while the mock's held response remains open. Closing the
mock first would let an unfenced descendant exit and hide the failure.
