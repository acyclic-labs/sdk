# Shipping acceptance cases

Build `graphcoder` and `harness-mock-model` on the target OS ahead of timing.
Windows MSVC executables must be cross-built on bb2; run Windows suites serially.
ConPTY preparation has no local PowerShell compiler entry point: cross-build
`scripts/graphcoder-conpty.cs` on bb2 and supply its executable as
`E2E_CONPTY_HOST` before running a Windows PTY case.
The Windows process host is the shared native-runtime `acyclic-process-host`
binary. `scripts/windows-test.sh` cross-builds and exports it with the selected
test artifacts; `scripts/windows-runner.py::admitted_host` verifies the complete
source and successful Cargo receipt before its absolute executable is supplied
as `E2E_PROCESS_HOST`. Changed host source requires a matching producer export.
External acceptance
commands run under a Linux subreaper, a macOS session or a Windows Job Object. Normal
exit and suite cancellation drain their service descendants before returning.
Linux external acceptance also leases its fresh fixture directory. After all
descendants exit, the supervisor normally unmounts its FUSE registrations and
checks that their mount IDs have disappeared. Failed retirement fails the
acceptance command; unrelated fixtures and pre-existing mounts are never adopted.
The runner passes that same root as `ACYCLIC_TEST_FIXTURE_ROOT`; installed Pi
fixtures create their service state below it and bind provider imports to the
installed modules before moving provider files outside the repository.
POSIX cleanup requires Python 3 and macOS `ps`; exit waits use Linux pidfds or
macOS kqueue notifications. Separate service process groups stay owned. Shipping GraphCoder processes retain their own recovery
oracle so an external-test cleanup boundary cannot hide orphan-fencing failures.
Install `scripts/graphcoder-pty` with its frozen Bun lockfile and lifecycle scripts disabled.
The terminal package is required for PTY cases and the complete helper suite;
headless selections load only their own runtime dependencies.

```sh
node scripts/graphcoder-e2e/helpers.mjs
# Linux/bb2: real FUSE mount custody, including the process-only negative control.
python3 scripts/graphcoder-e2e/mount.test.py
node scripts/graphcoder-e2e/coverage.mjs
node scripts/graphcoder-e2e/serial.mjs /absolute/graphcoder /absolute/harness-mock-model
```

Use the serial matrix on every OS to avoid concurrent native mounts and PTYs
competing inside a shared deadline. On Windows run it under the fleet's
`Global\AcyclicWindowsTestRun` mutex. Each case suite retains
the ten-second limit, including its filesystem variants. The aggregate receipt
requires every case and variant exactly once; total matrix time is recorded
separately. Run Node helper tests with `--test-concurrency=1` on Windows.
The strict helper launcher also owns its complete process tree. Its active
nine-second budget cancels and drains the scope; cancellation always fails
qualification, including when a late child summary reports success.

Set `E2E_SCRATCH_ROOT` to an absolute owned directory before any Windows
invocation; this lane uses `D:/lane-scratch/e2e-gate/`. Windows has no system-temp
fallback. The old independent `E2E_WORKSPACE_ROOT` and `E2E_NTFS_ROOT` selectors
are rejected. Fixture profiles, temporary files, helper workers, Git and the
mock all use the same scratch ownership boundary. `E2E_OUTPUT` can separately
select a durable evidence directory. Remove the owned scratch after retaining
the reports and retiring every process/mount. Helper scratch is removed at its
completion barrier. The secondary Windows case directory remains a distinct
fixture; two directories on D: do not constitute a ReFS-versus-NTFS proof.

`coverage.json` maps every A1–A26 target to real case identities and names the
owner of remaining scope gaps. Missing rows, unknown identities, duplicate
identities, unmapped cases and recorded gaps fail qualification. A mapped row
does not assert that every part of its target has been proven.

Supplying case IDs after the two executables runs a diagnostic selection. Its
receipt has `qualification: false`; a successful selection never qualifies the
complete platform. The full run requires both passing cases and coverage.
Known product failures retain their assertions and fail normally.

`instructions-and-skills` checks discovery without loading an unselected skill
body. `skill-invocation` selects `/inventory` through headless and real-PTY
composer input, then requires the instruction markers, selected body and expanded
arguments in the actual model request. Both paths use the retained skill catalog
and its pinned body reader. A live inventory that requires a body must select
the skill explicitly; listing available skill metadata does not invoke it.

`matrix.json` embeds each case's complete captures and acknowledged requests.
The identical JSON is published to retained job stdout before writing the export
file. Serial aggregates retain child stdout/stderr even when that child's report
is missing or unreadable. Capture evidence has one publication path, so a failed
fixture/export walk cannot erase it when the build snapshot is released.

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

For each build job, select executables from that job's Cargo JSON and stage them
before running cases:

```sh
node scripts/graphcoder-e2e/artifacts.mjs specification.json /absolute/fresh-job-bundle
```

The specification is a JSON array of `{name, target, test, cargo_json, kind?}` entries.
`target` is the exact Cargo target name, `test` is its `profile.test` boolean,
and `cargo_json` names the producer's `--message-format=json` output. The tool
requires exactly one artifact for every entry and a new destination directory.
`kind` defaults to `executable`; `cdylib` selects one Cargo `cdylib` record and its
single `.so`, `.dylib` or `.dll` filename. Import libraries, debug symbols and
Rust libraries cannot satisfy that selection. Executables and native libraries
share the same copy, hash and manifest admission; callers choose the destination
name (for example, `binding.node`) rather than guessing a target-cache path.
It opens each source once, streams it into the owned bundle and publishes a
manifest with paths and SHA-256 only after all copies complete. Execute those
bundle paths in the same job. A previous job's Cargo path is not an artifact
handoff: target caches can be retired between jobs.

The orphan case waits for an acknowledged HTTP barrier from a real native-tool
descendant, kills the shipping owner and reopens the workspace. It checks both
process identities while the mock's held response remains open. Closing the
mock first would let an unfenced descendant exit and hide the failure.
