# Installed GraphCoder qualification lanes

The installed package lanes use the production bridge entrypoint in
`scripts/graphcoder-production-entrypoint.mjs`. It dynamically loads the
installed package from `GRAPHCODER_PACKAGE_ROOT`, starts the host supplied
JSON-lines bridge named by `GRAPHCODER_BRIDGE_EXECUTABLE`, and passes terminal
commands through `runCliWithTransport`. The bridge receives only the explicit
`GRAPHCODER_BRIDGE_ENV_JSON` object; credentials from the invoking process are
not inherited.

For the packaged mock stage, generate a source-bound configuration with
`scripts/graphcoder-mock-stage-config.mjs`. It binds the installed package,
mock bridge, and production entrypoint to one source commit/tree and records
the fixture in the explicit bridge environment. This validates package
loading, process framing, dispatcher correlation, and terminal behavior; it
does not qualify the local Harness, filesystem effects, native runtime, or
PTY acceptance rows:

```json
{
  "env": {
    "GRAPHCODER_BRIDGE_ARGS_JSON": "[\"scripts/graphcoder-mock-bridge.mjs\"]",
    "GRAPHCODER_BRIDGE_ENV_JSON": "{\"GRAPHCODER_PACKAGE_ROOT\":\"<installed-consumer>/node_modules/@acyclic-labs/graphcoder\",\"GRAPHCODER_MOCK_FIXTURE\":\"deterministic\"}"
  }
}
```

Run the generated configuration with
`node scripts/graphcoder-qualification-suite.mjs capture CONFIG.json`. Its
artifact list includes the package archive plus both source drivers, so a
receipt cannot attach a package result to a different bridge or entrypoint.
The command sequence contains no `--fixture` flag; the bridge fixture is
explicit in `GRAPHCODER_BRIDGE_ENV_JSON` and appears in the fixture activity
record.

The production bridge must replace this helper before native or PTY evidence
can be bound to a receipt. Its runtime must exercise the durable local Harness
and real filesystem effects through the typed host boundary.

Build and install the GraphCoder package into a clean consumer directory, then
set these values in the suite capture configuration:

```json
{
  "command": {
    "executable": "node",
    "args": [
      "scripts/graphcoder-production-entrypoint.mjs",
      "start inspect the repository",
      "list",
      "activity",
      "messages",
      "approvals",
      "approve {{approval_id}} yes",
      "changes",
      "diff README.md",
      "file README.md",
      "writeback {{writeback_operation_id}} {{workspace_generation}} yes",
      "cancel",
      "resume {{session_id}}",
      "cancel"
    ],
    "cwd": "<sdk-root>",
    "env": {
      "GRAPHCODER_PACKAGE_ROOT": "<installed-consumer>/node_modules/@acyclic-labs/graphcoder",
      "GRAPHCODER_BRIDGE_EXECUTABLE": "<host-bridge-executable>",
      "GRAPHCODER_BRIDGE_ARGS_JSON": "[]",
      "GRAPHCODER_BRIDGE_ENV_JSON": "{}",
      "GRAPHCODER_BRIDGE_CWD": "<qualified-worktree>"
    }
  }
}
```

The `{{...}}` arguments are substituted by the production entrypoint after the
preceding JSON response supplies the session, approval, operation, and
workspace-generation identities. For interactive Windows qualification, use
`scripts/graphcoder-production-pty.mjs` as the executable argument instead:

```text
node scripts/graphcoder-production-pty.mjs "start inspect the repository" "activity" "messages" "approvals" "approve {{approval_id}} yes" "changes" "diff README.md" "file README.md" "writeback {{writeback_operation_id}} {{workspace_generation}} yes" "cancel" "resume {{session_id}}" "cancel"
```

Capture each lane with
`node scripts/graphcoder-qualification-suite.mjs capture CONFIG.json`. The
configuration must list the installed GraphCoder package archive and every
bridge executable consumed by the command, each with its exact SHA-256 digest,
qualified source commit, source tree, build ID, build timestamp, and `fresh:
true`. Every artifact must be freshly built for the qualified source and the
runner requires `fresh: true`; each configuration also has a positive
`timeout_ms` (120000 ms by default) that bounds the child process. The capture
runner checks the declared digest against the bytes before dispatch, records
the qualified source commit/tree in the suite descriptor, then checks the
digest again after the suite. Use separate
suite IDs and output directories for headless and PTY runs. Reusing an output
directory after a crash is rejected by the runner's exclusive descriptor and
transcript creation; resume with a new suite ID after confirming the previous
record's failed or uncertain outcome.

These lanes deliberately do not accept `--fixture`. The mock fixture remains a
separate explicit suite and cannot satisfy native, PTY, or package evidence.

For installed transport fault qualification, run
`node scripts/graphcoder-installed-transport-faults.mjs` with
`GRAPHCODER_PACKAGE_ROOT` set to the extracted package. The driver resolves
`@acyclic-labs/graphcoder/node` through the package export map, then exercises
malformed framing, an unmatched response identity, and a late response after
cancellation. Every case requires the typed transport failure, the expected
diagnostic, and observed child-process cleanup within a bounded deadline.
