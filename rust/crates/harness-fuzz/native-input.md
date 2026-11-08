# Native stdin and task admission

The installed plugin and `native_input` target include the same `input.rs`, which
admits stdin JSON and delegates selected task identities to the SDK AgentPath
decoder before any workspace lookup. The authored corpus contains user inputs,
not external command output. Generated valid JSON and original typed AgentPath
values must also be admitted; rejecting everything fails the oracle.

The byte-bound property supplies valid JSON below, at and above the existing
65,536-byte boundary. The former spaces-only example was invalid JSON at every
size and could not distinguish a removed bound check. The kept production guard
mutation must fail the unchanged bound assertion. That subsumed example is
deleted; real binary, OS pipe and lifecycle controls are separate.

```
python3 rust/crates/harness-fuzz/graphcoder-args-control.py native_input
```

The same target calls the production command catalog decoder with the installed
`commands.json` and derived caller inputs. It checks bounded admission, normalized
names and aliases, descriptor preservation, argument shapes, confirmation policy,
duplicate names, invalid descriptions and unknown fields. Control 7 removes the
actual argument-shape guard; its unchanged refusal assertion must detect the
mutation before the restored catalog corpus can pass.

The stdin oracle uses a real OS pipe, EOF from its producer, fragmented writes and
a write-only file's original OS read error. It does not execute a workspace or
host action. Full binary dispatch, owner effects, installed host stdout and native
settings remain separate obligations. Corpus replay is not complete source branch
coverage or a new fuzz campaign. Each manifest records only its qualified source;
the catalog extension requires its own mutation and restored-corpus result.

The real binary test
`native_input::native_stdin_failures_stop_before_workspace_effects` passes the
same unreadable file handle as stdin to `__workspace`, `__install-package` and
`__hook codex Stop`. It compares the original OS cause with stderr or the hook
refusal, checks process retirement and checks that workspace and settings effects
have not occurred. It uses the existing executable selection and process owner.
On Unix, stdin is an opened directory: Rust standard input deliberately maps a
write-only descriptor's EBADF to EOF, which would test JSON truncation instead.
Windows uses a write-only file's access-denied read error.
Control `native_stdin_binary` omits actual stdin read-error propagation and must
fail the unchanged cause assertion before exact-restored source passes:

```
python3 rust/crates/harness-fuzz/graphcoder-args-control.py native_stdin_binary
```

This named integration test uses the Cargo-built binary in its original source
snapshot. Relocated artifact selection, installed host loops, other lifecycle
branches and platform acceptance require their own evidence.

The recorded Antigravity `PreInvocation` input must select `ToolBoundary`, the
same shared capture operation used after tools. Its awaited native admission
can therefore refuse a failed capture even when the host ignored an earlier
`PostToolUse` failure. `native-input-agy-admission.patch` restores the former
attach-only mapping; the unchanged `native_input` oracle must reject it.
This checks routing and input preservation. Successful capture and native
termination after a capture failure require the installed-host control too.

The external AGY composition uses that same admitted-directory replacement,
the shipping plugin installer, the original workspace owner and AGY 1.5.2's
native `PreInvocation`. It requires zero provider requests after failed capture.
Restoring the original directory must admit one request, answered with HTTP 401
without purchasing a model. It uses the shared process owner and exclusive
fixture profiles, including a profile path containing spaces:

```
ACYCLIC_TEST_BINARY=/absolute/acyclic E2E_GRAPHCODER=/absolute/graphcoder E2E_AGY=/absolute/agy E2E_MOCK_MODEL=/absolute/harness-mock-model node --test --test-concurrency=1 plugin/tests/hosts/agy-capture-admission.test.mjs
```

Windows also requires the shared `E2E_PROCESS_HOST` and `E2E_SCRATCH_ROOT`.
The native host produces the callback inputs and its terminal. This control
checks admission and refusal, then uses the shared deterministic model producer
to exercise native writes and model-issued MCP coordination to a writing child.
It does not qualify background process retirement at a tool boundary, ADC
authentication, resume or reusable cwd across generations.
Repeated native error steps fail this control immediately and retire its
process before restoring the directory. This distinguishes refusal from a
Stop response that continually re-enters the same failed admission. That
native Stop progress contract remains unqualified until this control runs.
