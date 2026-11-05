# Native stdin and task admission

The installed plugin and `native_input` target include the same `input.rs`, which
admits stdin JSON and delegates selected task identities to the SDK AgentPath
decoder before any workspace lookup. The authored corpus contains user inputs,
not external command output. Generated valid JSON and original typed AgentPath
values must also be admitted; rejecting everything fails the oracle.

The native actions and command catalog are core contracts. The harness re-exports
the same definitions, while this target selects only `native-input`, without the
workspace runtime. The catalog's parser, installed data and name index have one
implementation in core. Fieldless request variants remain empty structs so their
serde decoders enforce the same unknown-field admission as data-bearing variants.
The resource-query variant has the same wire shape in every feature composition;
its runtime owner refuses unsupported execution. Its two original serializer
cases extend the existing writer matrix after all retained input identities.

The byte-bound property supplies valid JSON below, at and above the existing
65,536-byte hook-frame boundary. Workspace and package commands are EOF-delimited
JSON, with no hook-frame limit. The workspace transport independently admits its
request envelope; a nested completion is not a hook frame. The same pipe oracle
also checks unbounded request decoding against the original JSON, including an
actual serialized near-bound session completion with its claim and parent.
The former spaces-only example was invalid JSON at every
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
The same binary test sends the production-serialized completion through
`__workspace` and checks the original workspace-client refusal in an unowned
workspace. This proves CLI admission and dispatch, not session settlement. Its
second control reinstates the hook cap at that actual CLI call, which must fail
the unchanged workspace-client assertion before restored source passes.
Control `native_stdin_binary` omits actual stdin read-error propagation and must
fail the unchanged cause assertion before exact-restored source passes:

```
python3 rust/crates/harness-fuzz/graphcoder-args-control.py native_stdin_binary
```

This named integration test uses the Cargo-built binary in its original source
snapshot. Relocated artifact selection, installed host loops, other lifecycle
branches and platform acceptance require their own evidence.

At source `acce6c4d25`, the four selected production mutations failed their
unchanged assertions. Exact-restored Linux source replayed 389 retained inputs
(116,884 bytes) in two seconds and passed the named binary test in 0.009 seconds.
All 388 prior input hashes remain unchanged; `action-36` is the original
65,698-byte completion emitted by the existing production serializer oracle.
The corpus manifest retains the precise source and component qualification.
