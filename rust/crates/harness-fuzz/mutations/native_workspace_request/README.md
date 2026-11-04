# Native request envelope admission and retirement

`native_workspace_request` includes the original `host_files` validation module.
It writes real request projections using the SAME `Request` serde as
`request_operation`, then calls production `Served::observe`, `start`, `answer`
and withdrawal through physical file removal. No second decoder or transition
model is used. The shared `WorkspaceInput` owns one reusable canonical workspace
and held status directory; every input retires all request and reply state.

The corpus consists of authored generator inputs, not external host captures:
empty selects completion followed by reply retirement; byte `1` withdraws before
dispatch; byte `2` withdraws while running. Input bytes also select session and
operation identities and independently reach the original file reader/decoder.
Each invocation generates one accepted request and two distinct foreign
identities with the shipping writer. Reject-all cannot pass. Repeated keyed
notifications and replacement bytes must not dispatch accepted work again.

From the repository root:

```sh
cargo run --locked -p acyclic-harness-fuzz --no-default-features --features runtime --bin native_workspace_request -- rust/crates/harness-fuzz/corpus/native_workspace_request -runs=0 -seed=1
```

Apply each retained production patch separately and run this SAME command with
the unchanged oracle and corpus. Session/operation guard omissions must fail
identity equality, duplicate admission must fail dispatch cardinality, and
withdrawn reply publication must fail the physical reply-presence assertion.
Compilation/setup failures do not count as detected defects. Reverse each patch
before another mutation or the positive run.

This target qualifies envelope identity and the original keyed queue's
retirement transitions. `HostAction` variant semantics, shared command-result
codec semantics, notification overflow, physical waiter/owner loss, symlink
authority and installed host interpretation remain independent obligations.
Existing physical controls are retained. Corpus execution is not source branch
coverage; matching-toolchain branch exports must identify uncovered production
branches separately.

Production adds zero syscalls, barriers, round trips, allocations or copies.
Validation uses four fixed cases per input with at most two initial keyed
notifications each; reads/writes and durable retirement/publication use the
original filesystem primitives. Byte work scales with input size; keyed state
has at most one operation, no history growth, directory scan or polling. No
wait is introduced: every transition is driven by its file event or dispatched
result, and setup/I/O failures return the original typed error.
