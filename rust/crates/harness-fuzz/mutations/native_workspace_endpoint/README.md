# Native workspace descriptor admission

`native_workspace_endpoint` calls the production `host_files::discover` through
its validation-only module. Each thread reuses one real canonical workspace;
each input replaces its descriptor, with no retained input or admission history.
The reader, held-root file access, size/type checks, JSON decoder, version and
workspace authority checks are the shipping implementation.

The kept corpus contains generator inputs (empty, supported protocol number,
invalid UTF-8), not external host output. Each invocation uses the same
`Endpoint` and serializer as the original `serve_until` writer to generate an
accepted descriptor, a version selected by the input and a foreign workspace.
Its session UUID also varies with the input. Escaped JSON members derive from
those original writer bytes. Arbitrary input bytes independently reach the
same physical reader. The oracle requires positive admission and checks every
accepted descriptor's exact version and workspace authority; reject-all cannot
pass. Canonicalisation is compared only for lossless typed parses within both
physical byte limits, so duplicate members and oversized inputs retain their
original rejection semantics.

From the repository root:

```sh
cargo run --locked -p acyclic-harness-fuzz --no-default-features --features runtime --bin native_workspace_endpoint -- rust/crates/harness-fuzz/corpus/native_workspace_endpoint -runs=0 -seed=1
```

Apply each retained production mutation separately and run that SAME command
and unchanged oracle. `admit-foreign-version.patch` must fail the admitted
version equality assertion; `admit-foreign-workspace.patch` must fail workspace
equality. A compilation/setup failure is not a detected semantic defect.
Reverse each patch before the positive run.

This target covers descriptor byte admission. It neither dispatches
`HostAction` nor decodes shared command replies. The existing physical symlink,
owner-loss and request/waiter controls remain independent and are retained.
Corpus execution does not establish source branch completeness or installed
host qualification; retain matching-toolchain branch exports separately.

The production path has no added syscalls, barriers, round trips, allocations
or copies. Validation performs a fixed number of keyed file writes and reads
per input, with JSON work proportional to input bytes. It has no wait, poll,
background producer, directory scan or growth in history/agents. Filesystem
setup failures return the original typed error rather than waiting for an
absent producer.
