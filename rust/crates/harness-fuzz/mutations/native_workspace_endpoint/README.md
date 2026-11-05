# Native workspace descriptor admission

`native_workspace_endpoint` composes production `host_files::discover` and
`Endpoint::open_roots` through its validation-only module, as `Bridge::open`
does before creating an observer. Each thread reuses one real canonical workspace;
each input replaces its descriptor, with no retained input or admission history.
The reader, held-root file access, size/type checks, JSON decoder, version and
workspace authority and original directory identity checks are the shipping
implementation. Protocol v4 binds both directories to the live owner's held roots.
The version header is selected before decoding that version's directory fields;
unsupported versions are refused even when they omit the current fields.

The kept corpus contains generator inputs (empty, protocol numbers, invalid
UTF-8) and an actual `serve_until` descriptor captured by the original native
owner integration. Its workspace path is normalized as recorded in
`corpus/SOURCES.tsv`; the captured session and both identities are unchanged.
Each invocation uses the same
`Endpoint::new` and serializer as the original `serve_until` writer to generate an
accepted descriptor, a version selected by the input, a foreign workspace and
each foreign directory identity. These identities come from actual held roots.
Its session UUID also varies with the input. Escaped JSON members derive from
those original writer bytes. Arbitrary input bytes independently reach the
same physical reader. The oracle requires positive admission and checks every
accepted descriptor's exact version, workspace and both original roots; reject-all cannot
pass. Canonicalisation is compared only for lossless typed parses within both
physical byte limits, so duplicate members and oversized inputs retain their
original rejection semantics.
Foreign-version inputs also derive the preceding descriptor shape by removing
both directory fields from those writer bytes. This checks protocol selection
without introducing a decoder for a preceding version.

From the repository root on Linux x86_64, isolate target coverage from build
scripts and proc macros by selecting the target explicitly:

```sh
RUSTFLAGS="${RUSTFLAGS:-} -Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=4 -Cllvm-args=-sanitizer-coverage-inline-8bit-counters -Cllvm-args=-sanitizer-coverage-pc-table -Cllvm-args=-sanitizer-coverage-trace-compares" cargo run --locked --target x86_64-unknown-linux-gnu -p acyclic-harness-fuzz --no-default-features --features runtime --bin native_workspace_endpoint -- rust/crates/harness-fuzz/corpus/native_workspace_endpoint -runs=0 -seed=1
```

Apply each retained production mutation separately and run that SAME command
and unchanged oracle. `admit-foreign-version.patch` must fail the admitted
version equality assertion; `admit-foreign-workspace.patch` must fail workspace
equality; `admit-foreign-roots.patch` must fail original directory identity
equality; `defer-version-admission.patch` must fail foreign-version refusal
without current directory fields. A compilation/setup failure is not a detected semantic defect.
Reverse each patch before the positive run.

This target covers descriptor byte admission. It neither dispatches
`HostAction` nor decodes shared command replies. The existing physical symlink,
owner-loss and request/waiter controls remain independent and are retained.
Corpus execution does not establish source branch completeness or installed
host qualification; retain matching-toolchain branch exports separately.

Cold admission reuses its two required directory opens and cached identities,
with no added syscalls, barriers or round trips. The descriptor adds 32 fixed
bytes of identity to its existing JSON serialization; its backing storage grows
with those encoded bytes. The admission guard compares 32 bytes without
allocating. Retained connections pay no added request cost. Protocol selection adds
a version-header pass over the already-read JSON before its typed payload
decode, without materializing ignored fields. This is the work that preserves
typed version refusal before interpreting the current payload. Validation performs
a fixed number of keyed file writes, reads and root opens per input, with JSON
work proportional to input bytes. It has no wait, poll,
background producer, directory scan or growth in history/agents. Filesystem
setup failures return the original typed error rather than waiting for an
absent producer.
