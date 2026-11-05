# Native claim decoder validation

The `native_claim_record` target includes the production claim codec directly.
Its shared oracle and original writer corpus live under
`rust/crates/native-runtime/tests/claim_codec`, inside the published crate.
The fuzzer and capture example include this same oracle from there.
It checks arbitrary bytes, writer round trips without a whole-record ceiling, torn writes,
checksum rejection, domain separation, all five field lengths and trailing data.
Checksum-valid short bodies and invalid PID/token widths exercise grammar refusal
without relying on a random checksum collision.
Grammar mutations use the production writer and checksum, not another parser.

Capture the initial corpus and run the bounded instrumented control with:

```sh
python3 rust/fuzz/native-codec-control.py claim /tmp/native-claim-control
```

The control captures four seed files through `Record::encode`, records their
hashes, then disables the production checksum check in a private checkout.
The unchanged target must report
`claim checksum bypassed`; compilation failures do not satisfy this negative
control. The restored source then runs 10,000 deterministic fuzz iterations.
Original source bytes are restored even on failure. Run this command
only in a disposable checkout with no concurrent edits. The report retains
commands, source/oracle hashes and original logs. The bounded run's instrumented
edge counts and original codec coverage are retained, not an exhaustive proof
or OS ownership test. Package ordering changes by Cargo are checked for semantic
equality, then the original lockfile bytes are restored. The
nightly-2026-07-01 toolchain, its llvm-tools component and cargo-fuzz must already
be installed. The coverage export currently selects Linux x86_64 tools.

The kept corpus files below were captured by `capture_native_claim.rs` calling
this production writer at commit `1f648ee7e9520c3b519c3913e35431118cbd97a2`.
These are SDK-owned format outputs with deliberately varied field bytes, not
external-tool responses or evidence of an admitted OS process.

| Seed | Bytes | SHA-256 |
| --- | ---: | --- |
| empty-record | 57 | 4b6ff9434f4ba1afbfd182b14e6c3df0d30e35709d2e0601b779710b29ee6174 |
| native-bytes | 70 | f0073edf49f0d5db735df34016dee2986a41c4b9a9177e889a13e5382f429a1d |
| at-bound | 4096 | e305bedb50e4c1f83f6a5da59b873ec9f20066aab241701b8d25ec18b0994961 |
| past-bound | 4097 | 350469ea2abda9aa05a144747fb0141f35d4333c6b8e48f3d79559a14e4ea541 |

The historical `at-bound` and `past-bound` names identify unchanged 4096-byte
and 4097-byte writer captures; both are valid records. They impose no limit.

The ordinary `acyclic-native-runtime` integration test replays all four retained
corpus files through the same `check` function used by the fuzzer:

```sh
cargo test --locked -p acyclic-native-runtime --test native_claim_record
```

It also checks every truncated prefix of each retained writer seed, preserving
the exhaustive routine coverage without adding quadratic work to fuzz inputs.
The capture producer checks these prefixes once per seed, and routine replay
also verifies that the current production writer emits the retained bytes.
The producer remains available when Cargo compiles the example with `cfg(test)`.

The same source-defect control can qualify this routine test without repeating
the fuzz campaign: append `--routine` to the Python command above. It first
requires the checksum defect to fail the ordinary test, restores the production
codec, then requires exactly one passing test.

This shared oracle replaces the three former path, bound and truncation
examples in routine validation as well as fuzzing. The claim protocol's native
and concurrency controls remain necessary:
codec fuzzing establishes no process identity, lock release or OS retirement.
