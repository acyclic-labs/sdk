# Conformance runner protocol v1

Language and service runners execute the locked vectors in
`conformance/vectors/core.json` and emit a JSON report conforming to the packaged
[`runner-report.schema.json`](../../rust/crates/conformance/schemas/runner-report.schema.json).
Reports bind the exact suite bytes, implementation
source revision, artifact bytes, runner, Harness descriptor, capability profile,
and one evidence digest for every canonical Harness case. Cases must appear exactly once and in
suite order; a missing, additional, duplicated, or reordered case is invalid.
The JSON schemas validate portable document shape. The validator command is the
semantic authority for locked identity, ordering, and complete-suite rules.

Digest values use `blake3:` followed by 64 lowercase hexadecimal characters.
The suite digest is over the exact bytes of `conformance/vectors/core.json`.
Capability names use Unicode code-point order and must be unique. Evidence bytes
are runner-defined but must be stable for identical execution evidence and must
never contain credentials or customer data. Duplicate JSON object keys are
invalid.

Validate a report and emit a deterministic receipt with:

```sh
cargo run --locked -p acyclic-conformance --bin harness-conformance -- report.json
```

The command reads standard input when no path is supplied. It prints a receipt
conforming to the packaged
[`qualification-receipt.schema.json`](../../rust/crates/conformance/schemas/qualification-receipt.schema.json).
Structurally invalid reports
and well-formed reports containing any failed or skipped case exit nonzero. A
receipt has `qualified: true` only when every locked Harness case passed. The
receipt digest binds the exact, duplicate-free input report bytes.

Receipts are deterministic local validator output, not standalone signatures.
A release gate must retain the report and receipt inside trusted CI provenance
that identifies the executed command and independently hashes the same artifact.
