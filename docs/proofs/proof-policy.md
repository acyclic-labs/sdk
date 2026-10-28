# Wave-1 proof inventory

`wave1-inventory.json` records the active redesign obligations separately from the historical whole-stack source audit. It is a requirement/evidence inventory, not a declaration that the stack is proved correct. A negative control that detects the old implementation is distinct from a passing proof of the new implementation.

The first implemented suite is `acyclic_stream::conformance::wave1`. Its public `verify` and `verify_case` functions are generic over the production `StreamProvider` trait. Memory, local and hosted callers execute the same assertions. A fresh namespace is mandatory: deterministic keys and retained paths make a second invocation in the same namespace a setup failure.

Run memory and local histories on a Linux host:

```sh
cargo test -p acyclic-stream --features local,conformance --test wave1_keys -- --nocapture --test-threads=1
```

For an isolated HTTP endpoint, provide `ACYCLIC_CONFORMANCE_ENDPOINT`, `ACYCLIC_CONFORMANCE_TOKEN` and `ACYCLIC_CONFORMANCE_NAMESPACE` in the runner's environment, then run:

```sh
cargo run -p acyclic-stream --features http,conformance --example wave1-conformance
```

Optional `ACYCLIC_CONFORMANCE_CASE` selects one exact case name: `append_retry`, `commit_retry`, `absent_zero_tail`, `subtree_fork`, `composed_fork`, `spawn`, `closed_inbox`, `inherited_follow` or `fork_lineage`. An unknown selector fails rather than running zero cases. Each selected history has a nine-second outer deadline. Errors identify case, step, typed provider cause or violated invariant, and recovery command; tokens are never printed. The runner has no memory fallback, public fault-injection endpoint or Cloud service configuration changes. Secrets must be supplied through the build environment's existing credential transport rather than command-line literals.

These fixed histories cover observable conflict-key reuse, stable successful replay/inspection, changed-intent mismatch, record sequence/content and zero-tail absence. Discarding an acknowledgement is a simulated lost result, not a process-crash test. `inspect == None` is asserted only after a definitive conflict; it does not establish that a pending or unknown request has no effect. Lifetime deduplication, real crash/restart, publication concurrency, subtree retention/authorization, WASM boundary and hosted behavior need their own receipts. Do not credit any of those from this suite alone.

The subtree histories also check deep membership, path-segment sibling boundaries, private source/destination writes and pre-commit descendant heads when root/branch appends compose with a fork. They do not establish exclusion, authorization, publication concurrency, resource cost or GC safety.

The new tests fail on b733 production for the intended invariants. Their actual failures are negative controls; they will remain failing regressions until the owning Streams implementation lands. No verify PR should enter the gate before positive implementations and the remaining wave-1 proofs are integrated. Preserve one PR for the lane; never modify a queued head.

Check the manifest, source references and captured negative/Clippy evidence with `python scripts/check-wave1-inventory.py`. Its pass reports artifact integrity only. `--require-qualified` fails on the open obligations; positive receipt validation and the full public-entrypoint/unsafe/mapper CI policy remain a later milestone. Evidence files are captured real output/source, with provenance and hashes in the manifest. The Clippy JSONL retains exact source-identity, diagnostic and completion lines from the full external log. Four baseline Streams warnings remain; zero new-proof diagnostics is not a strict crate cleanliness pass.

The append source review under `../../scripts/tests/fixtures/proof-policy/fs-append-review/` is separate from runtime qualification. It captures the owner's unsigned source and the real rejected first run: five of six append cases passed, the prefix/publication case returned `FileRead(InvalidRange)`, and the mutation was never executed. The reviewed source hashes have not been mapped to that compiled run. Neither those five passes nor a native-work argument on the memory provider qualify persistent/hosted behavior. The new `fs.append-segments` obligation tracks EOF alias fencing, immutable prefix/range semantics, atomic refusals and original-key crash recovery.

Run `python scripts/test-wave1-inventory.py` to exercise the evidence policy against the real captures. Controls alter copies of captured logs/receipts and recompute outer hashes: missing semantic witnesses, zero-case selection, a mismatched executed oracle, an over-budget run and relabelled source-review/mutation credit must still fail. These tests prove only those inventory refusals, not Rust protocol correctness.

Policy tests run on a Linux host, or in the existing PR gate; do not repeat them locally. Captured test inputs live under `scripts/tests/fixtures/proof-policy/`. Their original receipt paths and bytes stay immutable. `scripts/proof_fixture_paths.py` maps the exact historical `docs/proofs/evidence/` prefix to that single physical directory, without falling back to another copy. Product contracts remain under `docs/proofs/`; ordinary lane logs remain external.

## Integration on merged main

This replacement for PR1526 starts at main b6bb4c435b4311c703a4c5eac3d113831fb71480. It preserves the merged canonical nine-case Streams suite, its agent histories and endpoint runner. The only suite additions are the typed HST availability adapter, its existing refusal cases, and reporting before endpoint setup. Historical old-source controls and all thirty complete obligations retain their original unqualified scope. No current-source test or proof run is implied by this integration.

