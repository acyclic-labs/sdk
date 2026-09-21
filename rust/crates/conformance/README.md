# acyclic-conformance

Reusable black-box qualification workloads for public Acyclic provider contracts. Run the relevant suite against your provider implementation before claiming protocol compatibility.

```sh
cargo add acyclic-conformance --dev
```

The crate exposes Filesystem workload vectors, a minimal Filesystem smoke check, canonical Objects and Stream provider suites, a Machines lifecycle suite, and machine-readable Harness reports and qualification receipts. Run provider suites against fresh, disposable state; their resources and idempotency identities are deliberately retained for exact replay checks.

Run the maintained workspace checks from the SDK root with `cargo test --workspace --all-features --locked` and `bun test`. The product-specific gate is `cargo test -p acyclic-labs-plugin --locked`; platform qualification builds and validates the release package. The committed conformance vectors and Rust/TypeScript suites are the single maintained source of compatibility evidence.

The same `qualify` binary owns the SDK's Filesystem fixture and diagnostic commands: `fixture`, `roundtrip`, `corpus`, `bench`, `restore-gen`, `mount-smoke`, `mount-smoke2`, `mount-hold`, and `source-probe`. They consume public SDK APIs. The plugin control plane consumes the same public APIs and keeps its end-to-end tests in its own crate.

For a measured Filesystem consumer, run `cargo run --release -p acyclic-conformance --features local-runner --bin bench-fs -- --writes=64` from `rust/`. Add `--batch` to publish one transaction, or `--barrier` to compare the local durability policy. The benchmark verifies the resulting files after timing and emits one JSON measurement. The combined gate checks that all four modes succeed and reports their timings; it does not yet impose a performance threshold.

The first [cross-host baseline comparison](benchmarks/2026-09-18-local-fs.json) records three warm full-flush samples from the preserved pre-lazy revision and this lab branch. This small workload did not establish a reproducible performance shift; continue comparing real consumers and add CPU, memory, and allocation measurements before setting regression thresholds.

The filesystem workload corpus is not yet an executable backend conformance runner, and Harness is currently the only family with report-to-receipt validation. The exported Objects inventory is a checklist, not a per-case execution receipt. Do not interpret a passing smoke check as full Filesystem qualification. See the [API](https://docs.rs/acyclic-conformance/latest/acyclic_conformance/) and the [protocol directory](https://github.com/acyclic-labs/sdk/tree/main/proto) for the underlying contracts.
