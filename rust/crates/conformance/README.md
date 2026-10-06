# acyclic-conformance

Private workspace qualification workloads for public Acyclic provider contracts. Run the relevant suite against your provider implementation before claiming protocol compatibility; this crate is not published to crates.io.

The workspace crate exposes Filesystem workload vectors, a minimal Filesystem smoke check, canonical logical Objects v2 and Stream v2 provider suites, a Machines lifecycle suite, and machine-readable Harness reports and qualification receipts. Run provider suites against fresh, disposable state; their resources and idempotency identities are deliberately retained for exact replay checks. The Objects suite uses a deterministic fixture that makes completed mutations visible before subsequent reads; it does not qualify live eventual consistency. The durable Objects local-runner case still exercises the legacy v1 journal while that engine is being migrated, and is not v2 acceptance.

Run the maintained workspace checks from the SDK root with `cargo test --workspace --all-features --locked` and `bun test`. The product-specific gate is `cargo test -p acyclic-plugin --locked`; platform qualification builds and validates the release package. The committed conformance vectors and Rust/TypeScript suites are the single maintained source of compatibility evidence.

The same `qualify` binary owns the SDK's Filesystem fixture and diagnostic commands: `fixture`, `roundtrip`, `corpus`, `bench`, `restore-gen`, `mount-smoke`, `mount-smoke2`, `mount-hold`, and `source-probe`. They consume public SDK APIs. The plugin control plane consumes the same public APIs and keeps its end-to-end tests in its own crate.

For a measured Filesystem consumer, run `cargo run --release -p acyclic-conformance --features local-runner --bin bench-fs -- --writes=64` from `rust/`. Add `--batch` to publish one transaction, or `--barrier` to compare the local durability policy. The benchmark verifies the resulting files after timing and emits one JSON measurement. The combined gate checks that all four modes succeed and reports their timings; performance thresholds are maintained separately from this command.

The first [cross-host baseline comparison](benchmarks/2026-09-18-local-fs.json) records three warm full-flush samples from the preserved pre-lazy revision and this lab branch. This small workload did not establish a reproducible performance shift; continue comparing real consumers and add CPU, memory, and allocation measurements before setting regression thresholds.

The Objects benchmark measures logical current-key publication and ordered, bounded pagination: `cargo run -p acyclic-conformance --features local-runner --bin bench-objects -- 2000 100`. It verifies every returned key and exact metadata. Public version-listing and snapshot benchmark modes are retired.

The Filesystem workload corpus exercises provider workloads and emits its measured output. Harness is the family with report-to-receipt validation. The Objects provider walkthrough executes its provider scenarios without per-case acceptance receipts. The [source](https://github.com/acyclic-labs/sdk/tree/main/rust/crates/conformance) and [protocol directory](https://github.com/acyclic-labs/sdk/tree/main/proto) contain the underlying contracts.
