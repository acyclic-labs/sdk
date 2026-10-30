# acyclic-conformance

Private workspace qualification workloads for public Acyclic provider contracts. Run the relevant suite against your provider implementation before claiming protocol compatibility; this crate is not published to crates.io.

The portable [AgentHost suite](agent-host/README.md) drives the public host API against disposable backend storage, including exact uncertain retry, admission, spawn cuts, guest codec/cancellation and crash/reopen vectors. It is also an independent workspace crate for browser wasm builds and is re-exported as `acyclic_conformance::agent_host`.

The `inference` feature supplies real-capture adapters for the eight `ModelProvider` vectors published at `acyclic_harness_core::model::conformance`: streaming, cancellation, paired tools/results, exact usage, typed refusals and deterministic replay. The public module exposes `Backend`, `verify`, `bench`, `suite!` and the shared `Counted` dispatch wrapper through `counted(provider)`. `inference-qualify` validates complete response families and their provenance; see [composition and qualification](../../../docs/backends/inference.md). This model seam is separate from the customer Inference context/run client. The host suite also includes cached OIDC identity admission and zero-read refusal vectors, with [local Dex qualification](../../../docs/backends/oidc.md).

The published Stream, Objects v1, Machines and Actors crates expose their shared `conformance` features, with `Backend`, `verify`, `bench` and `suite!(factory)`. AgentHost lives in `acyclic_harness::conformance`; the private agent-host crate re-exports that same implementation for existing adapters. Filesystem publishes portable workspace vectors over its existing public seam, while the historical workload taxonomy and proof producers remain available here. Run suites and benchmarks against independently fresh, disposable state. The Objects suite checks the v1 provider API, including durable local reopening; it makes no claim about a separate v2 contract.

Run the maintained workspace checks from the SDK root with `cargo test --workspace --all-features --locked` and `bun test`. The product-specific gate is `cargo test -p acyclic-plugin --locked`; platform qualification builds and validates the release package. The committed conformance vectors and Rust/TypeScript suites are the single maintained source of compatibility evidence.

The same `qualify` binary owns the SDK's Filesystem fixture and diagnostic commands: `fixture`, `roundtrip`, `corpus`, `bench`, `restore-gen`, `mount-smoke`, `mount-smoke2`, `mount-hold`, and `source-probe`. They consume public SDK APIs. The plugin control plane consumes the same public APIs and keeps its end-to-end tests in its own crate.

For a measured Filesystem consumer, run `cargo run --locked --release -p acyclic-fs --example bench-fs -- --writes=64`. Add `--batch` to publish one transaction, or `--barrier` to compare the local durability policy. The benchmark then times warm reads and cold reads after reopening, verifies every file, and emits one JSON measurement. These measurements run on demand.

The first [cross-host baseline comparison](benchmarks/2026-09-18-local-fs.json) records three warm full-flush samples from the preserved pre-lazy revision and this lab branch. This small workload did not establish a reproducible performance shift; continue comparing real consumers and add CPU, memory, and allocation measurements before setting regression thresholds.

The Objects benchmark measures logical current-key publication and ordered, bounded pagination: `cargo run --locked -p acyclic-objects --example bench-objects -- 2000 100`. It verifies every returned key and exact metadata. Public version-listing and snapshot benchmark modes are retired.

The historical filesystem workload corpus remains available to proof producers. The public `acyclic_fs::conformance` suite executes portable workspace semantics and request budgets against memory and durable local provider compositions. Mounted interfaces require their own actual mount qualification. Harness retains report-to-receipt validation; the Objects walkthrough does not produce per-case acceptance receipts. See the [source](https://github.com/acyclic-labs/sdk/tree/main/rust/crates/conformance) and the [protocol directory](https://github.com/acyclic-labs/sdk/tree/main/proto) for the underlying contracts.

The shared native hot-path benchmark runs on demand:

```sh
cargo bench --locked -p acyclic-conformance --features local-hotpath --bench local_hotpath -- --root /path/to/fresh-directory --source "$(git rev-parse HEAD)"
```

Select a fresh directory on the device being measured. The benchmark uses full
durability and one active operation on one executor thread. It calibrates a
dirty in-place flush, cross-thread rendezvous delivery, an uncontended call,
1 KiB copy and BLAKE3 throughput, and a warm 1 KiB read. A separate new-file,
write, flush and directory measurement distinguishes publication from an
in-place barrier. Setup composes actual agent actors, inbox subtrees and volumes
in bounded batches at populations of 1 and 100,000; it is excluded from timing.
The workload explicitly admits this population rather than using the provider's
default capacity ceilings. Population roots execute no model turns.

JSON lines retain executable identity, raw observations in acquisition order,
nearest-rank p50/p99, floor coefficients, and ratios to the sum of calibrated
component medians. Encoding contributes its minimum payload transfer; this is
an optimistic semantic floor, not a measurement of a particular serializer.
The p50 and p99 thresholds are respectively 1.5 and 3 times that same floor.
These are single-operation latency measurements, not throughput or concurrency
claims. Payloads and exact reads are checked outside the measured intervals.
Filesystem reads select an immutable generation; staged writes and dirty
transaction publication are timed separately. Objects PUT ends at stored-byte
admission; the explicit crash-survival flush occurs outside its interval, as
required by the distinct `Blobs::put` and `Blobs::flush` contracts.
Follow parks at the tail before
its append producer starts and measures notification-waker entry through record
delivery; append durability completes concurrently in its own interval.
Missing original execution or completion producers emit explicit
refusal rows instead of fabricated quantiles; a report with refusals is
incomplete.
