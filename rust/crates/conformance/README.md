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
through the shared root publisher at populations of 1, 1,000 and 100,000;
it is excluded from timing.
The workload explicitly admits this population rather than using the provider's
default capacity ceilings. The shared runtime settles each root's initial
replay before sampling, and setup checks the exact registration count and an
empty ready queue. The idle oracle must first reject the actual queued
population. Population roots execute no model turns; setup refuses any request
for model execution.

JSON lines retain executable identity, raw observations in acquisition order,
nearest-rank p50/p99, floor coefficients, and ratios to the sum of calibrated
component medians. Encoding contributes its minimum payload transfer; this is
an optimistic semantic floor, not a measurement of a particular serializer.
The p50 and p99 thresholds are respectively 1.5 and 3 times that same floor.
These are single-operation latency measurements, not throughput or concurrency
claims. Payloads and exact reads are checked outside the measured intervals.
Root setup and session samples share the same exact requested-agent receipt
oracle; a real receipt checked against another requested agent must fail.
Filesystem reads select an immutable generation; staged writes and dirty
transaction publication are timed separately. Objects PUT ends at stored-byte
admission; the explicit crash-survival flush occurs outside its interval, as
required by the distinct `Blobs::put` and `Blobs::flush` contracts.
Actor entry uses the population's original Handler, wrapped through the public
host composition seam. Each sample commits a fresh cancellation input and its
activation together, then measures `event-drain-to-handler-entry` from explicit
resident delivery admission to `Handler::on_record`. Input publication is
outside that interval. `event-drain-completion` separately includes checkpoint
durability and finite-input exhaustion; neither interval claims to measure a
continuous activation-follow notification. Entry identity and exactly one
invocation are checked, along with the consumed cursor and idle checkpoint.
The delivery scope closes the observing channel on completion, error or
cancellation. A dropped scope must return typed producer loss, and a real
entry checked against another actor, input cursor, subscription or admitted epoch
must be rejected. The observer snapshots the original public provider ledger
and current-thread allocation diagnostics at handler entry. These prefix counts
exclude later handler and checkpoint work; a real completed drain must contain
additional data publication, and substituting its ledger for the entry prefix
must fail. Physical and cross-thread counts remain missing.
Warm Actor reads borrow the admitted value through `ActorStateBinding::get`,
without a transaction or handle clone, and charge no payload-copy floor.
Their `borrowed-resident-read` label distinguishes them from transaction reads.
The durable Actor row publishes a 128-byte metadata mutation, a 128-byte
checkpoint and the original committed input cursor together, with preparation
and encoding inside its interval. Input publication is excluded. Its floor
charges 256 payload bytes and one barrier. The returned confirmed snapshot
feeds warm reads and the next sample without a post-claim reload. Outside
timing, exact tuple readback and one-commit/zero-control accounting are checked;
controls reject the unconsumed cut and a mismatched original input.
Follow parks at the tail before
its append producer starts and measures notification-waker entry through record
delivery; append durability completes concurrently in its own interval.
Missing original execution or completion producers emit explicit
refusal rows instead of fabricated quantiles; a report with refusals is
incomplete.

The same operation intervals supply the shared `acyclic_test_support::scale`
work report. Each metric retains its exact sum, maximum and missing-observation
count. Comparisons use completed observations rather than resident population
as the denominator, so fixed sample counts cannot hide a population scan.
Qualification requires every requested operation at all three populations,
complete original-work coverage, flat mean and maximum cost, flat retained
bytes per idle agent, and no additional per-agent threads or timers.

The public provider ledger counts control calls at that boundary; it does not
infer syscalls or transport requests. Current-thread allocation diagnostics
exclude native workers and therefore do not qualify allocation coverage.
Concurrent append/follow work remains unobserved until it can be attributed to
its original operation. Unobserved physical syscalls, locks, I/O, allocations,
visits and idle resources stay missing, and make counting qualification fail.
Run the deterministic observer controls with
`cargo test --locked -p acyclic-test-support scale::tests`. They execute an
injected scan, real per-agent threads, virtual timer registrations and a lost
event producer; these controls validate the observer, not production liveness.

Select core operation names with `--operations`, for example
`--operations 'actors.create,actors.durable_write,actors.warm_read,actors.wake'`.
The default selects all 17 rows. Selection uses the same drivers, floor
calibration and population setup; unrelated operation families are skipped.
Dependent fixture operations remain outside the selected intervals. Reports
name the requested operation set, and a subset never claims full core coverage.
`--populations '1,1000'` selects a subset of the qualification populations through
the same setup and drivers. The default is `1,1000,100000`. Reports retain the
selected population set; incomplete sets cannot qualify the three-population
counting obligation.
N names the initially settled dormant root population. Creation samples retain
new durable identities; session samples also add resident roots. The cohort's
namespace therefore grows across its fixed acquisition-order samples. Population
labels do not assert constant cardinality throughout creation measurements.
`--setup-credit` bounds concurrent original root preparation and submission
during population setup (default1). Each root retains its own intent, sealed
request and publication holds; independent lifecycles can share the provider's
existing group sync. Root-publication completion is reported before replay.
All submissions and initial replay settle before serial operation samples begin.

The native floor report also retains a timer-only observation control. Ratios
include sampler cost; rows flag targets below the measured timestamp resolution.
The control never inflates the semantic floor or subtracts quantiles, and its
timestamp pair excludes the additional cost of async polling and observers.
Call and copy calibration use4096-operation batches; hashing uses64-operation
batches. Their observations and quantiles are normalized batch averages, so
their calibration p99 does not describe an individual call's tail. Operation
rows retain individually timed samples without batch averaging.
Zero-duration samples are retained when both timestamps occupy the same clock
tick. Resolution flags also use the smallest positive timer-only observation;
an entirely unresolved timer or a zero-median semantic floor refuses calibration.
Requested HEAD and LIST results are data. The shared observer excludes exactly
the original query dispatch from control overhead, retains every provider class,
and checks that an additional real HEAD remains one control request.
