# Harness scenario substrates

From the SDK root:

```sh
cargo run --locked -p acyclic-harness-conformance --bin scenarios -- --substrate in-process --report out.json
cargo run --locked -p acyclic-harness-conformance --bin scenarios -- --substrate http://127.0.0.1:8080/contract --filter core/ --report out.json
```

Build on bb2. The runtime budget is measured separately from Cargo compilation.
`--corpus DIR` selects an exported copy of `harness/tests/scenarios`. Defaults
are merged by the existing corpus loader; no case JSON needs changing.
`--substrate local` runs the same portable executor fixtures directly, providing
a baseline for the Actors adapter. The existing `cargo test -p acyclic-harness
--test main` path and its native drivers are unchanged.

The report has a sorted `records` array with one `case`, `status`, `reason` and
`requires` entry per discovered JSON case, plus pass/fail/skip counts. Defaults
files are configuration, not cases. Filtering produces explicit skips. No wall
clock timestamps or random runtime identifiers appear in passing reports. Exit
status is zero only with at least one pass and no failures; a filter matching
nothing cannot qualify a host.

At the initial corpus revision, the 847 JSON files comprise 60 inherited-default
files and 787 executable cases, matching the existing discovery rule. The
reference qualifies 259 executor cases and explicitly skips 528 native-fixture
cases with the capabilities below. Defaults still apply to their descendant
cases; they are not executable scenarios.

## External hosts

Implement #1359's `ActorRuntime` and this crate's `substrate::Volume`, then use
`substrate::Actors` with the host's explicit bindings. Install the identical
`HarnessPackage`. Its pinned deterministic handler emits a
`harness.scenario.compute` command; bind that command to `FixtureEffects` with
an explicit `Providers` bundle (or an equivalent executor using
`observation::collect_on`). The effect runs the stock
executor with a scripted model and fixture-owned files. The package checkpoints
the resulting immutable fs reference. The runner supplies fixture inputs and
file paths to inspect, never expected answers. Assertions run on the client.

The runner uses only public Actors admission/recovery and fs publications. The
runtime's inbox, checkpoint and outbox use Streams as specified in #1359. The
reference host uses `InProcessRuntime` and disk-backed `LocalStream`; memory
Streams cannot authorize durable outbox dispatch. Fixture fs durability lasts
for its provider lifetime. Results reconcile by stable command identity.
Executor journals use that same host Streams provider; executor files use the
supplied fs providers. Hosts must not substitute fixture-private memory stores
for the providers being qualified.
This package qualifies operation-level executor hosting; it does not migrate
GraphCoder's native per-step scheduler to Actors or qualify process-loss fs
durability, live model HTTPS, production compute or workload authentication.

For HTTP, route POSTs at the supplied URL to the public serde
`substrate::remote::Request` enum. Respond with JSON-serialized
`Result<serde_json::Value, String>` (`{"Ok": ...}` or `{"Err": "reason"}`).
`Request::apply` is a reference bridge over an existing runtime and volume.
The `install` request carries the manifest, whose code identity must match the
host's pinned `HarnessPackage`; it never accepts arbitrary uploaded code.
The bridge exposes `install`, `place`, `deliver`, `recover`, `cancel`, `put`,
`get` and `lookup`. Production hosts supply admission policy and binding
resolution; the unauthenticated fixture policy is for isolated tests only.

## Capabilities

`executor` means turn/summary execution, scripted model recovery/replay,
compaction, context discovery, fs staging/reading and complete journal
observations. Both direct and Actors substrates declare it. Host adapters must
implement all these operations to declare `executor`; an unsupported operation
is a failure, never a silent skip.

The resolved recipe driver tags cases requiring native fixture introspection as
`local-driver:<driver>`, where `<driver>` is `local`, `goal-local`, `agents`,
`composer`, `composer_entry`, `notifications`, `task_query`, `subagents`,
`swarm`, `plans` or `plan-query`. They inspect native scheduler state, UI
entrypoints, process fixtures, fixture-owned projections, concrete memory fs
fixtures or instrumented provider counters outside the #1359 package adapter,
and are explicitly skipped by this package. Each report reason identifies the
concrete fixture dependency. These
capabilities describe missing driver adapters; declaring `executor` does not
imply them. The report is the authoritative case and skip count for a corpus
revision. Unknown drivers fail instead of acquiring an automatic capability.

`tests/scenarios.rs` compares every portable case with the original direct
driver, compares direct/Actors reports, exercises the HTTP bridge and proves
that dropping a contract completion observation fails qualification.
