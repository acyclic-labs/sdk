# Stream observability implementation plan

Baseline: `d363004e063`, inspected 2026-11-12. This is an implementation plan,
not a completion claim. The current contract in `docs/observability.md` still
describes the tracing/export implementation that this work must replace.

## Inventory and ownership

| Current source | Facts already available | Destination / removal |
| --- | --- | --- |
| harness-core executor journal (`executor/mod.rs`) | ModelStarted, Model/ModelInline, ToolStarted/Completed/Failed, original request and result references | Keep one original admission and settlement; extend these contracts for missing clocks and typed causes, never add a second observation writer. |
| `executor/model_usage.rs`, `model_observation.rs` | Provider/model, token counters, dispatch and completion clocks, monotonic latency; terminal usage staged separately | Bind identity once at admission; terminal delta owns counters and result. Reuse original committed source references in queries. |
| Agent status/inbox streams | Spawn, send, settle, child-wake facts and original delivery clocks | Reuse these records and their authenticated paged readers; add missing commit/process-failure facts at their owning primitive. |
| `harness-core/executor/metrics.rs` | Optional offline Arrow/Parquet projections | No new normal-mode store. Audit consumers before deleting redundant projections; preserve unrelated explicit research exports. |
| `executor/metrics/inspect.rs` | Inspect evaluation-log export | This is not `acyclic inspect`; do not treat it as live-query completion evidence. |
| `plugin/src/runner/lifecycle.rs` | ACYCLIC_LOG-selected text channel, caller/CLI storage reservation, stderr delivery; native WaitReport subscriber | Remove the text log mechanism after debug stream replacement. Preserve event-driven native wait ownership until its facts have a stream home. Default already has no text receiver. |
| `observability` crate | Operations registry, histograms, SLO queries, credited exporter, text output and lifecycle ownership | Delete duplicate metrics/text/export state. Move necessary completion/wait ownership into the owning shared primitive, not another observer crate. |
| `grpc-observability` crate | RPC lifecycle adapters and code generation | Replace with thin adapters to the one debug mechanism; remove crate and dependencies when no consumers remain. |
| Crate-private `obs` modules / direct tracing calls | Operation spans, internal timings, background errors | Normal facts go to owner streams exactly once; internal spans go only to configured debug streams. Delete duplicates. |
| `harness-providers/src/http.rs`, `harness-core/src/http.rs`, provider authorization | Shared exchange boundary; request headers may contain credentials and HttpRequest deliberately has no Debug | Capture only through a credential-free projection here, before any persistence. Both native and browser paths must use it. |
| Swarm inspection scripts (`inspect-metrics.mjs`, `audit-cache-remote.mjs`) | Independent transcript/status parsing, model/cache totals and inter-call gaps | Replace parsing with the shared query after CLI delivery. Inter-call gaps alone do not prove harness overhead. |

Source census at baseline: 4,897 Rust files under rust/crates and plugin;
208 direct tracing macro sites and 443 eprintln sites, including tests and
benchmarks. These counts are inventory, not a count of production log writers.
Macros and generated adapters require a separate consumer audit. Dependency
manifests currently reference observability from plugin, auth, actors, inference,
objects, conformance, harness-core, filesystem, native-runtime, stream, machines
and fuzz; grpc-observability is used by primitive RPC adapters and fuzz.

## Facts and guarantees

Keep original source stream/sequence and idempotent operation identity. Admission
does not imply dispatch; a staged artifact does not imply settlement. Unknown
counters/clocks remain unknown. A recovered call must not acquire fresh timing.
Use monotonic durations, not subtraction of potentially corrected wall clocks.
Failures retain stable typed classes and sanitized cause chains; never persist
raw Display/Debug errors as a shortcut. Retry-After waits are original transport
wait facts, not inferred latency. A failure before a terminal commit must remain
distinguishable from a terminal tool failure.

## Bytes per step

The current model observation path serializes every provider ModelEvent, inline
when supported, otherwise as a staged JSON artifact; terminal usage is another
staged JSON artifact referenced by that event. ModelStarted references the exact
request. ExecutionRecord repeats operation_id, sequence and idempotency_key.
Therefore current writes are not just a constant per-call metadata record:
they include per-delta framing, usage and storage publication overhead.

No measured physical bytes-per-step baseline is available yet. Source size,
JSON exports and recon summaries cannot substitute for storage-write evidence.
Before changing the journal layout, instrument the existing storage accounting
in a deterministic real-provider-fixture conversation: report model calls,
payload bytes, encoded metadata bytes, staged artifact bytes, physical writes,
and commits, separately for normal and debug. Vary prior history and number of
response deltas independently. Record exact fixture, platform and pushed SHA.

Target: zero normal observation files outside the original streams; metadata
per admitted/settled call independent of history length and payload chunk count;
content counted separately, with no repeated request prefix or state snapshot.
Choose the numerical byte expectation from the versioned encoding and fixture,
not an arbitrary capacity cap. Coordinate with GC-71 per-step persistence; inspect
its actual merged contracts before changing shared journal semantics.

## Delivery sequence

1. Normal facts and deletions: add missing original timing/wait/failure deltas
   to owning stream records; remove text logs and duplicate exporters only when
   each unique fact has its replacement. First cleanup removes raw settled-tool
   warning duplication and the unmatched read-profile markers. Remaining spans,
   crates and stderr paths are still outstanding.
2. Shared live query and `acyclic inspect <session> [--agent X]`: authenticated
   stream readers, children_page and follow; bounded pages, one visible agent's
   state at a time, no eager tree walk or polling. Return model/tool/wait durations,
   measured harness gaps with explicit coverage, provider cache fractions,
   uncached tokens per minute, failure classes, idle/stuck evidence. Update TUI
   and swarm-inspect to consume this surface. Stuck classification needs explicit
   policy and primitive alarm/event support; do not invent a fixed timeout.
3. Debug: per-process stream with version-one content-addressed zstd chunks;
   prefix references plus deltas, event-driven OS samples and harness span facts.
   User-configured byte capacity with documented default; evict oldest whole
   chunks atomically. References must remain readable: retain dependencies with
   their referencing chunks or evict their dependent chunks together. No dangling
   prefix references, periodic sampling, separate file logger or export pipeline.

## Required proof and completion ledger

- Normal deterministic real-binary run: all writes owned by stream backing
  storage, constant metadata slope as history and response chunk counts vary.
- Deterministic inspect fixture: all listed views, missing measurements, partial
  calls, restart, concurrent tools, corrected clocks and paged children; follow
  wakes on commits and cancellation, without periodic reads.
- Planted credential through real transport: headers, provider-echoed bodies and
  errors cannot leak to normal or debug streams or referenced objects. Verify
  decoded/decompressed storage too, not merely raw compressed bytes. Existing
  sanitized-error tests alone do not prove this invariant.
- Debug shared-prefix fixture: N calls retain the prefix once; round-trip bodies
  and budget rollover preserve every retained chunk's reference closure.
- Focused shared-conformance coverage for lifecycle changes; exact pushed-head
  check/compile through Foreman and native macOS proof for affected OS paths.
- Three implementation PRs merged to main, deletion/dependency audit complete,
  and swarm-inspect invocation/output evidence from the shared CLI. None of
  these completion conditions has yet been established on this branch.
