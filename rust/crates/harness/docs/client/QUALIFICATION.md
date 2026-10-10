# CL1 qualification and simplification audit

Scope: the portable Harness client speculation/view module, data-only hypothesis export, public Harness consumer, tests and examples. All client and optimistic work lives in the existing Harness package. There is no standalone client crate, private consumer package or separate release entry. CL2–CL5 frameworks, transport/checkpoint recovery, durable/shared adapters and inference computation are excluded. Existing domain reducers and CI orchestration are unchanged.

Baseline independently verified with `git ls-remote origin refs/heads/main`:
`e646b77d440631af72eecb29411b7c7497687994`. The owner subsequently rebased onto
independently verified actual main `fe001e9e732bd94bc200f27f793b8c34c557d1ac`.
Production source blob: `490df1b5eefc1038971f4c5cbec645041355f0d6`.
Platform and exact-head final qualification remain open until recorded below.

## Invariants and assumptions

* Canonical facts enter only through the installed typed adapter's evidence
  path. Predictions and exports have no promotion/admission path. Every selected
  overlay carries hypothesis identity, exact basis and adapter provenance.
* Mutation planning checks all limits and correspondence before publication;
  failures preserve canonical records, hypotheses, indexes and accounting.
  Domain callbacks must be pure; process OOM/abort is outside this atomicity claim.
* Edges name existing older identities. Monotonic identity allocation and immutable
  edges imply a DAG by induction. Disposal never recycles identities. Durable
  namespace uniqueness and recovered sequence checkpointing remain host obligations.
* A changed keyed basis or failed prediction visits its keyed roots and reverse
  dependency closure, without modifying unrelated facts. A hypothesis invalidated
  by its basis/dependency cannot become eligible through delayed completion.
* Completion correspondence, prediction status and original operation outcome
  remain separate. Unknown/admission cannot erase indeterminate completion.
  Provisional edges can still represent uncertain predictions; confirmed-only
  edges reject until correspondence is established. Neither admits effects.
* Retained facts, branches, edges and bytes have finite bounds. Per-mutation
  adapter steps and branch/edge visits have an explicit work bound. Map operations
  additionally cost logarithmic time in bounded resident demand. Returned and
  caller-retained snapshots, domain state and allocator fragmentation require
  separate embedding limits; transient planning is bounded by branch/edge/work caps.

These are source-based inductive arguments over the stated transition preconditions,
not unrestricted machine-checked proofs. The kernel trusts the pinned domain
adapter for authentication, reduction, original identity correlation, monotonic
source correspondence, conservative deep bytes, deterministic finite work and
immutable values/bases. It does not prove domain receipts or inference equivalence.
The generic `Basis` must include authority/generation/revision/content identity.

## Production transition evidence

`tests/client_transitions.rs` calls the real public kernel. Ten scenarios cover explicit
views, stable value/basis reference identity, exact duplicate observations,
reordered/delayed admission, missing evidence, independent concurrent hypotheses,
different outcomes, rejection, indeterminate completion, cancellation, partial
canonical updates, transitive invalidation, confirmed-only dependencies, stale
basis, unknown/self/future/duplicate edges, authority/generation/content conflicts,
wrong original operation/key, export identity recovery, unsupported correspondence,
branch/record/edge/byte/work/visibility/retention/identity exhaustion, expiry,
disposal and demand release.

The bounded exploration enumerates all 256 four-step traces over uncertainty,
matching completion, conflicting completion and rejection, with three concurrent
hypotheses. It checks authoritative preservation and selective dependency
invalidation after each production transition. This covers that finite alphabet
and depth; it does not exhaust arbitrary adapters, identities, graph sizes or
infinite schedules. A separate 10,000-revision trace checks constant residency and
one adapter work step per canonical update. With 16 active independent branches,
one changed key emits exactly one hypothesis notification and three work visits.

Harness `examples/client_views.rs` and `tests/client_public_consumer.rs` compile the same consumer
on native and WASM. Harness `Reducer`, canonical `ConversationMessage`/`FileRef`,
original `OperationId`, `Scheduler` and real lease fences own domain semantics.
It demonstrates message prediction plus concurrent authoritative data, provisional
dequeue losing to another worker's admitted lease, and exact canonical tool
call/result references invalidating a pure dependent continuation. Snapshot trust
is explicitly an in-process host assumption. No remote auth, provider execution,
UI equality, inference prefill or KV promotion is claimed.

Eight focused temporary source mutations were killed by test assertions:
mislabel prediction provenance as authoritative; bypass exact basis; accept
duplicate original operation; omit reverse-edge traversal; drop exported original
operation; treat indeterminate as terminal; bypass resident-byte capacity; bypass
work checks. Each mutant compiled, then failed a relevant public consumer/production
test. A `finally` block restored the exact source. No mutation engine or new
verification framework was added to the repository. The first controls preceded
the final basis-sharing change; all eight controls were then repeated against final
production blob `490df1b5eefc1038971f4c5cbec645041355f0d6` and again failed test
assertions. The restored blob matched exactly. Relocating the public fixture did
not change its source blob `49bbf0e157ab7b2cf756dee88e4d66ada4a9afa8`.

## Measured baseline and regression acceptance

Windows x86_64, Rust 1.98.1, existing workspace `dev` profile (`opt-level=1`),
Divan 0.1.21 with its allocation profiler:

`cargo bench -p acyclic-harness --bench client_demand --profile dev --locked -- --sample-count 1000 --max-time 0.5`

| Active records | Canonical view median | Predicted view median | Canonical reconciliation median | Selective reconciliation median | Cold canonical median / allocations / bytes | Cold speculative median / allocations / bytes |
|---|---|---|---|---|---|---|
| 1 | 18.22 ns | 23.59 ns | 214.8 ns | 394.5 ns | 225.3 ns / 5 / 584 B | 476 ns / 10 / 3,344 B |
| 8 | 18.61 ns | 23.30 ns | 216.4 ns | 463.3 ns | 1.291 µs / 19 / 1,088 B | 3.449 µs / 45 / 6,928 B |
| 16 | 19.59 ns | 25.06 ns | 224.2 ns | 503.9 ns | 2.633 µs / 37 / 2,672 B | 7.098 µs / 91 / 16,864 B |

Warm views allocate zero heap bytes. Canonical reconciliation allocates two
payloads / 72 B; selective reconciliation allocates five objects / 936 B for every
measured demand size. Cold retained allocations scale with active demand. The
speculative cost is explicit; the canonical non-speculative path remains available.
No full history/body clone or serialization occurs in a view. Cold allocation
totals include this fixture's installed adapter; accounted resident bytes are a
conservative bound, not RSS. Divan output disposal is outside measured work.

For this source/profile/fixture, acceptance requires unchanged zero-allocation
warm views, unchanged 2/72 B and 5/936 B reconciliation allocations, constant work
and residency in the history test, and one changed-hypothesis notification for
one independent key. An allocation increase requires explanation and renewed
measurement. Timing medians exceeding twice these recorded medians require
investigation on a comparable idle host; timing maxima contain scheduling noise.
These are recorded source-specific regression thresholds, not universal latency,
minimum-computation, allocator-layout or optimality claims. Body transfer, storage,
network, UI renders and inference waste are absent from CL1 and belong to later loops.

## Simplification/deletion audit

There is one kernel and one typed adapter seam. No reducer, authority ledger,
provider lifecycle, journal, transport, framework engine or backward compatibility
path was copied. Existing reducers remain in their own crates. The numeric test
adapter is only a bounded kernel fixture; the public consumer uses actual Harness
reducers. No replaced production path exists in this new module.

The initial standalone packaging and private qualification fixture were removed after the human required all client and optimistic work inside Harness. The unchanged kernel now has one public Harness module; its tests, example and benchmark use the existing package. Both new crates, workspace members, standalone lock entries and release entry are deleted together. Earlier standalone package archives, patched attempts and fresh archive consumer results are historical source evidence only. They do not qualify the final Harness package. No registry publication occurred.

Retained structures have concrete purposes: keyed demand avoids lifetime history;
original-operation index rejects duplicate live identity and correlates receipts;
key index finds affected roots; reverse edges find only dependent hypotheses;
monotonic sequence avoids a lifetime identity ledger. Terminal hypotheses remain
bounded demand until explicit discard/expiry. Retention uses one bounded active
scan instead of another timer/expiry index. View selection is explicit, with
ambiguity instead of an automatic branch ordering policy. Re-evaluation creates
a new validated hypothesis rather than introducing a callback scheduler.

Reconciliation was split into operation resolution and invalidation-root planning
to make the atomic commit boundary reviewable. Exact duplicates preserve shared
values and basis pins. Both pins and projections are reference-shared, so the
kernel never requires `Basis: Clone`. Adapter bytes remain distinct from kernel
metadata to prevent repeated observation accounting inflation. Conservative
metadata covers sparse indexed nodes; actual allocation baselines are above.
No known unnecessary production mechanism remains in this scope. The retained
complexity enforces demonstrated uncertainty, identity, DAG, atomicity and demand
bounds; this audit makes no universal minimality claim.

## Final-source gates

Production blob above and the unchanged public-consumer blob passed Windows,
WSL/Linux and macOS (`ssh ivar`) native tests/strict lint and actual installed
Chromium 154/WASM identical traces/strict target lint on commit
`50820c46f58f02fd71a8dc1645c83cd941b224b5`. Each ran ten kernel scenarios and
the public Harness consumer. Rust 1.98.1 and wasm-bindgen-test runner 0.2.117 were
used. These receipts remain scoped source evidence; the later package/fixture
partition requires affected closure qualification below.

Open: final Harness package/current consumer and relocated-module platform
qualification; required exact-head SDK Qualification; qualified PR merge and
independent actual-main SHA/tree verification. An open gate is not a pass.
