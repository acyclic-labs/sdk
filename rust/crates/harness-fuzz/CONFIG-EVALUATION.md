# Configuration dependency admission

The existing `config_evaluation` target calls the production evaluator through
`config::model::check`. Its thirteenth source family generates a shared import
graph, or a back edge in that graph, from arbitrary bytes. These are authored
internal inputs, rather than captured external fixtures.

`agent.ts` imports `sys/a.ts` and `sys/b.ts`. The first imports the second and
`sys/input.json`; the second imports that same JSON data. The expected value
contains every imported value, including its changed revision. The cycle
variant instead makes `sys/b.ts` import `sys/a.ts`.

The same cold/warm/fresh comparison runs on both configuration surfaces. It
checks the original output and digest, rejects the cycle before VM entry, and
counts cycle-membership decisions at the production walk. Each closure checks
five graph edges, or three edges before finding the back edge. A warm VM cache
still admits its dependency closure. The two corpus files select these graphs
with quote, backslash, NUL and invalid UTF-8 bytes in the imported JSON value.

Run the existing shared property without a separate graph implementation:

```sh
cargo test --locked -p acyclic-harness-core --lib config::evaluate::model::tests::generated_settings_match_fresh_evaluation_and_reject_corrupt_cache -- --exact --nocapture
cargo fuzz run --fuzz-dir rust/crates/harness-fuzz config_evaluation --no-default-features --features core -- -runs=1000 -seed=1
```

Three retained patches alter the actual production walk. Apply each separately
against an unchanged oracle, run that same named property, then restore the
source:

| Patch under `mutations/` | Required failure |
| --- | --- |
| `config-dependency-stack-scan.patch` | Membership work grows with active depth. This restores the original stack scan, with the same test-only observer counting each comparison. |
| `config-dependency-finished-active.patch` | A valid shared dependency is falsely rejected by the plain-data oracle. |
| `config-dependency-ignore-cycle.patch` | A cyclic closure enters the VM before admission. |

These patches are never linked into the shipped library. The membership
observer is compiled only for tests and the existing `test-support` feature.
The shipping walk keeps one active/completed bit beside each retained module
in its existing name index. It removes the additional O(depth) stack scan;
name lookup remains O(log modules), and source parsing remains proportional
to source bytes. No new read, syscall, barrier, round trip, lock or payload
copy is introduced, and each imported module retains its original one read.

This is component validation of generated source graphs. It does not establish
raw-source parser coverage, mounted-source authority, evaluator cancellation,
full source-branch coverage, or unbounded evaluator capacity. The existing
instruction and memory limits are unchanged. Current runtime, fuzz campaign
and source coverage results must be recorded separately; their presence here
is not a pass claim.
