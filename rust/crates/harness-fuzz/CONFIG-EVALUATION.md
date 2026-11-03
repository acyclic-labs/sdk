# Configuration source and dependency admission

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
cargo test --locked -p acyclic-harness-core --lib config::evaluate::model::tests::generated_settings_match_fresh_evaluation -- --exact --nocapture
cargo fuzz run --fuzz-dir rust/crates/harness-fuzz config_evaluation --no-default-features --features core -- -runs=1000 -seed=1
```

Fifteen retained patches alter the actual production source admission, walk, fold or cache. Apply each separately
against an unchanged oracle, run that same named property, then restore the
source:

| Patch under `mutations/` | Required failure |
| --- | --- |
| `config-layer-value.patch` | Composition loses the preceding layer's value. |
| `config-layer-generation.patch` | Provenance drops the original pinned generation. |
| `config-layer-digest.patch` | Provenance no longer identifies the original module closure. |
| `config-fold-default.patch` | Nested callbacks lose the original lower value. |
| `config-fold-own-property.patch` | An inherited prototype property becomes a lower default. |
| `config-fold-container.patch` | Object keys are incorrectly matched to array positions. |
| `config-dependency-stack-scan.patch` | Membership work grows with active depth. This restores the original stack scan, with the same test-only observer counting each comparison. |
| `config-dependency-finished-active.patch` | A valid shared dependency is falsely rejected by the plain-data oracle. |
| `config-dependency-ignore-cycle.patch` | A cyclic closure enters the VM before admission. |
| `config-dynamic-import.patch` | Dynamic import reaches the engine instead of being refused by source admission. |
| `config-json-object-literal.patch` | A JSON object is incorrectly interpreted as a JavaScript object literal, losing its `__proto__` property. |
| `config-transform-output.patch` | TypeScript transformation output is discarded, incorrectly refusing the required valid typed literal. |
| `config-import-root-escape.patch` | An import escaping the original tree root is incorrectly admitted. |
| `config-import-directory.patch` | A legitimate nested import loses its original directory and fails actual module linking. |
| `config-cache-key.patch` | A changed base or imported value incorrectly reuses an older result. |

The cache mutation removes the production key comparison. It uses the same
known-value and cold/warm/fresh oracle as the positive inputs, replacing the
one-off test that manually overwrote a retained cache value.

These patches are never linked into the shipped library. The membership
observer is compiled only for tests and the existing `test-support` feature.
The shipping walk keeps one active/completed bit beside each retained module
in its existing name index. It removes the additional O(depth) stack scan;
name lookup remains O(log modules), and source parsing remains proportional
to source bytes. No new read, syscall, barrier, round trip, lock or payload
copy is introduced, and each imported module retains its original one read.

Every input also reaches the actual TypeScript and JavaScript source parser.
Accepted emitted JavaScript must be admitted again with the same resolved
imports. Arbitrary programs are parsed without running their control flow.
The input also supplies an encoded string literal and a dynamic import using
that same literal. The TypeScript literal includes `satisfies string`, so its
successful admission requires type erasure rather than tolerating a refusal of
every typed program. The literal must parse; the dynamic import must return the
public invalid-module error without entering the engine on either surface,
for both source extensions. A missing loader's later execution error cannot
satisfy this admission check. The actual guard-removal mutation must fail that
same oracle. This subsumes the former single dynamic-import error example.

Raw JSON goes through the production evaluator and is compared with
`serde_json::Value`, preserving all keys, values and array positions. Numbers
are compared in ECMAScript's double-precision domain, which rounds large
integers and normalizes negative zero.

Five additional authored internal corpus inputs retain the original JSON
property-semantics example, numeric and escaped-string cases, TypeScript type
erasure with a static import, a forbidden dynamic import and malformed JSON.
The shared property subsumes the former JSON-only example and checks the
entire result through the original evaluation entry point. The object-literal
mutation changes production JSON module construction, without modifying the
oracle or linking a test-only parser.

This is component validation of raw source and generated source graphs. It does
not establish mounted-source authority, evaluator cancellation, full source
branch coverage, or unbounded evaluator capacity. The existing
instruction and memory limits are unchanged. Current runtime, fuzz campaign
and source coverage results must be recorded separately; their presence here
is not a pass claim.

The same property also executes fixed static-import programs through the real
closure and QuickJS linker. Input bytes remain a JSON string in the original
`sys/input.json` reader. Generated importer depths exercise relative imports
and direct `sys/` imports for both source extensions; the result must retain
that entire string. Root escapes and noncanonical segments must be rejected
by the production strip boundary before any tree read or VM entry. This
subsumes the resolver-only example test. The two import patches mutate actual
root-underflow admission and importer-directory retention, never an alternate
resolver or a test-only fault switch.

The existing nested-function family carries changed lower values through both
object properties and array positions in the actual evaluator. It distinguishes
an absent lower value from explicit null, refuses inherited prototype values
as defaults, and does not match object keys to array positions. Its two-level
object function and array-entry function, inside both root values and root
functions, subsume the former nested-fold example;
encoded input is retained in the complete expected result. Both configuration
surfaces and cold, warm and fresh evaluations use the same property. The three
fold patches mutate the actual lower-value selection, own-property admission
and container-kind check in the production finish function. No alternate fold
or test-only fault field is used.

The imported-value family also invokes public `compose` with a replacing lower
module, absent layer, optional `off` marker and typed upper callback. The output
must preserve the original lower data, or the original base above an `off` cut.
Its complete provenance binds the held layer order, pinned generation, form
and module digest, checked against actual original evaluator results. No second
resolver or interpreter supplies that reference. This subsumes the former
layer-fold example, including its named static and erased type imports. The
upper callback imports the named JSON value through the existing shared graph,
without introducing a separate reader. Three kept
patches mutate actual composition data flow and generation/digest publication.
