# Harness scenario substrates

From the SDK root:

```sh
cargo run --locked -p acyclic-harness-conformance --bin scenarios -- --report out.json
cargo run --locked -p acyclic-harness-conformance --bin scenarios -- --filter core/ --report out.json
```

Build on bb2. The runtime budget is measured separately from Cargo compilation.
Report identity, digest, bundle-digest and receipt validation live beside their
shared validator in `acyclic-conformance`:
`cargo run --locked -p acyclic-conformance --bin harness-conformance -- report.json`.
`--corpus DIR` selects an exported copy of `harness/tests/scenarios`. Defaults
are merged by the existing corpus loader; no case JSON needs changing.
`--substrate local` (the only substrate) runs the portable executor and
composer fixtures directly. The existing
`cargo test -p acyclic-harness --test main` path retains its native driver
selection and scenario inputs.

The report has a sorted `records` array with one `case`, `status`, `reason` and
`requires` entry per discovered JSON case, plus pass/fail/skip counts. Defaults
files are configuration, not cases. Filtering produces explicit skips. No wall
clock timestamps or random runtime identifiers appear in passing reports. Exit
status is zero only with at least one pass and no failures; a filter matching
nothing cannot qualify a host.
Every known driver's shared schema is validated before capability or filter
exclusions. A malformed native case fails even when its capability is missing.

At the initial corpus revision, the 847 JSON files comprise 60 inherited-default
files and 787 executable cases, matching the existing discovery rule. The
reference qualifies 436 executor/composer cases and explicitly skips 351
native-fixture cases with the capabilities below. Defaults apply to their descendant
cases; they are not executable scenarios.

## Capabilities

`executor` means turn/summary execution, scripted model recovery/replay,
compaction, context discovery, fs staging/reading and complete journal
observations. The local substrate declares it. A substrate must
implement all these operations to declare `executor`; an unsupported operation
is a failure, never a silent skip.

`composer` means catalog capture/resolution, completion, pinned mention attachments,
configuration publication and immutable fs reads through the supplied providers.
The local substrate declares it.

The resolved recipe driver tags cases requiring native fixture introspection as
`local-driver:<driver>`, where `<driver>` is `local`, `goal-local`, `agents`,
`notifications`, `task_query`, `subagents`,
`swarm` or `plans`. They inspect native scheduler state, UI
entrypoints, process fixtures, fixture-owned projections, native builder/run_prompt
fixtures or instrumented provider counters outside the portable fixtures,
and are explicitly skipped by this runner. Each report reason identifies the
concrete fixture dependency. These capabilities identify native fixture
dependencies; declaring `executor` does not
imply them. The report is the authoritative case and skip count for a corpus
revision. Unknown drivers fail instead of acquiring an automatic capability.

`tests/scenarios.rs` compares every portable case with the original direct
driver and proves that wrong answers and malformed cases fail qualification.
