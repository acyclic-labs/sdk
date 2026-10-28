# Tooling qualification and measurement scope

The public primitive suites expose `Backend`, `verify`, `bench` and `suite!`. Their provider counters measure logical dispatch. A physical HTTP request, credential refresh, placement lookup or registration must be observed at its actual seam before a zero-control claim can be made. Hosted compilation does not qualify execution. Keep the existing pre-handler authority observation and invocation/effect assertions.

## Retained generation admission

Historical owner compilation uses at most four workers by default. Set `ACYCLIC_SDK_JOBS` to an integer from 1 through 32 to select a smaller or larger bounded pool. Each worker finishes its queued owners before an operation panic reaches publication; results retain input order. This bounds owner workers, not every compiler child or compiler RSS.

The retained archive decoder admits at most 64 MiB compressed input, 256 MiB complete expanded bytes, 32 MiB per file and 65,536 tar members. The expanded bound includes metadata, padding and the final trailer-validation pass. Source replacement rechecks are also bounded. Links, escaping paths, duplicates, checksum/trailer corruption and immutable-write conflicts remain refusals. Explicit `ArchiveLimits` are available to decoder callers. The registry archive plan and imported-source verifier stream one checksum-verified immutable snapshot into private temporary files and retain path/digest metadata. Files are published only after the complete member/trailer checks and a bounded source recheck. The plan reuses that snapshot after Cargo metadata instead of decompressing again. Staging disk is bounded by the compressed plus expanded limits; payload transfer uses fixed 16 KiB buffers. The VCS JSON parser still reads at most one admitted member, and tar extension metadata remains subject to the aggregate expanded bound. The retained byte-map API remains for other callers; its previous implementation is a test-only small-fixture parity oracle. No RSS or exact allocator claim follows from these source bounds. No exporter or language backend is retired by these bounds.

## Standalone secret references

The Workers `./manifest` export initializes the existing manifest WASM module. After initialization:

```ts
const parsed = parseSecretReference("secrets:/Scope/deep/NAME");
// { reference: "secrets:/Scope/deep/NAME", segments: ["Scope", "deep", "NAME"] }
formatSecretReference(parsed.segments);
selectSecretReference("MODEL_KEY", parsed.reference);
```

These functions call the existing Rust constructors. There is no URL normalization, percent decoding, fixed path depth, implicit scope, alias interpretation or secret-value access. Selection admits the environment property separately. A valid reference grants no authority to resolve it: the host must still check the admitted source/binding and effective capability ceiling. Full-manifest validation remains available.

## On-demand evidence plan

No measurements below are supplied by compilation. Execute only the selected comparison on the appropriate host, retaining exact source/configuration, commands, raw outputs and receipt identity.

| Area | Required comparison | Independent failure control |
|---|---|---|
| Workspace and compatible test roots | Inventory actual target names, required features, platform/Loom gates, discovered/executed/skipped case IDs and reasons. Compare cold/warm links, process starts, compiler CPU/wall time, RSS and target bytes for the same original default-member scope. Keep proof/compiler/history isolation. | An absent selector or reduced-feature import must fail; zero selected tests is refusal. |
| Archive admission and workers | Sweep archive compressed bytes, expansion, member count and largest member independently; sweep owners and worker budgets. Record RSS, threads, child processes, descriptors, staging disk, byte passes, exact path/digest inventory, output ordering and publication outcome. | Corrupted checksum/trailer, late duplicate, oversized expansion/member, source replacement and early panic must refuse without accepted partial publication. |
| Primitive and lifecycle workloads | Fresh/reopen and cold/warm create, step, send, fork and archive at 1k/10k/100k where supported. Separate logical calls from transport requests and backend stages; record data/control/token/registration, retries, dependent RTTs, actual handler invocations/effects, bytes and allocations. | Inject an extra lookup/ACK or token refresh; unchanged request budgets must fail alongside unchanged authority/crash oracles. |
| Hosted cancellation | Bundle and execute the real Worker adapter. The bounded fixture emits actual entry/abort observations through one stream; it replaces readiness probes. Record actual driver calls, data/event delivery and start-to-cancel latency. | Never-entering, ignored cancellation and missing actual abort must fail within the fixture deadline. Shared fixture unit results alone do not qualify a hosted runtime. |
| Corpus/report and native custody | Compare sorted original IDs/counts/outcomes, native-portable traces, case/report/receipt digests, validator decisions and lifecycle/descendant cleanup on each actual OS. | Preserve all eleven oracle mutation classes, missing/duplicate case refusal, forged digest, held pipe, escaped descendant and failed Windows child admission controls. |

Current work preserves the legacy StockExecutor corpus and receipt producers, filesystem taxonomy, language backends, native OS distinctions and historical proofs. Full hosted Actors composition, physical/auth request instrumentation, remaining archive reader adoption/exporter consolidation and cross-driver custody parity remain distinct joins. G24/G25 blob qualification waits for its actual provider contract; no preview API is integrated.
