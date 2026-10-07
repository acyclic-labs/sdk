# Integration checkpoint, 2026-10-07

The goal remains active. Website presentation, registry publication and
production deployment are outside this loop.

## Authoritative state

- Main: `fd8272d97ffc20fea444e37f100e738d94e0dbc4`.
- Coordinator merges: PR252 (`f4b27bea5092b2c9f575f81c6e4ecf3226898c09`)
  and PR254 (`4f9447472ad3288ef51cd9d7e5dbc9e7d8785c41`).
- Remote source checkpoint: `fe5eef146075c3e166eddb58c374a7474c78ffb9`.
  This preserves unqualified work; it is not a release qualification.
- PR258 remains draft. Revised source uses Cargo package metadata, required
  v2 package/search schema fields and same-version immutable-history checks.
  Independent review and an exact signed candidate are still required.
- PR259 signed tree `2ad68b79d5d9ac202de95d096889de150bc19d8c` is identical
  to the previously qualified Stream tree. Its signature gate passed, but
  final review found loader, platform fallback and provenance gaps. No merge.

## Evidence and failures

- Actors Rust renderer and Buf descriptor have semantic parity; six descriptor
  compatibility tests passed. The immutable archived descriptor stays intact.
- Extracted Actors Cargo archive passed 70 tests. All-target compilation failed
  because a cross-family conformance example depended on a local-only Workers
  dev dependency. Relocate that fixture to an existing unpublished owner;
  do not hide the failing target from qualification.
- Portable pending Actors fixture and control client are source-owned under
  `scripts/`. The control client passed with normal process exit after observing
  pending server work, client cancellation and zero remaining active work.
- Swift regenerated from a maintained UniFFI 0.31 source-template patch observed
  pending work on the live gate, then propagated Task cancellation into Rust
  and returned `CancellationError`. Preserve the source patch and qualify the
  full package/cohort before enabling support.
- Filesystem WASM numeric-only admission was insufficient: its f64 ABI coerced
  strings, booleans, null and boxed numbers. The owner replaced those boundary
  arguments with Rust JsValue primitive-number extraction and regenerated
  artifacts. Independent packed-package native/browser matrix remains required.
- Filesystem native u64 counters and finite-domain public TypeScript unions
  require exact Rust-owned projections; ordinary string/number casts do not
  establish the requested type fidelity.
- Python and Dart maintained-generator typing/lowering gaps remain open.
  Passing constructor or transport probes alone does not qualify a language.
- An obsolete read-only Git unreachable-object scan owned by this loop was
  stopped after its process ancestry was verified. Free physical memory rose
  from approximately 2 GB to 30 GB; source files were not changed by that action.

## Next bounded milestones

1. Review and sign the complete PR258 data producer, including its proof source
   hash and actual schema-negative tests; run cheap core CI and merge only the
   verified revision.
2. Correct all PR259 loader, unsupported-platform, compiled-dependency and
   source-root provenance findings. Requalify the exact resulting package and
   require independent review before admin merge.
3. Finish Actors archive all-target compilation and minimal mirror removal;
   freeze the current-main generator closure before the single full ten-owner
   generation/drift run. Integrate new docs metadata through Cargo metadata.
4. Close strict Filesystem installed-package parity and exact public TypeScript
   projections. Preserve qualified language source patches and receipts remotely.

Renderer replay tests are empirical regression evidence. Mathematical claims
are limited to the explicit properties proved by the current source-bound Kani
harnesses or constructive proof notes; package/runtime behavior needs its own
execution evidence.
