# Integration checkpoint, 2026-10-07

The full goal remains active. Generated versioned documentation data is in scope;
website presentation, registry publication and production deployment stay outside
this loop.

## Authoritative state

- Remote main verified by ls-remote: ef65198d08ddf59d632e73ff8c7c362defb9ffda.
  PR262 changes binding conversions, dependencies and generation scripts. Every
  pending port must preserve these fixes rather than overwrite newer main files.
- Coordinator merged PR252, PR254 and PR258. PR258 merged at
  3a7ca21c8195e0ef847cf88c12a950dd41b903f0 on 2026-10-07 08:12:48 UTC.
  It supplies Cargo-bound v2 package/search docs metadata and immutable bundles.
- Verified remote WIP checkpoint refs before this update:
  foundation 4ef2b10761afde394f49cc4f7b30f10cca1658e1;
  Actors generation 1429d5cf38b982ba0be4d87233a6bacbdda85ac7.
  These preserve source; they do not establish release qualification.
- PR259 remote head 018ef828b8ecf6d98844791f3c218b2f0c85de46 lacks the latest
  locally qualified fixes. It must not be merged in that state.
- Filesystem checkpoint 9dd4261da162f6f28225e0c270ffdcb1379ac1c4 is remotely
  preserved but has old parent fd8272d97ffc20fea444e37f100e738d94e0dbc4.
  A current-main candidate and new source/artifact attestation remain required.

## Evidence and remaining gaps

- Installed Stream testing caught a cancellation constructor import failure.
  The generated-binding async factory fixes it. Latest local source passes
  TypeScript, 55 Stream tests, 19 planner tests, browser fallback and installed
  Windows native follow cancellation/server release. Selection and alias tests
  were added. Independent review and direct stalled native-connect cancellation
  evidence still gate the final candidate.
- Filesystem staging passes 70 tests, strict native/browser input matrices,
  Rust u64 preservation and extracted-package finite-type compile negatives.
  This proves that snapshot only. The old 0D29 archive cannot be relabeled with
  a new source identity. Build and attest the exact final signed candidate.
- Machines transport validation reuses the Rust safe-integer predicate. Its WASM
  build, 24 tests and TypeScript check pass. Keep it out of the Filesystem PR;
  port the bounded TypeScript deletion while retaining main's conversion fixes.
- Actors has Rust-owned Protify contracts, descriptor semantic parity, archive
  all-target compilation and 70 passing tests. The pinned ten-owner release
  fixture now passes (625.66 seconds), including package/version/source SHA,
  generated artifacts, drift rejection and three compiled-source tamper checks.
  Its compiled closure is 244cb35048bd78ceb70b2a286e050f5212f69fb970465cedb405f9dcce950187.
  This qualifies the frozen staging tree, not the pending current-main port.
  Other families retain legacy contracts;
  broad authored-TypeScript deletion has not been accomplished.
- Real Rustdoc feature-profile executions pass. Production integration and
  binding API coverage remain outstanding. Resolving a core-crate owner must
  not hide the binding's public Rust types.
- Maintained language generator source patches are being qualified for strong
  types and Rust future cancellation. All-operation installed evidence is
  cohort-specific; constructor probes and older receipts cannot qualify a newly
  generated package.
- New symbolic u64/presence Kani runs reached the solver but timed out. They are
  inconclusive, not proofs. Existing proof claims retain their recorded scope.

## Next bounded milestones

1. Port Stream and Filesystem onto current main without undoing PR262. Complete
   exact-source installed qualification and independent review before merging.
2. Port the qualified Actors ten-owner generation/drift implementation, close the minimal
   contract cutover, and delete replaced mirrors and generation scripts.
3. Preserve Machines and language/profile/proof prototype source remotely;
   integrate only qualified, minimal dependencies and source patches.
4. Extend Rust authority to remaining families, remove duplicate TypeScript
   policy, and qualify executable examples and versioned documentation data.

Ordinary CI stays cheap. Broader qualification binds source, generator versions
and installed artifacts. No auto-merge is enabled.
