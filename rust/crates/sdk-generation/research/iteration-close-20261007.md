# Integration checkpoint, 2026-10-07

The full goal remains active. Generated versioned documentation data is in scope;
website presentation, registry publication and production deployment stay outside
this loop.

## Authoritative state

- Remote main after PR261: 0324cb1fb55a2e902e42eebd853bf092f2fd148f.
  PR262 changes binding conversions, dependencies and generation scripts. Every
  pending port must preserve these fixes rather than overwrite newer main files.
- Coordinator merged PR252, PR254 and PR258. PR258 merged at
  3a7ca21c8195e0ef847cf88c12a950dd41b903f0 on 2026-10-07 08:12:48 UTC.
  It supplies Cargo-bound v2 package/search docs metadata and immutable bundles.
- Coordinator merged PR263 at 03bbf867c32ab61dfb262ab20fdf9d31a4e6dae1
  on 2026-10-07 09:48:48 UTC. Exact signed candidate
  cdfb3c072f065f6a8a517205fa81f200127776ea passed independent review,
  27 Machines tests, TypeScript, and every required cheap CI gate. Primitive
  transport and page-size checks now execute in Rust before custom providers;
  TypeScript retains thin error-class adapters and removes duplicate policy.
- Verified remote WIP checkpoint refs before this update:
  foundation 8a8f1fa8e2c20ca916dd6b2b3f2500c2c3c9ae43;
  Actors generation 7e5c2fe42f6e7342da08d3a93e251f4db6cd0a12.
  These preserve source; they do not establish release qualification.
- PR259 remote head 1a3ec9481b6d19722bbc23fbef1f34e52657b355 passed local
  installed qualification and CI. Branch protection requires latest-main
  ancestry. Clean signed integration ed2f7c5e0a9e588e6b7559ca1325f0b1d09caf0f
  is saved remotely. A subsequent review found lockfile checkout line endings
  were not pinned. Signed 5c91a9ab5c8fbfd6f50505b01a462d651a4f1d75 adds
  `*.lock text eol=lf`; fresh `core.autocrlf=true` checkouts of Cargo.lock and
  bun.lock retain identical LF bytes. Exact package metadata is being refreshed
  again before the final PR update. Runtime source/closure bytes are unchanged.
- Filesystem checkpoint 9dd4261da162f6f28225e0c270ffdcb1379ac1c4 is remotely
  preserved but has old parent fd8272d97ffc20fea444e37f100e738d94e0dbc4.
  A current-main candidate and new source/artifact attestation remain required.

## Evidence and remaining gaps

- Installed Stream testing caught a cancellation constructor import failure.
  The generated-binding async factory fixes it. Latest local source passes
  TypeScript, 55 Stream tests, 19 planner tests, browser fallback and installed
  Windows native follow cancellation/server release. Selection and alias tests
  were added. The direct installed stalled-connect test fails: cancellation
  settles promptly, but tonic's background connection retains a TLS socket.
  Rust connection lifetime cleanup, a rebuilt installed artifact, and exact-source
  independent review gate the final candidate.
  The earlier 4de review scratch install loaded the older 931d9254 native binary,
  while the archive contains f9382db8. Its paused raw Node socket also failed to
  consume ClientHello bytes, delaying EOF observation. The coordinator corrected
  both conditions: direct generated N-API cancellation against actual f9382db8
  archive bytes passes, with typed cancellation, zero sockets and unhandled errors.
  `stream-native-connect-audit/run.mjs` records actual loaded paths and hashes.
  The public default provider still needs the same exact final installed check.
  Additional source tests passed in the older rust-sdk-docs-source checkout;
  they must be ported to the production stream-napi candidate and rebuilt.
- Filesystem staging passes 70 tests, strict native/browser input matrices,
  Rust u64 preservation and extracted-package finite-type compile negatives.
  This proves that snapshot only. The old 0D29 archive cannot be relabeled with
  a new source identity. Build and attest the exact final signed candidate.
- Machines validation cutover is merged. Broader handwritten Machines type
  definitions and the other families' TypeScript policies remain to migrate.
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
- Package-model validation rejects unsupported generator families and claims
  not present in the executed receipt's status, operations, and checks. Its six
  focused tests pass; the emitter consumes scope instead of inventing it.
  Source is verified remotely at 75c3ced84ae03b155e532acc5cf01d434ead0926.
  Production integration remains gated on
  actual executed qualification claims and producer-specific provenance: the
  earlier Python record used the Kotlin patch digest and is not qualified.
- The rebuilt C# package targets net8 and has assembly identity Acyclic.Actors,
  Version=0.2.0.0. A clean external net8 consumer passed all eight operations,
  cancellation and negative type checks on Windows. Other platforms and the
  final Actors producer remain separate qualification work.
- Three symbolic u64/presence Kani prototype models passed, including a failing
  mutation control. They invoke local model projections, not production
  projection functions; production equivalence is not established.
- A separately source-snapshotted actual Actors `validate_add_subscription`
  theorem passes for the full symbolic u64 cursor domain: 0/295 failed. The
  intentional negative assertion fails (1/295, exit 1). This is a narrow
  production-validator result; final semantic constructors, limits, digest,
  enum and oneof proofs still require the final generator-cutover snapshot.
- The maintained Protify fallible-proxy source patch now passes 17 prototype
  tests, covering nested conversion, optional messages and oneof presence.
  Its exact source is preserved in the primary research tree; production
  cutover and descriptor compatibility remain required before merge.
- Stream frozen commit 1a3ec9481b6d19722bbc23fbef1f34e52657b355 is clean and
  saved remotely. Its exact installed archive (SHA256
  bce8fb4bb97455ab462607c86cbb6e29522cd5bd66549ba9a39312a050350379)
  passed independent public cancellation checks three times with zero open
  sockets and zero unhandled errors. PR259 now points to this commit; its final
  cheap CI run 37612671771 passed. GitHub's merge rule still reports the required
  SDK Qualification context as expected; no merge has occurred. The old scratch cancellation failure loaded a stale
  native binary and used an undrained socket observer; it is invalid evidence.
- Machines type deduplication is signed commit
  9711a5598bb9411b125a657b82516b9538614754: two files, net 54 authored lines
  removed. Rust tsify declarations supply public domain unions; 27 Bun tests,
  TypeScript compilation and Rust/WASM tests passed. Independent review remains
  passed and PR264 is open on the remotely verified source branch.

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

## Current integration audit: 2026-10-07

- Main is 89cb2ec2e1c3b7a48c6555c0c3f9c7dcd77bdf3f (PR265). Preserve its tracing, observer adapters and lint/qualification changes in all ports.
- Stream signed source 7c2be68e75f966f0dccbe0cfef44e79f1195c9fc is remotely pushed to PR259. Exact archive 5e8de23b53f61a8450038846a2a81f5a93b0518cab2cf1625517b6150c9dc24e independently passes native cancellation and native/WASM byte guards. New review findings require preserving invalid ifAbsent input and Rust-owned follow recovery before admission. No merge claimed.
- Machines PR264 installed external consumer positive/negative typing and runtime smoke passed at9711. Re-port only its two owned files onto current main, preserving observer exports; refresh exact-source package evidence.
- Filesystem source candidate actual4c5191da93365fff068ec8f588946e7b7792825f has36 admission cases passing. Receipt contained a nonexistent source hash and lacks full native archive/prepost attestation; repair, sign, integrate latest main and requalify. Finite payload kind typing remains required.
- Actors remains two authored semantic/schema layers. Production cutover onto executable semantic structs is required; minimal maintained Protify extensions are assigned, plus persistent archived descriptor mutation tests. Passing76 tests does not establish single authority.
- Rustdoc profile integration library/sidecar tests pass, but production feature/target receipt matrix, private binding inputs and compiled freshness/version checks remain assigned before admission.
- Formal evidence includes actual production cursor full-u64 proof and11 domain harnesses. Refresh against final derived semantic producer before claiming final property coverage.

## Verified merge and current source checkpoints

- Machines PR264 merged at `2026-10-07T12:27:28Z`, producing main `af6f814ccb97c3d49f93689e70b19d97d7840377`. Reviewed source `714ca576ec63f5df2ef3a769eae3cb61f05c7e95` passed 27 tests, external positive/negative typing and runtime checks. Two files changed: 39 additions, 95 deletions, net 56 authored TypeScript lines removed. Required inexpensive CI passed.
- Stream final signed candidate `92c267c08b4cd2cac7b1347b05547f3c1dab768b` is pushed to PR259 and its checkpoint branch. It preserves invalid absence conditions, retries transient follow failures in Rust, and passes native all-target Clippy. Final exact-source package qualification is being refreshed. Earlier archive receipts remain historical. No Stream merge is claimed.
- Actors signed checkpoint `edcc1292ac2363d2ca62d24c7e9033279b32c087` and foundation checkpoint `66e3bba580562cbe7c2498ae7dfd374646591925` are verified remotely. Normal worktree indexes and HEADs remain preserved. Actors raw field mirrors are removed; current-main integration and installed qualification are next.
- Filesystem signed candidate `f775b8465d836cf69b358f9fa9ebd8edbfa819db` is saved remotely. Root review identified hosted payload-kind duplication, an unchecked string projection, and possible missing current-main counters. Peer fixes and exact installed qualification are required before a merge-ready PR.
- All 16 agents have useful production, qualification, generator-gap, proof or review assignments. No registry publishing, production deployment or website merge occurred.

## Latest admission audit (supersedes candidate statuses above)

- Main remains `af6f814ccb97c3d49f93689e70b19d97d7840377`. Stream PR259 is open at signed, clean source `486bd26ac1f9c8c9a88b13a70bdcf49d5637ecb1`; its inexpensive qualification run `37630346267` passed. All existing review threads are resolved. Exact installed-package evidence is being refreshed for this revision; the `448f99` receipts are historical, not final-source evidence.
- Stream qualification receipt creation now validates archive contents and package identity using the existing maintained archive validator. Its 26 focused archive/publication/planner tests pass. Fixture creation uses system tar and does not introduce a Bun dependency into the full policy lane.
- The current integrated Actors candidate is `Q:/sdk/work/sdkgen-main-port-current`, not the primary foundation checkout or the older `sdkgen-main-actual03bb` checkout. The remotely saved candidate `f8bf8daf64fae0c04366fd1e5326053dde3d4e9e` now has further local cancellation/lint work. It requires a minimal signed production scope, exact installed qualification and independent review. Its authored TypeScript currently grows; removed transport files do not establish net code reduction.
- Direct production `acyclic_fs::exact_u32_from_f64` verification covers arbitrary IEEE-754 bit patterns. Kani reported zero failures across 14 checks; an intentional fractional-acceptance negative control failed. The source-bound verification manifest records the theorem, source hashes, pinned toolchain and raw logs. This proves the specified numeric conversion property only. The separate original Actors solver remains active; no terminal proof result is claimed.
- Kotlin's older producer receipt demonstrates installed runtime cancellation and immutable records, but aliases erase nominal distinctions and `CurrentHead(Boolean)` admits invalid construction. It does not qualify the integrated producer or close the strongest-type requirement. Maintained generator policy work remains assigned.
- Foundation source checkpoint `0f91d7683e68028cf9e4a5b553d58693af447153` and Actors candidate `f8bf8daf64fae0c04366fd1e5326053dde3d4e9e` were verified remotely. Further source and evidence changes require another checkpoint.

## Stream merge admission

PR259 merged at `2026-10-07T13:53:51Z`, producing main
`c8bef0be476cbfd5d4fa30d1f8d017fa5336d964`. Final signed source was
`486bd26ac1f9c8c9a88b13a70bdcf49d5637ecb1`. Exact installed-package
qualification, independent review and required inexpensive CI passed. Root
independently verified the loaded native binary, stalled-TLS abort in 272 ms,
zero remaining sockets, credentials rejected before network and zero unhandled
or uncaught failures. Raw qualification evidence and the merge record are
preserved under `qualified-prototypes/stream-pr259-final486-20261007`.

Filesystem and Actors must preserve this new main during final integration.
The goal remains active: unified generation, all-owner docs profiles, complete
generated snippets, remaining family authority and exact language qualification
are outstanding. No registry publishing or deployment occurred.
