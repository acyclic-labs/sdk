# Actors and Objects simulation properties

The simulation uses `acyclic_native_runtime::testing::Schedule` and
`seeded_runtime`. Operation choices draw from `fill_seeded`; there is no
subsystem clock, scheduler or random generator. Replay requires the same seed,
locked dependencies and compiler. Failures report their seed.

## Actors

The persisted checkpoint, subscription cursor, alarm generation and delivered
generation, output records, child creation and companion records describe one
atomic prefix. An unacknowledged delivery can have committed or not committed;
reopening that prefix must never duplicate its effects. A definite refusal must
leave its input available. A committed alarm generation fires once, including
after reopening. Composition preserves the original actor publication and can
append wakes for several children even when they share a scheduling shard.
Admission close rejects new work while preserving already accepted work.

`local::model::simulation` currently samples dropped submissions and lost
acknowledgements around the existing production Store script and checks its
tuple after each publication attempt. This is logical recovery over MemoryStream,
not physical restart proof or full Runtime coverage.

## Objects

Every verified put names the digest of its input. Every batch has one result per
input in original order, including duplicate digests and references to earlier
members. Every acknowledged object remains readable with exactly its original
bytes. A valid half-open range returns precisely that slice. Empty non-full
ranges are refused. Failures must not reorder other members or acknowledge
unrecoverable data.

`tests::simulation` currently checks the acknowledged prefix after each generated
put batch over the real Store and MemoryMedium. Physical storage faults,
reopening LocalMedium and mixed-error batches still need coverage.

## Execution and remaining proof

The default tests run four seeds with bounded histories. The separate ignored
`deep_` tests enumerate 4096 seeds and are intended only for idle workers.
Neither sweep is a substitute for historical negative controls.

Historical targets are the shared scheduling-shard composition fix and a second
independently reproduced Actors or Objects fix. Both need an exact pre-fix
revision, failing seed, trace and matching passing fixed revision. These results
have not yet been established by these drivers.

Shared-core seams still needed: a reusable boundary driver with crash/restart
outcomes, network fault selection and trace minimization. Consumers should use
the shared implementation as it becomes available rather than implement these
mechanisms here.
