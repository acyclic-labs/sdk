# Filesystem deterministic simulation

`workspace::tests::simulation::seeded_volume_history` drives the production workspace,
transaction, fork, join and generation code over the real in-memory Streams and
Objects implementations. Entropy, owned-task scheduling and boundary events
come from `acyclic_native_runtime::testing::Schedule`; there is no subsystem
clock, scheduler, fault injector or random generator.

Storage faults use the same Schedule. `acyclic_fs::simulation` wraps an
authority and an object store so that each operation is a logical site
(`simulation::sites`). A test plans a `SimulationFault` with
`Schedule::inject(site, ordinal, fault)` or `Schedule::inject_next`. The
schedule counts calls, records `Schedule::site_calls` and holds a paused call
at a `Boundary` that the driver releases or drops. The wrapper only defines
what each fault does to a call. A fault planned at a site that cannot apply
it fails the call, so a plan cannot silently inject nothing.

The oracle is an independent map of acknowledged file contents. After every
operation, both the current workspace and its independently reopened immutable
generation must agree with it. The obligations are:

- Acknowledged publications survive reopening; retrying the same candidate
  returns the same generation.
- Candidates prepared against one head cannot silently overwrite a winner.
- A fork initially has exactly its parent's contents.
- Disjoint child and parent changes merge to their union. Further incremental
  joins preserve parent resolutions of previously imported child changes.
- Divergent edits conflict without changing the target generation.
- Dropping an unpublished candidate changes no durable state.
- Paged directory enumeration has exactly the oracle's names, including after
  a failed or abandoned publication.
- Reading a native source file leaves it unresolved in the authored generation.
  Every retained lazy snapshot preserves its authored bytes across later writes.

Volume operations are generated as explicit actions before production code
runs. Their ordinal, path, payload and race order remain fixed when actions are
removed. This lets the shared minimizer shrink a history without redrawing later
operations or renaming fork paths. The failure output retains the action data
as well as the seed.

`seeded_command_range_publications` also drives the production native command
checkout and completion path. Each seeded range edit is checked against a byte
array, and completion must publish that array. Retained commands must observe a
subsequent external publication. This uses the existing mount source and command
publication fixture without requiring a mounted kernel driver.

`publication::commit_recovery::history::seeded_physical_crash_history` uses the
existing cold-process driver with seeded multi-transaction histories. The
production local backend runs eight atomic two-file publications. The shared
Schedule cuts an original file operation before execution, after execution or
after a torn prefix, and the process exits before releasing its acknowledgement.
A separate process reopens the volume and reconciles the last operation by its
original idempotency key. The whole namespace must match either the acknowledged
map or the pending map, never a partial mixture. An acknowledged operation must
retain its lookup identity. A new fork and join must succeed after recovery
without losing previously committed files.

All three histories use `acyclic_native_runtime::testing::sweep`. Default volume
and command runs use four seeds; worker restart uses two. A volume history has
sixteen operations. `DST_SEED=<seed>` replays one world; `DST_SEEDS=<a>..<b>`
selects a deep range. The separate deep target is
`DST_SEEDS=0..10000 bash scripts/sweep-filesystem-simulation.sh`; submit it only
to an idle-priority remote worker. It lowers CPU priority as well and selects
only these seeded filesystem histories. Each sweep reports its elapsed time.
The same locked dependencies and compiler are required for scheduler replay.

A failed volume history uses the shared `minimize` function to remove actions
while preserving the original invariant report. Error results and panics pass
through the shared sweep capture on every replay. The failure includes its
one-minimal action list and the standard `DST_SEED` replay command. Lazy overlay
payloads also come from that explicit list, so shrinking does not redraw them.

Historical target: commit `2aef6627c360ea70fe544800d656a1af2fcf90ea`
fixed incremental join ancestry that could overwrite a parent's resolution of
an earlier child change. The simulator's incremental join operation exercises
that class. A pre-fix seed result is required before claiming historical proof.

Execution and timing evidence, including the historical negative/positive
comparison, must be recorded before treating these obligations as proved.
