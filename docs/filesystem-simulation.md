# Filesystem deterministic simulation

`workspace::simulation::seeded_volume_history` drives the production workspace,
transaction, fork, join and generation code over the real in-memory Streams and
Objects implementations. Storage interception uses the existing filesystem
`Simulation` adapter. Entropy, owned-task scheduling and boundary events come
from `acyclic_native_runtime::testing::Schedule`; there is no subsystem clock,
scheduler or random generator.

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
- A failed durability barrier publishes nothing; a lost acknowledgement is
  reconciled by the original operation identity after reopening. A successful
  commit must never be absent from that operation lookup.
- Paged directory enumeration has exactly the oracle's names, including after
  a failed or abandoned publication.
- Reading a native source file leaves it unresolved in the authored generation.
  Every retained lazy snapshot preserves its authored bytes across later writes.

`seeded_command_range_publications` also drives the production native command
checkout and completion path. Each seeded range edit is checked against a byte
array, and completion must publish that array. Retained commands must observe a
subsequent external publication. This uses the existing mount source and command
publication fixture without requiring a mounted kernel driver.

The default test uses four seeds and sixteen operations per seed. Replay one
seed with `ACYCLIC_FS_SEED=<seed> ACYCLIC_FS_SEEDS=1`. The separate deep target is
`ACYCLIC_FS_SEEDS=<count> bash scripts/sweep-filesystem-simulation.sh`; submit it
only to an idle-priority remote worker. It lowers CPU priority as well. Failures
include the seed. The same locked dependencies and
compiler are required for scheduler replay.

Historical target: commit `2aef6627c360ea70fe544800d656a1af2fcf90ea`
fixed incremental join ancestry that could overwrite a parent's resolution of
an earlier child change. The simulator's incremental join operation exercises
that class. A pre-fix seed result is required before claiming historical proof.

Remaining coverage: physical crash/restart cuts and trace minimization are not
yet established by this test. In-memory handle reopening is not physical crash
durability proof. The existing `publication::commit_recovery` suite separately
checks actual process termination at shared-core physical storage cuts.
