
# Storage and crash recovery

New journals use `ACYCLIC-STREAM-LOCAL-V2\0`. Snapshots retain the
`ACYCLIC-STREAM-SNAP-V01\0` container and canonical V1 state. Both containers
bind eight little-endian limit words and a journal epoch. Command frames retain
their four-byte length, eight-byte Unix commit timestamp, canonical protobuf and
SHA-256 checksum. No shared wire command or browser format changes.

A shared journal flush writes one 78-byte checksummed group seal before syncing.
Its zero timestamp, local tag 6, epoch, group start, seal position and cumulative
command count bind its boundary. Seals count toward journal bytes, not commands.
Raw command appends continue while an earlier group is flushing. Preparing a
mutation reserves room and native IO funding for its eventual seal.
For deferred native responses, one outstanding seal reservation can outlive the
accepted response until a later flush, replacement, compaction or provider drop.
Late cancellation cannot revoke that accepted mutation or prevent its funded IO.

Recovery validates the semantic prefix before disk repair. After a damaged
frame, later raw frames and the same group's seal can be unacknowledged writes
that persisted out of order. A valid position-bound seal starting after the
damage proves a preceding flush completed and makes that damage fatal. Otherwise
the damaged suffix is truncated. A recovered unsealed prefix is sealed and
synchronized before startup exposes state. Semantic violations fail closed
without authorizing repair.

Under the configured device's successful full-flush guarantees, acknowledged
commands survive power loss. Recovery cannot distinguish a torn final group
from subsequent media corruption of that group, so such damage can discard
multiple commands. A later group's seal proves corruption in the earlier group.
Checksums detect damage; fabricated valid seal bytes can conservatively prevent
recovery. This format does not authenticate hostile storage. The barrier policy
retains its existing weaker device-ordering semantics.

Only V2 headers are accepted. Unsupported versions fail closed without changing
the existing journal or creating a migration snapshot.
