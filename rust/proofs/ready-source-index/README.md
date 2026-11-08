# Fixed-width source lookup

```sh
node rust/proofs/qualify.mjs ready-source-index
```

The shared source scheduler uses `acyclic_collections::FixedIndex` for the
original 64-bit channel identity. Queue links and provider wake identities use
the same index; no ordinal table or second channel registry is constructed.
The collection's strictly increasing branch coordinates bound an exact read
by 64 branches, one leaf and eight key bytes, independently of source count.
Registration creates at most a leaf and branch; extraction allocates nothing.
Registration remains owner-constructed, as its existing stream and wake owners
were, without introducing an accounting owner or copying admission authority.

The existing production scheduling property retains actual MemoryStream
append/read output. Disabled original record cursors occupy every bit of the
channel key while the delivered cursor traverses the complete zero-key path.
The same property checks exact delivery, zero scheduling allocations, source
extraction, stale callbacks and publication to original pending follows. Its
original queue/credit property and Loom publication schedules consume the same
index; there is no copied radix implementation or scheduler model.

The retained mutation repeats the actual mutable lookup on its original
observation sink. The unchanged key-width bound must fail before restoration;
the restored scheduling property must pass. This negative checks redundant
physical traversal, not elapsed time or helper-call counts.

This component closes population-dependent lookup in the source scheduler.
It does not certify provider work, transport buffering, physical platform
acceptance, or the allocation behavior of the separate wake-hint queue. No
thread, timer, wait, payload copy, durability operation or round trip is added.
