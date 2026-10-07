# Generational handle registries for a future C ABI

Status: architecture research only. This note does not add a C ABI, a foreign
pointer contract, or a production dependency.

Date: 2026-10-07

Rust source snapshot reviewed: `371bb4170e16aca973176b6756a261ee5add7297`.

## Qualification boundary

A generational arena solves one storage problem: a stale `(slot, generation)`
must not resolve to a later value in the same registry. It does not by itself
prove a C handle ABI. The future ABI must separately prove:

- the handle carries a registry/type domain and can only be issued by Rust;
- lookup checks the slot, generation, live state, and domain before borrowing;
- a copied integer is not silently treated as a second ownership token;
- every explicit clone mints one new release token, and every release token can
  be consumed at most once;
- the last release drops the value exactly once;
- generation exhaustion retires a slot or returns a typed error before reuse;
- no operation dereferences an arbitrary foreign pointer or trusts a raw value
  reconstructed from untrusted bits; and
- synchronization, panic containment, ABI calling conventions, and shutdown
  are specified separately from the single-thread state machine below.

These are obligations of a Rust-owned facade. They are not implied by a crate
whose key type implements `Copy`.

## Maintained crate comparison

### `slotmap` 1.1.1

`SlotMap` provides O(1) insertion, removal, and lookup with stable keys, and
its `new_key_type!` macro makes handles for different maps distinct Rust types.
Its lookup compares the key's index and version, so the basic ABA check is a
useful internal primitive ([crate documentation](https://docs.rs/slotmap/1.1.1/slotmap/),
[source](https://docs.rs/crate/slotmap/1.1.1/source/src/basic.rs)).

It is not sufficient as the C ABI registry by itself. Removal increments the
slot version with `wrapping_add(1)` ([source](https://docs.rs/crate/slotmap/1.1.1/source/src/basic.rs#L2068-L2080)),
and the crate documentation explicitly describes a possible stale-key match
after `2^31` delete/reinsert cycles of one slot. There is no
`GenerationExhausted` result. `SlotMap` also owns values, but does not define
clone/release tokens or exactly-once foreign ownership.

The smallest safe use is therefore as private storage under a Rust registry:
use a custom key type, never expose its raw representation, and maintain a
separate per-slot lifecycle/lease record. If the wrapper's generation is about
to exhaust, permanently retire the slot (the private wrapper may use
`detach`, which removes a value without returning its slot to the free list),
or return a typed exhaustion error. That wrapper is material semantic state;
this is more than a newtype conversion.

### `generational-arena` 0.2.9

`Arena` is a maintained, `no_std`, zero-`unsafe` generational store. Its
`get`/`remove` paths compare an index's generation with the occupied entry,
and stale indices return `None` ([module documentation](https://docs.rs/generational-arena/0.2.9/generational_arena/),
[source](https://docs.rs/crate/generational-arena/latest/source/src/lib.rs#L1888-L1993)).
That is a good reference implementation for the basic stale-handle property.

It is a poor direct FFI handle type. `Index` is `Copy`, and its documented
`from_raw_parts` accepts arbitrary values but warns that malformed indices can
ultimately panic ([Index API](https://docs.rs/generational-arena/0.2.9/generational_arena/struct.Index.html#method.from_raw_parts)).
The arena keeps one global `u64` generation and increments it with ordinary
arithmetic on removal; there is no fallible exhaustion protocol in the public
API ([source](https://docs.rs/crate/generational-arena/latest/source/src/lib.rs#L1560-L1575),
[source](https://docs.rs/crate/generational-arena/latest/source/src/lib.rs#L1888-L1902)).
The wrapper would still need to reject raw parts, add a registry domain, add
lease tokens, and define retirement before overflow.

### `thunderdome` 0.6.1 and typed arena variants

`thunderdome` has a compact generational `Index`; its arena checks the stored
generation and stale keys fail lookup, while `Index` is `Copy` and the crate
does not provide ownership-release semantics ([crate documentation](https://docs.rs/thunderdome/0.6.1/thunderdome/),
[Index documentation](https://docs.rs/thunderdome/0.6.1/thunderdome/struct.Index.html)).
It is a reasonable internal arena candidate, but it does not close the C ABI
obligations. The `typed-generational-arena` documentation is even more
explicit that generation increment “may wrap or panic on overflow,” which is
incompatible with a required error-before-reuse rule
([trait documentation](https://docs.rs/typed-generational-arena/0.2.9/typed_generational_arena/trait.GenerationalIndex.html)).

## Recommendation

For a future C ABI, a small Rust-owned registry is the smallest auditable
semantic core. A dependency may provide storage, but it should not own the
ABI contract. `slotmap` 1.1.1 is the best maintained storage candidate when
custom key types and O(1) lookup are useful; its documented version wrap means
the wrapper must add an independent generation/retirement record. If adding
that side state and proving `detach`-based retirement is less code than
maintaining a direct vector of slots, use `slotmap` internally. Otherwise a
custom registry is clearer and avoids relying on hidden generation bits.

`generational-arena` is attractive for a no-`unsafe` prototype, but its global
generation and raw-parts API make it a weaker basis for a strict C handle
contract. None of these crates should be selected as evidence that an
arbitrary `u64` or foreign pointer is valid.

## Pure state model and proof obligations

The state machine to prove before any ABI implementation is:

```text
Slot = Vacant { next_generation }
     | Live { generation, object, leases }
     | Retired
Handle = { registry_tag, slot, generation, lease_id }
```

`next_generation` is never zero and is never allowed to wrap. Allocation
selects a vacant slot, checks that advancing its generation is representable,
and creates exactly one live lease. A generation overflow transitions the slot
to `Retired` or returns `GenerationExhausted`; it never creates a new live
value with an old generation.

Lookup succeeds only for a matching registry tag, slot, generation, and live
lease. A released lease is removed from the lease set before any destructor
can run. `clone` is an explicit registry operation that mints a fresh
`lease_id` and increments the lease count. `release` consumes exactly that
lease id; repeating the same call returns `AlreadyReleased` and cannot lower
the count again. When the count reaches zero, the value is dropped once and
the slot becomes vacant or retired.

This separate `lease_id` is necessary. A `(slot, generation)` key with a
refcount cannot distinguish two legitimate shared releases from the same
integer being released twice. Treating every copied C integer as an implicit
clone would make exactly-once release unprovable.

The first pure harness should cover: stale lookup after reuse; forged domain,
slot, generation, and lease rejection; clone then two releases; repeated
release; final-drop count; move without count change; generation exhaustion and
permanent retirement; allocation failure leaving the old state unchanged; and
registry shutdown refusing new lookups. A separate concurrent harness is
required for atomic lease operations. No harness here proves arbitrary foreign
pointer validity, allocator compatibility, callback safety, or panic/unwind
behavior across an ABI boundary.

