# Current Actors cutover proof preparation

This artifact records a source-bound preparation against
Q:\sdk\work\sdkgen-main-port-current. It is not a Kani result. The checkout is
actively changing, so audit/current-source-inventory.json is a capture at one
observation time and an explicit source freeze is required before running.

The current production domain is exposed through acyclic_actors::domain.
The prepared nominal-ID harness calls ActorId::try_from, ActorId::as_str, and
From<ActorId> for String directly. It does not copy the constructor or
conversion algorithm. Existing current production Kani harnesses cover the
u64, nonzero, and oneof obligations listed in theorem-map.md; they must be
rerun from the frozen current source and must not inherit the old 03bb receipt.

The original 03bb snapshot and its receipt are retained for historical
comparison only. They are not evidence for this current checkout.

Selected current-source rerun status:
the original E0583 module-path blocker is preserved in
receipt.current-blocked.json. After the source-owned Kani-only path attribute
was added, receipt.current-fixed.json records successful PositiveU64 and
SubscriptionStart oneof proofs under their fully qualified inline-module names.
