# Observation and revision proof design

This is a source-only design note. It is not an executed receipt and does not
change `domain/kani_proofs.rs` while the compiled fixture is frozen.

## Universal field model

The next selected cohort should prove wire-to-domain-to-wire preservation for
each raw unsigned field below:

| Projection | Fields | Symbolic domain |
| --- | --- | --- |
| `SubscriptionObservation` | `delivered_cursor`, `completed_cursor`, `recoverable_cursor` | independent arbitrary `u64` values |
| `SubscriptionObservation` | `failed_cursor` | arbitrary presence bit plus arbitrary `u64` payload |
| `ActorObservation` | `checkpoint_unix_millis` | arbitrary presence bit plus arbitrary `u64` payload |
| `ActorObservation` | `checkpoint_epoch`, `configuration_revision` | independent arbitrary `u64` values |
| `UpdateActorRequest` | `expected_configuration_revision` | arbitrary `u64` value |

For each row, the harness should construct the real generated wire message,
run the existing `TryFrom` implementation, inspect the typed value, convert it
back with the existing `From` implementation, and assert exact equality. This
proves both full-width preservation and `Option` presence state without a
second validator or a narrowed integer model.

## Fixture assumptions

The observation harness should use fixed valid values for the unrelated fields:
non-empty actor/subscription IDs and paths, a fixed non-zero 32-byte digest,
empty subscription collections, and a known enum value. A separate branch may
quantify the raw enum integer and assert the existing unknown-value error, but
that is already covered by the enum mapping harness.

The request harness should use a fixed non-empty actor ID, valid digest, empty
bindings, positive limits, and a short valid idempotency key. Only
`expected_configuration_revision` is symbolic. These assumptions isolate the
revision preservation property from aggregate collection and string bounds.

The option model must build both `None` and `Some(symbolic_u64)` paths from an
unconstrained presence bit. The assertion must compare both the payload and
the presence bit after conversion; checking only the numeric payload would
miss an absent-versus-zero regression.

## Scope boundary

This cohort would qualify lossless semantic projection of the listed fields.
It would not prove arbitrary string admission, collection uniqueness or
limits, service transport, generated TypeScript behavior, or FFI ownership.
Those require separate models or empirical/maintained-boundary evidence.
