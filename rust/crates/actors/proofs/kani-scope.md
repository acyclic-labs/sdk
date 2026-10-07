# Actors proof scope

This catalog separates mathematical obligations from empirical qualification.
Receipts are source-bound: the eleven-harness receipt predates the targeted
required-message additions, while `kani-required-message-targeted.json` covers
the current `kani_proofs.rs` hash and only its two harnesses. They must not be
combined as one current receipt without rerunning the full set.

## Selected obligations for mathematical domain qualification

These are the selected mathematical obligations behind a strongest typed
domain qualification, rather than a claim that every runtime behavior needs a
theorem:

- Digest admission must quantify over the accepted length and reject the
  all-zero value; accepted bytes must be preserved.
- Every nominal unsigned value used at a semantic boundary must preserve its
  full `u64` range, with an explicit proof of any zero/nonzero predicate.
- Every optional message and protobuf oneof selected for the public semantic
  projection must preserve presence, absence, variant identity, and payload
  values. Required nested messages need an explicit rejection proof.
- Every published numeric enum mapping selected for the public projection must
  be inverse and lossless for known values, while unknown values remain
  observable errors.
- Generated contract data requires an injectivity/identity argument before it
  can be called a mathematical qualification. Repeated rendering is useful
  evidence but is not that proof.

## Completion workplan

Already proved by the source-bound receipts:

- `PositiveU64`, all three `ActorLimits` fields, and
  `SubscriptionStart::Cursor` over symbolic full-width `u64` values.
- The exact 32-byte nonzero digest predicate and valid-byte preservation, with
  explicit representative malformed lengths; this is not a theorem over every
  possible malformed slice length.
- The three selected numeric enum mappings over symbolic full-width `i32`
  values, including unknown-value errors.
- Required `limits` rejection for fixed otherwise-valid create and update
  fixtures; this is a concrete regression proof, not a universal request
  theorem.

The smallest remaining selected domain cohort is:

1. Symbolic round trips for the unproved observation and revision `u64`
   fields, including `failed_cursor` and `checkpoint_unix_millis` presence.
2. A targeted absent-subscription proof for `AddSubscriptionRequest`, using a
   fixed valid actor/idempotency fixture as with the current required-message
   proofs.
3. A bounded response-wrapper proof for absent and present optional Actor
   observations, preserving the option state and nested typed values.
4. A separate source-to-artifact identity argument for generated contract
   data; the renderer repeat-output test and descriptor compatibility test
   remain empirical guards until that argument exists.

ID spelling, arbitrary string contents, collection uniqueness/limits, route
metadata, transport, cancellation, packaging, and FFI ownership are optional
strengthening or integration qualifications. They must not be reported as
mathematically proved by this domain cohort unless separately modeled and
verified.

The current targeted receipt proves only that absent `limits` is rejected by
the canonical create and update validators for these fixed, otherwise-valid
fixtures. It is a concrete required-field regression proof, not a universal
quantification over arbitrary requests. It does not quantify over strings,
collections, aggregate limits, or operation metadata; a universal theorem
would require a symbolic request model for those fields.

## Empirical and maintained-boundary evidence

Transport behavior, generated SDK behavior, packaging, cancellation, and FFI
ownership are qualified by source-bound integration tests and maintained
binding guarantees. They are not inferred from these pure domain proofs. The
renderer repeat-output test is an empirical regression guard; it does not
prove universal generator determinism or generated-data injectivity.
