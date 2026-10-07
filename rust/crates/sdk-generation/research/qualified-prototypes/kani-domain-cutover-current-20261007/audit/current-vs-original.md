# Current canonical domain versus original actual03bb snapshot

The current canonical checkout observed at capture has revision
0093da12d7d4205d56db1c25fb75d50b59de11d6; the original proof snapshot was
03bbf867c32ab61dfb262ab20fdf9d31a4e6dae1. The current checkout is a later
derived producer and its domain files differ materially from the copied 03bb
source. The exact current and historical hashes are in
audit/current-source-inventory.json.

Relevant current production symbols:

- ActorId::new, TryFrom<String> for ActorId, ActorId::as_str, and
  From<ActorId> for String implement the non-empty nominal identity boundary.
- PositiveU64::new, TryFrom<u64> for PositiveU64, and
  From<PositiveU64> for u64 implement the nonzero boundary.
- SubscriptionStart::try_from and its generated From conversion preserve
  Cursor(u64) versus CurrentHead(true); missing and false current-head
  payloads are rejected.
- SubscriptionObservation::try_from and its generated From conversion
  preserve delivered, completed, recoverable, and optional failed cursors.
- ActorObservation::try_from and its generated From conversion preserve
  optional checkpoint time, checkpoint epoch, and configuration revision,
  subject to the production digest and enum checks.
- Generated CreateActorResponse::try_from and UpdateActorResponse::try_from
  preserve None versus Some(ActorObservation).

The current domain/kani_proofs.rs already contains production harnesses for
the positive-u64, subscription oneof, subscription-observation u64/presence,
actor-observation u64/presence, optional actor-response, and enum obligations.
This artifact adds only the missing ActorId preparation and a precise rerun
map; it does not add a second conversion implementation.

No theorem here claims arbitrary string behavior, transport behavior, whole
message cloning, or a proof of the moving checkout before source freeze.
