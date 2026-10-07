# Actors cutover rerun scope

The long Actors launch was source-bound to Q snapshot 03bbf867c32ab61dfb262ab20fdf9d31a4e6dae1, whose checkout was dirty. Its live handle is PTY 17660 / CBMC 1031718. The later Actors semantic cutover checkpoint edcc1292ac2363d2ca62d24c7e9033279b32c087 is a different source and cannot inherit that result.

After an explicit clean source freeze at the new checkpoint, rerun every actual production harness whose implementation closure can change:

- SubscriptionStart conversion: cursor and current-head optional presence, valid oneof identity, missing/false oneof rejection.
- SubscriptionObservation::try_from: delivered/completed/recoverable full-u64 fields, failed_cursor Option presence/value, and unknown state rejection.
- ActorObservation::try_from: full-u64 checkpoint/epoch/configuration_revision, optional checkpoint presence, fixed 32-byte digest, unknown state rejection.
- CreateActorResponse and UpdateActorResponse conversion: optional actor None/Some presence and roundtrip.
- UpdateActorRequest::try_from: expected_configuration_revision preservation through the actual production validation/conversion path; record the admission preconditions separately.
- PositiveU64 and ActorLimits constructor/conversion obligations, including zero rejection and presence.
- Enum numeric mappings and inverse/unknown behavior.
- Digest predicate/constructor obligations: exact 32-byte admission and roundtrip. The later symbolic length predicate harness added after the launched snapshot must be included in the refreshed command.

A refreshed receipt must enumerate the exact source files, dirty state, command, raw log hash, check counts, and terminal exit. Existing 03bb results remain historical to that snapshot; the filesystem float theorem in this artifact is independent.

