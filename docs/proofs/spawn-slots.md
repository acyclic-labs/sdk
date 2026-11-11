# Agent boundary proofs

The fixed 256-child slot arithmetic and its Kani crate are retired. Child
admission no longer reserves or reclaims slots. Persistent membership roots
and counts keep checkpoints independent of width; child-node IO is paged.
The historical proof manifest retains the retired component's evidence and
does not qualify the current uncapped mechanism.

`agent-model::slot_schedules` executes the production serialized actor step:
60 pause/client/resume schedules, client response-loss replay, and completion
of an already-open recorded child during an approval pause. Production actor
steps commit serially; this is a deterministic transition simulation rather
than a model of different concurrency primitives. It does not qualify a
provider's crash recovery or hosted endpoint.

The completion burst control leaves the parent waiting after N-1 children,
then resumes it on the final original completion. Wait telemetry retains a
cursor into original status facts instead of a per-child checkpoint vector.
The native Actors control admits 65 held deliveries under default scheduling
and exactly 64 under explicit credit, then settles every original input.
Forced team reservations separately count lifetime admissions; worker
settlement never replenishes that user-selected reservation.

The generated `ExplicitStreamProvider` removes defaults from the canonical
provider declaration. Missing adapter implementations fail compilation,
including future default methods. The property exercises every currently
supported attested write family and the prepared-commit refusal. Its internal
probe checks the canonical digest at the bound provider and captures original
local/admitted authority on exact retries. This verifies adapter forwarding;
an external `Attestor` must still satisfy its documented deterministic-function
contract. The admitted-authority probe is not a deployed Cloud receipt.

The record property includes every agent inbox/status/closed/conversation
variant, maximal bounded field lengths, representative long paths, largest
counters, every outcome, and maximal JSON escape expansion.
It checks canonical bytes before the bounded encoder's refusal. An oversized
record refused by the encoder therefore fails the property. The settled
checkpoint and actor envelope bound retain their separate maximum-size test.

`rust/proofs/spawn-slots/proofs.json` retains historical production mutations. The historical producer changes one
production file in a job-owned derivative and checks its complete tracked
input manifest before compiling; the tests and proof harnesses stay unchanged.
It records commands, toolchain, raw output hashes, timings, mutation hashes and Cargo-selected executable hashes.

Current checks use `cargo test --locked -p acyclic-harness-core --test main agent_model::`
and `node scripts/qualify-topology-modes.mjs GRAPHCODER MOCK_MODEL FRESH_OUTPUT`.
The latter exercises the shipping binary beyond the retired width/depth caps
and runs contrary-policy negative controls against the same assertions.

Main's correlated-turn revision adds WaitingHost. Its host must satisfy the same
1–128 byte, non-control name bound as inbox host names. The transition turns an
invalid driver wait into a bounded failure before publishing its effects. The
maximum-record property includes the new phase and conversation inbox sequence.

The versioned admission manifest
retains their SHA256 hashes and the original captured archive hashes.
