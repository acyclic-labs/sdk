# Agent boundary proofs

The former bounded-slot Kani proof receipts remain in
`rust/proofs/spawn-slots/evidence/`. The slot cap, reservation arithmetic and
its active proof package have been removed together.

`agent_model::spawn_schedules` retains the original production actor-step
schedules, client response-loss replay and child completion during an approval
pause. It now checks that client and recorded spawns both complete beyond the
former child ceiling, including duplicate completion. These deterministic
transition controls do not qualify provider crash recovery or hosted endpoints.

The generated `ExplicitStreamProvider` removes defaults from the canonical
provider declaration. Missing adapter implementations fail compilation,
including future default methods. The property exercises every currently
supported attested write family and the prepared-commit refusal. Its internal
probe checks the canonical digest at the bound provider and captures original
local/admitted authority on exact retries. This verifies adapter forwarding;
an external `Attestor` must still satisfy its documented deterministic-function
contract. The admitted-authority probe is not a deployed Cloud receipt.

The record property includes every agent inbox/status/closed/conversation
variant, maximal bounded field lengths, selected child populations and paths
across the former ceilings, largest counters, every outcome, and maximal JSON
escape expansion.
It checks canonical bytes before the bounded encoder's refusal. An oversized
record refused by the encoder therefore fails the property. The sampled settled checkpoint and actor envelope retain their byte-size check.

`rust/proofs/spawn-slots/proofs.json` declares actual production mutations. The producer changes one
production file in a job-owned derivative and checks its complete tracked
input manifest before compiling; the tests and proof harnesses stay unchanged.
It records commands, toolchain, raw output hashes, timings, mutation hashes and Cargo-selected executable hashes.

Install the pinned Rust toolchain. Run `python3 rust/proofs/spawn-slots/run.py MODE --evidence /path/to/new/output` from a clean checkout, selecting `rust`, `mutants` or `finish`. Each invocation requires a fresh evidence directory. `remaining` resumes only the compiler check and missing-method control after the captured successful doctests; it does not replay the positive properties.

Main's correlated-turn revision adds WaitingHost. Its host must satisfy the same
1–128 byte, non-control name bound as inbox host names. The transition turns an
invalid driver wait into a bounded failure before publishing its effects. The
maximum-record property includes the new phase and conversation inbox sequence.

The versioned admission manifest retains the original SHA256 and captured
archive hashes.
