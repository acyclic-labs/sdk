# Actor timers use the checkpoint and one alarm

An actor has one durable alarm. The artifact keeps its logical timers in its own checkpoint, with a stable identity and deadline for each entry. A transition that inserts, replaces or cancels an entry publishes the updated checkpoint together with `guest::AlarmChange::Set` for the earliest remaining deadline, or `Clear` when none remain. Alarm delivery consumes only the due entries and rearms the next deadline in the same transition. `Keep` preserves the existing alarm when the timer set does not change. A stale generation cannot consume a replacement alarm.

This transition contract does not require a second timer registry or timer stream append. The Rust `timers::schedule`/`set_alarm` callback, TypeScript `schedule`/`setAlarm` callback and `TimerRecord` export, and WASM `timerRecord` encoder, are retired. Applications migrate their actual pending entries into their artifact checkpoint and return the corresponding alarm change. They must preserve distinct logical identities rather than replacing several timers with one anonymous deadline.

The existing Rust `timers` codec and inbox delivery helper remain live for the harness generic heap layout and diagnostics, the frozen timer corpus, Loom proof and independent timer baseline oracle. Their retirement is a subsequent source composition, not implied by retiring the scheduling callback. Useful native process/readiness helpers remain independent.

## Validation and measurement plan

The bounded SDK caller census covers Actors, Actors WASM, Machines, Machines WASM, Stream, Stream NAPI, and their TypeScript packages. The retired callback's only callers in those roots were its public wrapper and tests. This does not attest absence in external applications or deployed Cloud artifacts.

Before claiming runtime or performance parity, exercise two separately identified deadlines, replacement and cancellation of one entry, early delivery, stale-generation delivery and an unknown checkpoint publication. The negative control must drop the earlier entry or skip rearming and fail the same trace. Record actual alarm callbacks, publication requests and retained checkpoint bytes at injected RTT; no performance improvement is claimed from deleting the old export.

For the parallel Stream binding cut, record time and allocations to first yielded record and complete page, physical HTTP calls and credential callbacks. An invalid read must refuse at canonical admission even if a later append grows the stream; a prefix followed by an error must fail the iteration. Byte, enum, u64, error and applied/durability watermark parity remain required at the transport seam. No handwritten external-service fixture substitutes for a captured provider response.
