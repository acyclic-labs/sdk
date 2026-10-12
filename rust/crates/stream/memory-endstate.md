# Memory rebuild from main `362947de8`
Counts use rustfmt edition 2024. Main ranges partition production memory.rs; tests excluded.
| Concept | Main lines | Current lines | Target |
|---|---:|---:|---:|
| Imports/module wiring | 29 | 73 | 35 |
| Limits | 32 | 29 | 29 |
| Construction and clocks | 55 | 75 | 60 |
| State and retained data | 40 | 52 | 65 |
| Ordinary provider queries | 198 | 264 | 180 |
| Mutation execution | 313 | 574 | 270 |
| Shared request validation and hashing | 224 | 224 (request.rs) | 200 |
| Capacity checks | 101 | 117 | 95 |
| Replay/retention/identity | 95 | 119 | Included above |
| Histories and cursors | 270 | 252 | 210 |
| Snapshot codec | 552 | 544 | 550 |
| Native operation context | 0 | 301 | 156 |
| Native entries | 0 | 360 | 160 |
| Prepared completion/recovery | 0 | 486 | 290 |
Main memory.rs production: 1,357; snapshot production: 552.
Memory production is 3,246 versus 2,300; request.rs carries 175 transferred lines.
Largest addition against main: prepared completion/recovery; it was 518 lines.
Prepared ingress builds its fixed append directly with main's record and history functions.
Prepared state retains fixed conditions, the key, funded output/history and capacity.
It no longer builds a generic commit plan or disposable path/history staging vectors.
Commit and replay indexes now reference one retained canonical entry.
The snapshot encoding remains main's encoding, with unchanged semantic validation.
Keep original I/O causes, typed guards, deadlines, exact replay and actual byte/memory limits.
Preissue allocation funds output/recovery vectors and the original future frames.
After issue, only fill existing buffers and consume previously admitted credits.
Abandonment releases promised capacity and unused retention storage.
Keep behavioral ownership, cancellation, capacity, replay, chronology and corruption tests.
Delete forwarding types, duplicate data stores and implementation-pin tests.
Retention buffer allocation now uses one path for ordinary and prepared mutations.
Target column totals are an allocation of the 2,300 ceiling, not completion evidence.
Current production remains over the ceiling; no READY claim.
Coordinator owns commits and integration sync under developer lane rules.
One scoped locked stream check per completed batch, through Q:/lanes/slot.mjs.

