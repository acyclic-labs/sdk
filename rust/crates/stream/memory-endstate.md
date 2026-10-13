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
| Histories and cursors | 270 | 203 | 210 |
| Snapshot codec | 552 | 354 | 550 |
| Native operation context | 0 | 313 | 156 |
| Native entries | 0 | 292 | 160 |
| Prepared completion/recovery | 0 | 434 | 290 |
Main memory.rs production: 1,357; snapshot production: 552.
Memory production is 2,899 versus 2,300; request.rs transfers do not count as deletion.
Largest addition against main: prepared completion/recovery; it was 518 lines.
Prepared ingress and ordinary commits now use one mutation plan and publication function.
Prepared state retains fixed conditions, the key, one funded mutation plan and capacity.
Deleted its duplicate publication path, recovery context and allocation-splitting helpers.
Commit and replay indexes now reference one retained canonical entry.
Snapshot encoding is main's; decode rebuilds from retained facts and verifies the full canonical bytes.
Keep original I/O causes, typed guards, deadlines, exact replay and actual byte/memory limits.
Preissue allocation funds output/recovery vectors and the original future frames.
After issue, only fill existing buffers and consume previously admitted credits.
Packet-owned retention storage is required: abandonment refunds it while the provider stays alive.
Keep behavioral ownership, cancellation, capacity, replay, chronology and corruption tests.
Deleted the duplicate graph decoder/inventory checker, cursor indices and forwarding Deref/header API.
Native reads stage only requested-range windows in one pass; read admission uses one shared function.
Target column totals are an allocation of the 2,300 ceiling, not completion evidence.
Current production remains over the ceiling; no READY claim.
Coordinator owns commits and integration sync under developer lane rules.
One scoped locked stream check per completed batch, through Q:/lanes/slot.mjs.

