# Memory rebuild from main `362947de8`
Counts use rustfmt edition 2024. Main ranges partition production memory.rs; tests excluded.
| Concept | Main lines | Current lines | Target |
|---|---:|---:|---:|
| Imports/module wiring | 29 | 73 | 35 |
| Limits | 32 | 29 | 29 |
| Construction and clocks | 55 | 73 | 60 |
| State and retained data | 40 | 61 | 65 |
| Ordinary provider queries | 198 | 246 | 180 |
| Mutation execution | 313 | 527 | 270 |
| Shared request validation and hashing | 224 | 224 (request.rs) | 200 |
| Capacity checks | 101 | 117 | 95 |
| Replay/retention/identity | 95 | 108 | Included above |
| Histories and cursors | 270 | 188 | 210 |
| Snapshot codec | 552 | 364 | 550 |
| Native operation context | 0 | 313 | 156 |
| Native entries | 0 | 286 | 160 |
| Prepared completion/recovery | 0 | 432 | 290 |
Main memory.rs production: 1,357; snapshot production: 552.
Memory production is 2,817 versus 2,300; request.rs transfers do not count as deletion.
Largest addition against main: prepared completion/recovery; it was 518 lines.
Prepared ingress and ordinary commits now use one mutation plan and publication function.
Prepared state retains fixed conditions, the key, one funded mutation plan and capacity.
Deleted its duplicate publication path, recovery context and allocation-splitting helpers.
Commit and replay lookup scans retained canonical entries; no secondary indexes remain.
Snapshot encoding is main's; decode rebuilds from retained facts and verifies the full canonical bytes.
Keep original I/O causes, typed guards, deadlines, exact replay and actual byte/memory limits.
Preissue allocation funds output/recovery vectors and the original future frames.
After issue, only fill existing buffers and consume previously admitted credits.
Packet-owned retention storage is required: abandonment refunds it while the provider stays alive.
Keep behavioral ownership, cancellation, capacity, replay, chronology and corruption tests.
Deleted the duplicate graph decoder/inventory checker, cursor indices and forwarding Deref/header API.
Finite reads share the history cursor; append-only retention uses the funded vector allocator.
Target column totals are an allocation of the 2,300 ceiling, not completion evidence.
Deleted the unused public preparation builders and their provider-comparison tests (436 to 82 lines).
The provider was rebuilt from main; native finite reads now terminate directly on the first I/O error.
Current memory remains 517 over the ceiling; the rewrite target is not met.
Coordinator owns commits and integration sync under developer lane rules.
One scoped locked stream check per completed batch, through Q:/lanes/slot.mjs.

