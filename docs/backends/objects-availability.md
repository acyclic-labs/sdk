# Objects availability

`Blobs::availability(digest)` registers an exact immutable identity inside the
provider's authenticated account. Register first, then run the consumer's
existing verification. After `changed().await`, repeat that verification. For
a selected closure, register its selected member identities. A notification is
an invalidation, never proof of presence, integrity, repair or durability.

The opening cursor follows registration. Changes during verification stay
pending and may coalesce. Every reconnect opens a new observation and requires
verification after its opening cursor; an old cursor cannot skip this step.
No event history, timer, readiness probe or per-object task is needed.

The shared immutable engine publishes physical payload completion over memory,
local files and S3, including a same-reference repair returning `AlreadyExists`
without advancing the catalog revision. `Account::availability_source()` is the
trusted physical producer's capability: an external repair service shares that
source and calls `restored(digest)` after making the original bytes available.
It calls `fail()` when its push producer fails. Raw edits by another process or
provider cannot be observed unless their actual repair producer is connected;
a catalog watch cannot replace that connection.

Observers do not retain their producer. Source loss, ambiguous completion and
cancellation before physical acknowledgment terminate observations with
`Unavailable`. An uncertain detached write may still complete; the consumer's
verification decides its result. A failed authority must be reopened by a new
store/source owner, followed by the mandatory subscribe/verify handshake.

gRPC serves the same dispatch under the original account resolver, rechecking
permission before each disclosed frame. The native HTTP client consumes the
canonical length-delimited Protobuf stream at `blobs/availability`; gateways
use `route::invoke_stream` after authentication and retain their original grant
through each disclosure. Both clients retain the original credential and end
idle observations on custodian-provided change events or absolute expiry.
An idle observation has no reconnect timer. Producer/transport EOF and
malformed cursors fail closed. gRPC and HTTP/2 multiplex observations over the
client's shared transport; deploy HTTP/2 for HTTP subscription fanout.

Only actively observed keys occupy the source index. Registration performs one
selected-key lookup and allocates a watch channel only for its first observer.
A physical completion performs one lookup, clones the selected sender and
publishes one scalar cursor without copying object bytes. Notification work is
proportional to that key's subscribers; terminal source failure necessarily
notifies its active observers. Neither path scans object history or unrelated
accounts. The new observation adds no storage read, write or durability barrier.

Run the shared provider suite and physical repair controls with
`cargo test --locked -p acyclic-objects --all-features --lib`. The disposable S3
control uses `STORAGE_S3_*` and runs with `--include-ignored`. Physical controls
pause real payload writes before acknowledgment, subscribe during repair,
verify unchanged catalog revisions, and cancel sent repairs to check terminal
failure. The catalog-only negative control observes the actual unchanged
revision and therefore misses the physical repair.

## Live steps

Connect the deployed repair writer to the same authenticated, scope-filtered
source consumed by the Objects endpoint. With its periodic repair cadence
disabled, subscribe to a missing closure member, restore that member's existing
physical reference without changing its catalog/layout, and verify that the
original consumer's verification resumes. Repeat while registration races the
write, after reconnect, under another account and after dropping the producer.
Only then remove a deployment's repair re-verification timer. Independent S3,
R2 or Tigris writers need that actual push-producer connection; these services'
ordinary object writes do not supply it through this adapter.
