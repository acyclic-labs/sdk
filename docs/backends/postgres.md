# PostgreSQL Stream

The OSS Actors server accepts this concrete storage composition. Full endpoint
observation and reopen remain unqualified and are listed under Live steps;
missing filesystem resource follows fail closed. Historical emulator receipts
below remain tied to their recorded source. The canonical prepared-decision
correction landed with Streams surface PR #1705.

Enable `acyclic-stream/postgres` and call
`postgres::PostgresStream::connect(connection_string, namespace)`. Each namespace
is an independent Stream provider in the caller-owned database. The adapter
installs `acyclic_stream_journal` and its namespace/revision index. The database
role needs schema DDL permission because connect runs idempotent installation,
plus SELECT/INSERT, sequence access, advisory locks and LISTEN/NOTIFY. Use a local
Unix socket or an authenticated TLS proxy;
the initial adapter connection uses `NoTls`.

Namespace names have no SDK length ceiling. Connect hashes the name once into a
32-byte journal key and retains the complete name in its generation row. The
generation upsert compares the exact spelling under the `C` collation, so a key
collision refuses rather than reopening another namespace. Journal indexes,
advisory locks and notifications use the fixed key; diagnostic connection names
use a non-authoritative key prefix. Warm operations do not copy or transmit the
original name. This is the v1 schema; existing databases need fresh tables.

Pass `Arc<PostgresStream>` to `StreamAuthorityStore::new`, combine with
`LogicalObjectStore::new(Arc::new(Store::new(S3Medium::new(bucket, prefix, capabilities)?).account(account)?))`,
and supply both to `Fs::new` with durable embedded capabilities. Enable
`acyclic-objects/s3` for the S3 medium; its endpoint capabilities and qualification
requirements are documented in [S3-compatible blob storage](s3.md).
Filesystem does not inspect backend types.

Postgres commits an ordered command journal under a transaction-scoped namespace
advisory lock. Connect sets READ COMMITTED for this session, including databases
whose default is REPEATABLE READ. Catch-up reads a bounded snapshot without
holding the publication fence. The canonical shared Stream core owns append-if-tail CAS, exact
idempotency, hierarchical fork/commit, filters and bounded public paging. Journal
rows bind commands to the database acceptance clock, so reopening preserves record times and retry
receipts. Publication survives caller cancellation; only committed commands enter
the visible core. The shared `MemoryStream::apply_durable` prepares the canonical
decision before invoking SQL publication and installs accepted state only after
the server's implicit COMMIT. Refused conditions do not publish a journal row; successful same-key
replays reuse their retained outcome. The historical runtime receipts below
qualify their exact source, rather than every subsequent change to this path.
SQL synchronous commit must remain enabled for durable acknowledgements.

`acyclic_stream_publish(namespace, expected_revision, command_bytes[], deadlines[])`
is the physical publication primitive for one frame or an ordered batch. Its
prepared call checks the journal cursor once under the namespace fence, records
the batch's database acceptance time, inserts the exact commands in array
order and queues one NOTIFY in one server-side statement. The returned identity
is an opaque storage cursor; gaps never imply missing Stream records. A final
database-clock check against the earliest deadline aborts every insertion and
notification on refusal, including a previously inserted batch prefix. READ COMMITTED is
required, and the head snapshot is obtained after waiting for the lock. A stale
cursor writes nothing. SQL does not interpret Stream conditions or retry keys.
Installation needs PL/pgSQL and permission to create/replace and execute this
function as well as the table permissions above.

Publication obligations and controls:

| Obligation | Argument and negative control |
| --- | --- |
| Safety | The server transaction fence covers head comparison, ordered inserts and implicit COMMIT. SQL evaluates no Stream condition. A stale cursor adds no rows; the shared replica discards the uninstalled decision, catches up and replans the original command and deadline. An expired later frame rolls back an already inserted prefix. A deferred-trigger commit failure must return an error even after the function produces its row. |
| Progress | The owned PostgreSQL protocol driver completes the query through transaction completion. Dropping that driver ends the request with an error; dropping the notification driver ends follows with `Unavailable`. No polling or per-stream producer is added. A competing session can publish while the first session plans. Transport errors retain the existing fail-closed reconciliation path. |
| Cost | One prepared publication query serves a supplied batch, independent of its width, with one server commit and no separate COMMIT exchange. Encoding and insertion cost O(input bytes + frames). Tests use widths 1/16/256; compare against a scalar-call loop as the measurement negative control. The indexed namespace-head lookup remains an index seek, not a proved O(1) storage operation. Full history replay and contention retries remain separate costs. |

The real SQL controls run with the existing Postgres provider checks. They verify
order and replay stamps, consumed identity gaps, malformed-array refusal, atomic
deadline rollback and driver loss. These controls do not establish throughput,
lock fairness, survival of an indefinitely stalled network or constant total
recovery cost. Measure messages, advisory-lock hold and p99 with the scalar-call
negative control before making a performance claim.

The shared replica can prepare against its retained cut when the journal supplies
an acceptance clock. An accepted uncontended write needs one conditional
publication statement, reduced from a read followed by publication. The cursor
CAS validates the exact cut used for planning; a stale proposal catches up in
bounded pages and replans the original command and deadline. Each prepared read
obtains one READ COMMITTED journal page, head and database-clock sample. A local
condition or capacity refusal refreshes before returning, because notifications
alone cannot establish freshness. Permanent successful key replay and an
immutable changed-request key conflict need no SQL.

The returned database clock binds only the original unpublished records and
envelope. Selected COW paths retain their original allocations; timestamp binding
does not rebuild the namespace, copy payloads or request new allocation admission
after durability. Other journals can retain their existing proposal-clock cut.
There is one advisory acquisition and one database commit per publication, with
no database lock spanning client-side planning. A final database-clock deadline
check after insertion prevents planning or lock-wait time from extending admission.
Statement preparation, initial installation, contention and catch-up are separate
costs. These operation counts do not establish latency, throughput or a calibrated
hardware-floor ratio.

An applied revision floor already covered by this replica needs no SQL query.
`read_commit` similarly returns an already-retained immutable successful envelope
without replaying unrelated progress; a missing envelope still refreshes before
returning absence. These paths check replica health and introduce no receipt or
negative cache. Mutable observations retain authoritative refresh. Delayed NOTIFY
alone never establishes freshness or permits returning an uncovered revision.

The canonical core separates publication ordering from access to visible state.
Once a storage acknowledgement is pending, readers and follow registration can
observe the last accepted state while the original decision retains writer
ownership. Competing mutations wait on that owner, rather than blocking new
readers behind a state writer. Installation and notifications follow successful
acknowledgement; failure leaves the decision uninstalled. An immediately ready
acknowledgement uses the original state guard without reacquiring it.

Local immutable `read_commit` hits also bypass the publication fence, checking
provider health and using the same retained envelope index. Missing envelopes
still join publication before reporting absence. This adds no receipt cache,
storage read or durability barrier and does not change mutable read freshness.
Snapshot capture pins the persistent namespace and the original immutable
retained facts under one short read guard. Those roots retain their original
native allocation ownership across later publications and state replacement.
The existing Local compaction worker performs encoding after the guard is
released, before taking the journal mutex. This adds no worker, file operation
or durability barrier; the existing replacement fence still orders mutations
against physical journal replacement. Retained fact headers are reserved before
acceptance, including prepared native work. Capturing a cut is constant work;
encoding and cold recovery remain proportional to the complete snapshot. The
encoder follows once-published weak successor links, stopping at the captured
tail. It neither copies the retained inventory nor pins later payloads. An old
tail can retain one retired successor's allocation header; the link owns that
header's original funding guard until its weak reference is dropped.

The shared Streams replica owns catch-up and stale-CAS replanning. The physical
port has no condition evaluator, retry cache or private connection protocol.
Two stages do not establish the one-round-trip target, bounded contention
latency or constant total recovery cost.

Each opened provider owns two connections and three shared async tasks: one
serialized query connection, one LISTEN connection, and one notification refresh
task. Clones share them. Streams and follows add no connection, OS thread or
dedicated background task. LISTEN is active before initial catch-up; notifications
trigger journal reads and the core's indexed subscription delivery. There is no
timer or polling fallback. Listener loss fails record and progress follows closed.
Returned record and progress subscriptions retain the shared provider until they
are dropped, even after the caller drops its last provider handle.

Limits: command recovery uses bounded SQL pages; public pages retain the unchanged
core bounds. Namespace writes serialize, and recovery replays retained history into
one shared core per opened provider, using `MemoryLimits::default()`.
The historical resource check below opened 100,000 flat paths and 100,000
subscriptions with its source's unchanged defaults. It measured idle control
overhead, not active-agent throughput. Retained state remains subject to the
configured core limits; the SQL journal itself has no compaction yet.

## Validation

From the repository root, run:

```sh
bash scripts/backends/compile-storage.sh
bash scripts/backends/storage-conformance.sh
```

The driver provisions disposable PostgreSQL and MinIO containers and stops both on exit. It runs the shared Stream, filesystem and host suites with retained storage across independently opened sessions. Supplemental checks cover exact replay, concurrent CAS, notifications, expired-deadline precedence, listener loss and unchanged persistent rows after refusals. The filesystem smoke is distinct from full filesystem qualification. Record the source, provider versions, commands and outcomes when qualifying a deployment.

## Audit v3b measurement plan

No new SQL runtime, throughput, latency or freshness qualification is claimed.
The compile-only refresh does not execute PL/pgSQL. Before claiming PLATFORM-9
performance, use a disposable Linux PostgreSQL container and process-owned RTT
proxy under `${TMPDIR:-/tmp}`, bound only to `127.0.0.1:15400-15499`, with cleanup on exit.
Use the same source, command sizes, work budgets and semantic oracles for the
sequential baseline and the publication function. Keep setup/statement preparation
separate from warm operations and count actual frontend protocol messages,
flush/response boundaries and SQL executions, not just outer provider calls.

| Workload | Measurements and unchanged controls |
| --- | --- |
| 1, 2, 4, 8, 16, 32, 64 writers; distinct paths in one namespace | Test shared-provider clones and independent provider processes separately. Record completed throughput, end-to-end p50/p99, SQL/protocol messages, session-mutex wait/hold and namespace advisory-lock wait/hold. Repeat with measured RTT 0.02/0.5/5 ms. Keep total inflight work bounded and report the single-query-connection limit. |
| Retained immutable envelope and already-applied revision; unrelated journal backlog | Count actual SQL queries and transferred command bytes: successful local hits require none. Keep the six-stage sequential wrapper and unconditional refresh as negative controls at the same source/workload. A missing envelope still performs an authoritative read; a foreign incarnation still refuses. |
| Conflicting paths and multi-stream commits | Preserve exactly one CAS winner and all-or-nothing conditions. Replay the same key after response loss and independent reopen; no new key or duplicate accepted command. Stale journal cursor and deadline-after-lock-wait publish no row/notification. |
| Cancellation before send, after send, after commit; process crash | Retain original command bytes and native work custody. Reconcile a sent Unknown using the original key; inspect actual journal rows and immutable receipts, not only returned errors. Retain existing refused-command/DG071 oracles when adopting the shared replica. |
| Delayed/lost notifications and stale grants | A delayed notification never establishes freshness. Keep the authoritative read/decision boundary and stale-grant AlreadyExists/privacy oracles in the canonical immutable Objects core. No polling or private grant cache. |

Replay the sequential adapter as the negative control without changing workloads
or budgets; report whether the stage/lock measurements detect it. PLATFORM-25's
ordered native journal batching remains separate acceptance work: compare
batch widths 1/16/256 and 1/32 writers, counting jobs, syscalls, copies, queue time,
CPU and p99 on each supported OS. Batch 1 must restore the dispatch slope.
Preserve original offset/byte/work ownership, grouped fsync, torn-tail recovery,
short writes, dropped physical completion and cancelled observers. Shared native
backend pipelining belongs in native-runtime, with the portable replica owner
retaining the journal's visibility boundary. No such native batching or runtime
qualification is claimed by this Postgres source change.

The native batching prerequisite groups contiguous Linux `NativeFile` write
buffers into one owned vectored operation, bounded by 16 buffers and 1 MiB.
Payloads retain their original backing storage. Each contiguous run completes
before the next run, and a short completion is an ordered byte prefix completed
by the existing positional write-all path. An uncertain operation keeps the
entire buffer group in the existing retired-proactor custody. An unavailable
ring retains the existing positional backend. Windows batches with multiple
members use the existing IOCP driver even when inline blocking is permitted;
Apple already uses its shared vectored path. These are source changes, without a
new OS/runtime or performance qualification claim. LocalStream still writes one
frame under its publication guard: the ordered journal queue and its original
work/cut integration remain separate from this native prerequisite.

Any further frame batching or mutable-read elision needs an ordered-observation
proof and the same conflict, deadline, notification-loss and crash oracles. G20
transport/context is Auth-owned; G24/G25 blob/closure belongs to Objects; G30 uses
the existing guest Placement. This adapter adds no framing, reconnect loop,
private blob engine, or backend-specific filesystem branch.

## Live steps

Complete the OSS endpoint qualification separately from compile-only admission:

1. Compose a durable selected filesystem policy-cut provider with asynchronous
   revalidation immediately before disclosure and independent terminal push at
   zero credit. Retain Postgres AtomicPublication; no process-owner relabel or
   polling. Missing resources must keep failing closed.
2. On Linux run `scripts/backends/storage-conformance.sh` with its unchanged host
   vectors and endpoint phases for Postgres/memory, local/MinIO and
   Postgres/MinIO. Bind emulators to 127.0.0.1:15400-15499 under ${TMPDIR:-/tmp} and stop
   them after the run. Prove prompt/retry, follow, reply, settlement and reopen;
   then run the same GraphCoder binary against each endpoint. Record exact
   source pins, failures and timings without widening existing limits.

No paid provider account is required. The coordinator can repeat the same runner
against the target PostgreSQL deployment through its intended TLS/socket boundary,
verify synchronous commit and failover durability settings, then run:

```sh
cargo run --locked -p acyclic-stream --no-default-features --features postgres,conformance --example provider-conformance -- --postgres
cargo run --locked -p acyclic-stream --no-default-features --features postgres,conformance --example provider-conformance -- --postgres --scale
```

The first command also checks all three follow APIs after their creator's last
provider handle is dropped and terminates its own disposable LISTEN connection.
Repeat resource measurements with representative nonempty histories and active-agent concurrency.
Native work-meter entry points retain their explicit Unsupported default; this
adapter currently qualifies the ordinary public provider contract.

## OSS Actors server composition

Enable `acyclic-harness/serve` in the application. Keep the selected Stream
provider concrete when building `StreamAuthorityStore<P>` and the shared
`hosting::Bound<P, O>`; the same provider holds agents and filesystem authority.
Construct the admitted `hosting::Server` through `hosting::serve` or `reopen`,
start its canonical runtime, and pass it to the public server entry point:

```rust,ignore
let host = acyclic_harness::server::Hosted::new(server, clock, runtime_owner, resources);
acyclic_harness::server::serve(
    listener,
    acyclic_harness::server::ServerConfig { harness: host, tls },
    ready,
    shutdown,
).await?;
```

`runtime_owner` retains the running tree and its storage resources, stopping the
runtime when dropped. `resources` is the explicitly supported resource-follow provider;
pass `None` to reject resource follows while retaining ordinary Stream follows.
The local composition supplies its process-owned filesystem change provider.
Postgres does not declare the exclusive process policy owner needed by that
provider: remote idle filesystem-policy watches remain unsupported.
The current `Conversation::observe` always opens its tools resource, so a
composition without that resource provider does not yet support the full
frontend. Endpoint qualification remains open; typed composition alone does
not establish a working remote GraphCoder session.
The diagnostic at source `0983a6e402f7ab134d243a2c64b0b007ce2df3e8` plus the
typed composition reached prompt submission, then failed while following the
turn with a session registration deadline. predates the
resource-refusal correction. Its unchanged Stream suite passed in 128 ms,
Objects in 311 ms, filesystem smoke in 132 ms and chunked reopen in 191 ms;
the first complete host-vector pairing passed in 3.88 s. Later pairings and
endpoint reopen were not reached. The filesystem owner holds durable
policy-cut/follow composition after the canonical session supports asynchronous
delivery revalidation; Postgres retains its truthful AtomicPublication capability.
`ServerConfig` also accepts the existing
`LocalHarnessConfig`; both compositions use one credential minting, session and
gRPC service path. Use the same public `Connection` with either endpoint.
This selects providers in the server application's composition; it adds no
provider-specific filesystem or agent dispatch. GraphCoder's native checkout and
shell composition still has its separately declared native work-meter limits;
this API is not a claim that those limits have been removed.
