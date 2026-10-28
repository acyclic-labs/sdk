# PostgreSQL Stream

The OSS Actors server accepts this concrete storage composition. Full endpoint
observation and reopen remain unqualified and are listed under Live steps;
missing filesystem resource follows fail closed. Historical emulator receipts
below remain tied to their recorded source. The prepared-decision correction
was transferred to Streams surface PR #1705 and is not included here.

Enable `acyclic-stream/postgres` and call
`postgres::PostgresStream::connect(connection_string, namespace)`. Each namespace
is an independent Stream provider in the caller-owned database. The adapter
installs `acyclic_stream_journal` and its namespace/revision index. The database
role needs schema DDL permission because connect runs idempotent installation,
plus SELECT/INSERT, sequence access, advisory locks and LISTEN/NOTIFY. Use a local
Unix socket or an authenticated TLS proxy;
the initial adapter connection uses `NoTls`.

Pass `Arc<PostgresStream>` to `StreamAuthorityStore::new`, combine with
`LogicalObjectStore<S3Objects>`, and supply both to `Fs::new` with durable embedded
capabilities. Filesystem does not inspect backend types.

Postgres commits an ordered command journal under a transaction-scoped namespace
advisory lock. Publication explicitly uses READ COMMITTED so catch-up obtains a
fresh snapshot after lock acquisition, even when the database's default isolation
is REPEATABLE READ. The canonical shared Stream core owns append-if-tail CAS, exact
idempotency, hierarchical fork/commit, filters and bounded public paging. Commands
include the database clock sample, so reopening preserves record times and retry
receipts. Publication survives caller cancellation; only committed commands enter
the visible core. The current implementation journals before semantic application,
so condition refusals can retain journal rows. The separate #1705 correction
prepares the canonical decision before SQL publication and installs it after
durability; its source-scoped receipts below do not qualify this source.
SQL synchronous commit must remain enabled for durable acknowledgements.

`acyclic_stream_publish(namespace, expected_revision, command_bytes, deadline)`
is the physical publication primitive. Its prepared call checks the journal
cursor under the namespace fence, samples the database clock, inserts the exact
command and queues NOTIFY in one server-side statement. A final database-clock
deadline refusal aborts both insertion and notification. READ COMMITTED is
required, and the head snapshot is obtained after waiting for the lock. A stale
cursor writes nothing. SQL does not interpret Stream conditions or retry keys.
Installation needs PL/pgSQL and permission to create/replace and execute this
function as well as the table permissions above.

The current wrapper still takes its existing transaction fence and performs
bounded catch-up before that call, then commits before exposing the canonical
result. This is the first PLATFORM-9 change, not the one-round-trip target or
closure of G07. Moving catch-up/replanning/batching into the one shared replica
belongs with the Streams owner. The existing DG071 correction must be retained
when that owner consumes this primitive; no condition evaluator is moved to SQL.
The separate BEGIN, lock, catch-up and COMMIT exchanges still exist. Delayed
NOTIFY is a wake hint, not evidence that a read is fresh; reads still catch up.

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
one shared core per opened provider. Default core capacity is 100,000 paths (including implicit
parents), one million records/commits/retry identities and 256 MiB of payloads.
The local resource check opens 100,000 flat paths and 100,000 subscriptions with
unchanged defaults. This measures idle control overhead, not an active-agent
throughput qualification. Retained history remains bounded by the core limits;
the SQL journal itself has no compaction yet.

## Validation

Run only on bb2:

```powershell
pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/backends/compile-storage.sh
pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/backends/storage-conformance.sh
```

The drivers' companion `.inputs.json` declarations name no comparison receipts
or borrowed proof artifacts; these jobs qualify the uploaded source and create
fresh emulator fixtures.

The second command creates PostgreSQL and MinIO containers with disposable data
under `/mnt/bb`. It invokes the unchanged Stream suite, then checks independent
sessions for exact replay, a concurrent CAS winner, notification delivery within
one second, reopening, expired-deadline retry precedence, and terminated LISTEN
connections. On bb2, 2026-10-26, the unchanged Stream suite passed in 121 ms;
independent-session checks passed with REPEATABLE READ as the database default.
Creating 100,000 paths took 872 ms; opening their subscriptions completed in
15.531 s total with two provider connections, four total runtime tasks (including
the test observer), one OS thread and 321,232 KiB RSS. Connection/task/thread
counts did not increase with streams. The unchanged filesystem smoke passed over
Postgres + MinIO in 194 ms; a 256 KiB chunked-file check read exact content after
independent provider reopen in 257 ms.

Evidence: buildbox job `1793048750090-108901-0c54a923`, output log
`/home/var/log/buildbox/1793048734046-108901-7d4d6f66.log`, PostgreSQL 17 Alpine,
MinIO built from the pinned source release in the runner. The filesystem check
uses the canonical public smoke export, which main explicitly distinguishes from
full filesystem qualification; its full workload corpus has no executable runner.
The conformance README labels the provider suites v2; its unchanged Stream export
currently invokes the v1-protocol `acyclic_stream::conformance::verify`.

On 2026-10-27 the same complete agent-host export passed over Postgres + memory
Objects in 4.96 s and Postgres + MinIO in 6.16 s. The guest wire boundary, crash
vectors, admitted roots and approval continuations retain their original
assertions and actor deadlines. Evidence: job `1793127734572-64852-d7b53ebe`, log
`/home/var/log/buildbox/1793127718113-64852-15957735.log`. This qualifies storage
composition beneath the host; it does not qualify an OSS endpoint or real
GraphCoder binary.

That run also passed both local host baselines (6.38 s), the cross-session and
listener-loss checks, and the idle 100,000-stream/subscription check in 17.65 s
(700 ms path creation), with two provider connections, four total tasks including
the observer, one OS thread and 321,380 KiB RSS. Browser and Cloudflare host
fixtures compiled for wasm. The current storage draft's adapter inventory is
508 Rust lines plus 62 SQL lines, pending the #1705 integration; the shared
journal-orchestration extraction remains follow-up #1629.

The shared prepare/durability/install correction passed all 390 Stream library
tests in 8.65 s, with no ignored or filtered tests. A real SQL observation of 48
conditional refusals and an elapsed deadline retained zero rows and bytes;
corrected requests plus append/fork/commit retries after independent reopen
retained exactly three accepted rows (189 bytes). The unchanged Postgres suite
passed in 135 ms; cross-session CAS, notification, subscription ownership and
listener-loss checks also passed. Evidence: job `1793131869120-84430-ddfd841b`,
log `/home/var/log/buildbox/1793131846910-84430-5f30da4e.log`, source checkpoint
`ff24415a75ce838a784e2f9c619eabdf6106c653`. The subsequent database planning-clock
correction at `0983a6e402f7ab134d243a2c64b0b007ce2df3e8` passed a compile-only
Postgres/example check: job `1793132220956-87488-8ef61921`, log
`/home/var/log/buildbox/1793132200512-87488-3bc05557.log`.

The session resource-refusal correction at
`8b43263bcb28829056e33036b90de2974e25c7d6` passed all 391 Stream library
tests in 8.87 s (zero ignored or filtered in the complete run), followed by a
default GraphCoder compile. Evidence: job `1793132738027-89535-de4d1919`, log
`/home/var/log/buildbox/1793132723435-89535-8486c831.log`. Missing resource
routing and synchronous resource refusal now close only their channel. This
preserves ordinary follows; it does not supply the missing remote policy watch.

## Audit v3 measurement plan

No new SQL runtime, throughput, latency or freshness qualification is claimed.
The compile-only refresh does not execute PL/pgSQL. Before claiming PLATFORM-9
performance, use a disposable bb2 PostgreSQL container and process-owned RTT
proxy under `/mnt/bb`, bound only to `127.0.0.1:15400-15499`, with cleanup on exit.
Use the same source, command sizes, work budgets and semantic oracles for the
sequential baseline and the publication function. Keep setup/statement preparation
separate from warm operations and count actual frontend protocol messages,
flush/response boundaries and SQL executions, not just outer provider calls.

| Workload | Measurements and unchanged controls |
| --- | --- |
| 1, 2, 4, 8, 16, 32, 64 writers; distinct paths in one namespace | Test shared-provider clones and independent provider processes separately. Record completed throughput, end-to-end p50/p99, SQL/protocol messages and namespace advisory-lock wait/hold time. Repeat with injected RTT 0/1/5/20 ms. Keep total inflight work bounded and report the single-query-connection limit. |
| Conflicting paths and multi-stream commits | Preserve exactly one CAS winner and all-or-nothing conditions. Replay the same key after response loss and independent reopen; no new key or duplicate accepted command. Stale journal cursor and deadline-after-lock-wait publish no row/notification. |
| Cancellation before send, after send, after commit; process crash | Retain original command bytes and native work custody. Reconcile a sent Unknown using the original key; inspect actual journal rows and immutable receipts, not only returned errors. Retain existing refused-command/DG071 oracles when adopting the shared replica. |
| Delayed/lost notifications and stale grants | A delayed notification never establishes freshness. Keep the authoritative read/decision boundary and stale-grant AlreadyExists/privacy oracles in the canonical immutable Objects core. No polling or private grant cache. |

Replay the sequential adapter as the negative control without changing workloads
or budgets; report whether the stage/lock measurements detect it. Any later frame
batching or refresh elision needs an ordered-observation proof and the same
conflict, deadline, notification-loss and crash oracles. G20 transport/context is
be-edge-owned; G24/G25 replication/blob contracts are Streams-owned; G30 uses the
existing guest Placement. This adapter adds no framing, reconnect loop, private
blob engine, or backend-specific filesystem branch.

## Live steps

Complete the OSS endpoint qualification separately from compile-only admission:

1. Integrate the canonical async credential-verifier shape from #1715
   (`CredentialVerifier::verify` and retryable `VerifyError::Unavailable`).
   This does not by itself make filesystem policy cuts current.
2. Compose a durable selected filesystem policy-cut provider with asynchronous
   revalidation immediately before disclosure and independent terminal push at
   zero credit. Retain Postgres AtomicPublication; no process-owner relabel or
   polling. Missing resources must keep failing closed.
3. On bb2 run `scripts/backends/storage-conformance.sh` with its unchanged host
   vectors and endpoint phases for Postgres/memory, local/MinIO and
   Postgres/MinIO. Bind emulators to 127.0.0.1:15400-15499 under /mnt/bb and stop
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
turn with a session registration deadline. Job `1793132557129-88589-6f2aebba`,
log `/home/var/log/buildbox/1793132539573-88589-57ecfc1e.log`, predates the
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
