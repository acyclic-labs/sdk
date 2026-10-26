# PostgreSQL Stream

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
the visible core. Conflicts leave retry keys available according to the shared
contract. SQL synchronous commit must remain enabled for durable acknowledgements.

Each opened provider owns two connections and three shared async tasks: one
serialized query connection, one LISTEN connection, and one notification refresh
task. Clones share them. Streams and follows add no connection, OS thread or
dedicated background task. LISTEN is active before initial catch-up; notifications
trigger journal reads and the core's indexed subscription delivery. There is no
timer or polling fallback. Listener loss fails record and progress follows closed.

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

## Live steps

No paid provider account is required. The coordinator can repeat the same runner
against the target PostgreSQL deployment through its intended TLS/socket boundary,
verify synchronous commit and failover durability settings, then run:

```sh
cargo run --locked -p acyclic-stream --features postgres --example postgres-conformance
cargo run --locked -p acyclic-stream --features postgres --example postgres-conformance -- --scale
```

The first command terminates its own disposable LISTEN connection. Repeat resource
measurements with representative nonempty histories and active-agent concurrency.
Native work-meter entry points retain their explicit Unsupported default; this
adapter currently qualifies the ordinary public provider contract.
