# acyclic-fs

Immutable, versioned workspaces with embedded and hosted backends. Generations are stable identities; mutations produce new generations instead of rewriting history.

For a stable paginated directory walk, capture `workspace.sync().await?.into_generation()` and call `generation.list_directory(...)` for every page. The moving workspace head intentionally has no paginated directory API.

```sh
cargo add acyclic-fs
```

Choose the backend for your deployment: embedded storage for local durability, hosted authority and Objects for a remote workspace, or embedded native mount integration. Transactions stage edits before commit; forks branch from a generation. The provider determines persistence, isolation, and mount guarantees.

`WorkspaceContextRegistry` records the control-plane hierarchy of physical roots and their exact workspace identities without enumerating file contents. A context has one direct parent, but its roots can be independently adopted by another authorized context; ancestry is not a prerequisite for reference visibility. Native and browser bindings exchange the same bounded `WorkspaceContextRoot`, `WorkspaceContextRoots`, `WorkspaceContextSnapshot`, and `WorkspaceContextDiscard` protobuf records. The Rust codec rejects malformed identities, noncanonical paths, duplicate or unordered roots, invalid lifecycle states, and noncanonical encodings before adapters act on them. Context records convey routing and lineage, not permission to read or write another owner's bytes.

Start with the [embedded workspace example](https://github.com/acyclic-labs/sdk/blob/main/rust/crates/filesystem/examples/embedded_workspace.rs), the [hosted example](https://github.com/acyclic-labs/sdk/blob/main/README.md), or the [Rust API](https://docs.rs/acyclic-fs/latest/acyclic_fs/). The [v2 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/filesystem) defines compatibility.

The [publication migration inventory](#filesystem-publication-migration) describes the
portable Streams record contribution API, compatibility boundaries and pending
caller migrations.

## Hosted generation custody adapter

`AsyncObjectStore::prepare_generation_publication(request, proof, append, scope, budget, cancellation)` runs after the original closure flush and before the exact authority CAS, while `PublicationHold` remains live. It borrows the canonical `PublishGenerationRequest`, `GenerationProof`, and full `GuardedAppend` (original `ProposedCommit`, permit, epoch and expected head). The supplied closure scope includes the exact object identities and original collection sweep count. `StagedObjects` forwards this preparation to the durable provider after draining; it preserves the original collection hold through the CAS. Default preparation creates no hosted custody.

Execution owns the hosted filesystem provider and its existing accepted-operation journal. Data owns generic private Objects generation references and retained-root capabilities. The Actor restore journal and its `actors` product permission cannot authorize filesystem operations. `LogicalObjectStore` is a stateless public bucket adapter; neither its `ETag` nor a successful default flush is private retained-root custody.

The provider can call `kernel::encode_generation_publication_evidence` with the actual admitted `VolumeConfig` and a positive evidence byte bound. Rust validates matching request/proof/append identities and encodes the complete canonical manifest, original root, logical-file byte total, exact commit and permit. Its returned SHA256 names exactly the returned bytes, not a reconstructed history or the BLAKE3 authority identity. Runtime work counters and collection sweep counters are not durable generation facts and are excluded from the archive bytes.

Before the callback returns, the actual filesystem adapter must durably journal that original preparation and its exact `ObjectId -> GenerationRef` mapping under the protected filesystem accepted operation, then retain the original closure through Data's current private root-claim path. Accepted operation identities and exact bytes survive ambiguous outcomes and ownership transfer. Only the actual authority result can supply final acceptance, sequence/digest and settlement time; the provider must durably reconcile and bind that original outcome in its existing journal. This callback covers generation publications, not the separate initial-volume, fork-retention or pin creation boundaries, which still require their own exact accepted-operation custody integration.

`PublicationHold::none()` lets an external provider complete the existing flush contract after its own durability work. It provides no collection gate, root claim, historical proof custody or release permission. Dropping a hold must not release provider-owned durable claims. Default preparation, an accounting page watermark and delivery ACKs never authorize collection. PR #215 remains incomplete until the filesystem owner implements and supplies the atomic original-proof custody boundary for every relevant transition.

`kernel::decode_generation_publication_evidence` projects retained bytes against the SHA256 from protected original custody, with explicit byte/item bounds and canonical re-encoding. It returns original request, manifest/configuration, root, logical bytes and full append; it does not manufacture a fresh closure proof or final outcome. Execution can reuse this Rust decoder during journal recovery instead of defining a private Cloud proof serialization.

## Filesystem publication migration

M1 adds record contributions to the existing `distributed::FilesystemCommit`.
It does not change the shipping workspace storage layout or implement G48's
selected retirement of `Fs<A, O>` and `AsyncAuthorityStore` without a compatibility
window. Those types are still exported in this milestone's source.

### Record contract and compatibility

The caller chooses a volume path inside its forkable subtree. Its first
conditional append implicitly creates the stream. Root and companion snapshot
occupy one `StateRecord::Volume` record. `PortableRoot` contains the root FileId
and immutable file-table identity, without a volume identity, writer epoch,
FS hash chain or locator. `From<&GenerationRoot>` is the compatibility
construction path; legacy root codecs and volume checks still serve callers
that have not migrated.

V1 framing is `domain || key_length:u16-le || original_key || path_length:u16-le
|| original_destination || kind:u8 || body`. The original destination belongs
to publication provenance; it does not change when a subtree inherits the record.
Recovery compares it with the intended destination, so an inherited source record
cannot establish a new destination's append outcome.
A volume body begins with `root_file_id:16 || file_table_digest:32`, then the
companion codec bytes. Companion tags select existing context, sparse-workspace,
Git-state or permanent-outcome codecs. Framing does not validate their semantic
payloads: existing typed validators remain required at each consumer boundary.

`observe_record` returns a provider-bound `AuthorizedObservation` with retained
data. NotFound becomes Absent; empty state streams, transport failures and
unavailable history are errors. Present state uses Head(path, sequence+1,
commit_id). The external observation constructor rejects tail-only fences.
Callers authorize the resolved paths and supply every policy/actor/metadata
observation to `seal_observed`. These conditions are not capabilities.
Immutable-root proofs may cover ancestor and symlink traversals without a
separate fence per immutable object.

Stage and verify the immutable closure before `contribute_record`, retaining
its `PublicationHold` through submission. `PublicationHold::none()` alone proves
no remote retention. `seal_observed` freezes explicit facts and the optional
exclusive Unix-millis deadline without reading the legacy poll-local policy cut.
It reuses Streams' canonical request validation and full protobuf encoded-total
admission. Oversize requests are not chunked. Conflicts publish no mutations.
Cross-provider observations and mixed-owner contributions are refused. Prepared
commits remain bound to that owner through submission, settlement and retries;
no cross-provider atomicity is claimed.
Independent remote sessions may conservatively refuse composition.

A committed envelope yields `RecordReceipt { path, sequence, commit_id }`.
`read_receipt` performs one exact read and verifies history identity.
`reconcile_record` handles a lost reply at its predicted append position by
comparing the original key and complete encoded state, without an idempotency
lookup. Missing/mismatched history never authorizes execution at a later position.
Application retention must preserve permanent semantic outcomes; this API does
not pin all records forever or create another receipt service.

M2's portable batch preparation uses `prepare_records` to stage roots, companion
snapshots and refs against explicit observations, then seal one command. A local
refusal preserves the caller's original conditions and held closures. Submission
uses the existing Streams commit path. `reconcile_states` reads each original
predicted position once, requiring every destination-bound state and header to
match under one commit identity before releasing the original owner. Callers use
unique operation keys. Missing records, separate commits, inherited source
records, retention loss and local refusal preserve Unknown; none authorizes a
new append. This portable source does not yet replace the shipping multi-root
coordinator, companion database or legacy authority consumers.

Unknown retains the original request/key, deadline, holds and native request
custody, including when local retry preparation, cancellation, budget admission,
owner validation or cleanup refuses. Such refusal cannot settle an earlier
uncertain publication. Resubmit that commit unchanged. External submitters must honor
`deadline()` with `commit_before` and settle their actual provider outcome.
Current head reads still use tail plus exact read pending G01. Multiple
observations are optimistic facts, not a coherent G05 snapshot. Legacy `seal`
retains its policy bridge until callers migrate.

Generation facts retain two bounded windows. Once a window is forgotten, a
missing fact is Unknown and uses the existing durable lookup; it cannot establish
absence. The shared actor registry keeps exact weak registrations for live
actors and at most 256 strong idle references per owner. Cache collisions may
discard idle state, but never replace a live actor. Final actor destruction
retires its exact registration and index nodes; reopening reconstructs forgotten
state from Streams. These bounds describe source structure, not measured RSS or
latency at 100k volumes.

### Caller and deletion inventory

| Existing owner paths (relative to src/) | Adoption and deletion release |
| --- | --- |
| `facade/{fs,volume_creation,workspace_creation,checkout,publication}.rs`; `kernel/publication/**` | M2/M3: portable volume records and shared builder after live creation/open/publication/recovery parity; retain immutable algorithms |
| `distributed/{lineage,authority,replay,operations,reservations}.rs`; `distributed/append/**`; `streams_record.rs` | M2/M3: caller subtree placement, exact receipts and actor/root fences before removing epoch/hash/locator/reservation families |
| `core_state/{contexts,publications}.rs`; `core_state/nodes/store.rs`; `record_store.rs` | G27: conditional typed companion contributions first; del-fs-model deletes WAL/checkpoint/record stores after release |
| `lazy_workspace/{model,store,fork}.rs`; `workspace_context.rs`; `lineage/**` | M2/M3: root+companion commit and subtree activation; preserve sparse indexes, routing and filtered forks |
| `git_compat/{store,refs}.rs`; `git_compat/repository/{publication,transitions}.rs` | M2/M3: refs/outcomes in root commit; preserve permanent outcomes and physical write/capture custody |
| `multi_root/{coordinator,publisher,validation}.rs` | M2: one admitted root/read-set commit before deleting reserve/release and roll-forward phases |
| `operation_window/{coordinator,streams}.rs` | M3: owning actor checkpoint/alarm and exact provider expiry; delayed alarm cannot admit an expired write |
| `workspace/access.rs`; `async_storage/policy.rs`; `distributed/policy.rs` | G26: explicit resolver observations before deleting TLS fallback; keep immutable-root proofs |
| `distributed/actor/generations.rs`; `kernel/publication/combiner/{mod,volume}.rs` | M4: separate retained active ownership from bounded cached facts; evicted Unknown is never Absent or a second combiner |
| sibling `filesystem-{napi,wasm}/src/**`, `harness-core/src/filesystem/**`, conformance | G48/G49: migrate consumers and retire authority exports without a compatibility window; share the existing G44 work context |

No path is released for deletion by M1. del-fs-native owns native
view/frontier/rebase integration; this API is not an implemented shared follow
frontier. del-fs-model owns G44's actual shared work/custody implementation;
del-core owns its harness consumers and state-free codecs. Propagation of public
deadline and cancellation into all coordinators remains G49 work. No second work ledger is
introduced. Report del-fs-model deletion separately from these additions.

### Measurement and acceptance plan

These are proposed experiments, not performance results. Use existing tooling
on demand; merge admission is compile-only. Record exact SDK/provider revisions,
host/transport/features, dataset, load, accepted/rejected work, duration and raw
artifact location. Keep cold, reopened-warm and newly-created-warm paths separate.

| Sweep | Measurements | Negative controls |
| --- | --- | --- |
| History 10²–10⁷; 100k dormant volumes and fixed active set | Paths, charged bytes/RSS, recovery reads, flushes, calls and p99 | Same tail/different history; evicted present fact; rejected preparations cannot grow resident history without bound |
| N=1/8/64 roots, exact conditions+mutations+record bytes | Preparation reads, encoded total, all commits/companion IO, flushes and p99 | Advance one root, metadata, policy or actor cut: nothing moves; oversize refuses without a submitted prefix |
| Full subtree fork across file count/history/depth | Root PUTs, activation/fork commits, lineage writes and retained paths | Source advances; lost reply; child cannot read unrelated roots. Filtered fork tree work is counted separately |
| Tool lifecycle and long tools with renewals | Baseline 10 control calls plus 5 per renewal, retained paths, alarms/probes and physical starts | Exact expiry/superseded actor state despite delayed alarm; queued stale write; crash during final reconcile |
| Lost reply/expired idempotency lookup | Original outcome identity, data/control recovery calls, duplicate effects | Permanent record is authoritative; missing/retention-lost history stays error/Unknown, never replay permission |

New unit cases cover stale-root/policy atomicity, cross-provider refusal,
encoded-total admission, receipt identity, recovery without idempotency lookup,
malformed framing and exact expiry retention. Compilation is not runtime proof.

Cloud input is the coordinator-located draft
`Q:/lanes/cloud/design-audit-v2/fs-scale-draft1.md`, SHA256
`980BA694CF42AABE595527F6EEB05EC10521BDF228E28A73ADA8AB3BC53CD4FF`.
A final/superseding disposition remains pending. Subtree splitting and hot
directory partitioning remain Streams capabilities.

The `conformance` feature publishes `conformance::{Backend, Fixture, verify, bench,
suite}` over the existing public `Fs`, `Workspace` and immutable `Generation`
operations. `from_providers` composes actual Stream and Objects providers with one
shared `Counted<P>` dispatch ledger, including errors. Downstream storage adapters
retain their concrete authority and object types so publication guards, original
work and custody cross the same production seams. Temporary resources must stay
owned by the backend throughout each invocation.

Portable vectors check binary size boundaries, immutable history, typed read
refusals, cancellation, fork isolation and removal. The checked benchmark reads
one exact immutable 4-KiB value after warmup and rejects head, metadata, list,
lease or token probes. Mount qualification additionally exercises the real mounted
interface through the existing mount qualifier. Run the memory and durable local
adapters on demand (the default features include local storage):

```sh
cargo test --locked -p acyclic-fs --features conformance,memory --test main public_conformance
```

The Postgres/MinIO adapter runs the same public suite and retains the independent
provider-reopen check through `bash scripts/backends/storage-conformance.sh`.
The browser adapter uses real `BrowserStream` and `IndexedDbObjectStore` providers:

```sh
bash scripts/run-browser-tests.sh cargo test --locked -p acyclic-fs \
  --no-default-features --features browser,memory,conformance \
  --target wasm32-unknown-unknown --test browser_commit public_conformance
```

The runner uses installed Chrome/Chromedriver. Set `CHROME_BINARY_DIR` and
`CHROMEDRIVER` when selecting another installed distribution.

`Counted<P>` also forwards `AsyncObjectStore` directly. Cache and collection
visits preserve the original owner; async storage calls retain the original
`AuthorityWork`, buffers and publication holds. Counts describe the wrapped
provider boundary, rather than physical traffic hidden inside a provider.
