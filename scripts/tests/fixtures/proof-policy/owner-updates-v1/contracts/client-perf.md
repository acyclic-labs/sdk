# Client A7: one bounded session and host composition (Codex, 2026-10-26)

Status: API sketch, not an implementation or qualification. P4 PR #1508 at
cef1e415194397155569bee6b40d6b3dd48020c8 is frozen in the gate first. The next
single PR carries the whole A7 scope. Read against REDESIGN.md, common.md,
the harness API audit, ui-sync/design.md and the current b733/cef source.
No Cloud review file exists yet; absence is not approval or hosted evidence.

## Source reconciliation and ownership

The historical audit's private TS HarnessClient/wire-transport and GraphCoder
terminal.rs are already absent. StreamSessions is an I/O adapter to Rust
WasmSession. Keep acyclic_stream::session::Client as the sole connection,
subscription, credit and resume owner; one physical connection is per serving
authenticated route, not a claim of arbitrary cross-cell fan-in. ClientViews
already owns the fold. The hypothesis Client<D> is a different mechanism.
Provider Registry and tools::defaults() already exist: reuse them.

Current FileClient, task query folds and swarm orchestration depend on TaskVolume.
A7 must not extend that retired substrate. Replace that dependency with A5's
filesystem snapshot-and-follow API; preserve the outbox publication/durability
invariants while deleting the TaskVolume-specific implementation. A3 owns
children_page and transport credit; A5 owns query cut/cursors; A6 owns verified
authority. No parallel protocol, receipt ledger or reconnect scheduler here.

## Public API direction

Names below are proposed signatures. Use the concrete A3/A5/A6 types as they
land; these declarations do not introduce alternate wire schemas.

```rust
struct ClientPolicy { /* finite selected/expanded/page/byte/outbox budgets */ }
struct ViewDemand { /* visible, focused, expanded nodes, open file panes */ }
struct WireSession<P: StreamProvider + ?Sized, F: FilesystemClient + ?Sized> {
    // One shared Streams session, filesystem reader/outbox, ClientViews,
    // bounded demand and page owners. No independent transport state.
}
impl<P: StreamProvider + ?Sized, F: FilesystemClient + ?Sized> WireSession<P, F> {
    async fn open(session: SessionClient, files: Arc<F>, owner: VerifiedScope,
                  policy: ClientPolicy) -> Result<Self, ClientError>;
    async fn set_view(&mut self, demand: ViewDemand) -> Result<(), ClientError>;
    async fn children_page(&mut self, node: &StreamPath, cursor: Option<ChildCursor>,
                           limits: PageLimits) -> Result<ChildPage, ClientError>;
    async fn submit(&mut self, intent: OwnedIntent) -> Result<PendingId, ClientError>;
    async fn next(&mut self) -> Result<ClientChange, ClientError>;
    fn views(&self) -> &ClientViews;
}

struct PageWindow<T, C> { /* one shared paging state machine */ }
struct PageEntry<T, C> { value: T, cursor: C }
impl<T, C> PageWindow<T, C> {
    fn begin(&mut self, direction: Direction) -> Result<PageRequest<C>, ClientError>;
    fn apply(&mut self, request: PageRequest<C>, page: Page<T, C>)
        -> Result<PageChange, ClientError>;
    fn items(&self) -> impl Iterator<Item = &T>;
}

struct HarnessBuilder<P, F, Owner, Models> { /* one composition owner */ }
impl HarnessBuilder<LocalStream, LocalFilesystem, VerifiedScope, MissingModel> {
    fn filesystem_local(stream: Arc<LocalStream>, fs: Arc<LocalFilesystem>,
                        owner: VerifiedScope) -> Self;
}
// Provider-neutral preset accepts caller-owned memory/remote providers too.
// Native checkout bootstrap is an adapter into that same composition path.
impl<P, F, Owner> HarnessBuilder<P, F, Owner, MissingModel> {
    fn providers(self, registry: Arc<ProviderRegistry>, selection: ModelSelection)
        -> Result<HarnessBuilder<P, F, Owner, BoundModel>, BuildError>;
}
impl<P, F, Owner> HarnessBuilder<P, F, Owner, BoundModel> {
    fn tools(self, tools: ToolRegistry) -> Self;
    async fn build(self) -> Result<Harness<P, F>, BuildError>;
}
```

Export the same WireSession/PageWindow through WASM. TS supplies transport/file
callbacks and opaque item handles; ordering, cursor movement, admission and folds
stay in Rust. JS caches one immutable items array per changed revision, so an
unchanged read never crosses WASM to copy the whole window. Native iterates a
borrowed view. GraphCoder, plugin/library and web use the same core; no second
wire loop. Consumer deletion follows the A7 API in wave 2, in this lane's PR.

## Invariants and bounds

- Full record streams exist only for visible or focused agents, with one combined
  selected cap. Nearby/prefetch agents receive no full transcript follow. Other
  agents are discovered lazily through capped pages per expanded node. Do not
  retain every agent or an all-swarm status map as a prerequisite to browsing.
- Policy has finite validated defaults and hard ceilings, charged before retaining
  input: interests, expanded nodes, page records/bytes, pending commands/bytes,
  queued frames/bytes and hydrated view bytes. Counters include backing segments
  and pending producer ownership. A blocked producer is still retained memory.
  Reject an over-budget request with a typed capacity error; no silent dropping.
- Per-interest record and connection-byte credit remain owned by Streams. Hold a
  delivery's credit until the fold/application releases its backing payload. Never
  requested authority is a typed refusal; retired generation traffic is fenced
  separately. Do not convert either into revision zero. Resume is at-least-once
  transport with exactly-once cursor application, not exactly-once sockets.
- Page admission checks generation, exclusive edge cursors, item/byte bounds and
  request identity before mutation. Retained first/last entries own both cursors;
  eviction therefore moves the corresponding edge and restores its has-more flag.
  Opaque page-level cursors cannot recover a partially evicted page: require
  per-entry cursors or retain/evict whole pages. Choose per-entry cursors where the
  source supports them and whole-page ownership otherwise. Never fabricate a cursor.
- One mutation owner serializes opposing page loads; resets fence responses and
  retain bounded in-flight custody until cancellation/settlement. No unbounded
  chain of stale promises, waiter sets or tombstone identities.
- Resident client cost O(expanded * page + selected * page + outbox + queue)
  under fixed byte budgets, independent of 100000 total agents. Frame visits and
  copies O(visible), no overview scan. Changed-page work may be O(retained page
  window); unchanged item reads O(1) in JS, borrowed iteration in native.
- Builder construction validates owner/provider/model/tool policy before opening
  execution. Missing model cannot build. Registry selection uses existing provider
  resolution/catalog/capacity rules, with credentials confined to host adapters.
  Presets call one composition function; they do not reproduce Runtime::open's
  positional wiring. Existing tools::defaults remains the only default catalog.

## Proofs and deletion plan

Public generic conformance in the existing conformance package takes the actual
provider traits, namespace and policy. Its endpoint runner reads
ACYCLIC_CONFORMANCE_ENDPOINT/TOKEN/NAMESPACE without printing credentials or
falling back to memory. Memory, durable local, browser/WASM and hosted run the
same histories; mode receipts are separate. Coordinate concrete entry points
with verify, Streams, A5 and A6 before implementing adapters.

1. Run production demand at 1000/10000/100000 agents with fixed visible/focused
   counts; record retained bytes, interest counts and frame visit/copy counters.
   Old eager enumeration/full-follow/overview scan fails the same oracle.
2. Drive actual server with zero/exhausted record and byte credit, cancellation,
   resume, late retired-generation frames and unknown authorities. Credit bypass
   and unknown-authority acceptance mutants fail the same assertions. Reuse
   production Streams Loom, not a new accounting model beside it.
3. PageWindow proptest/DST uses an independent ordered exclusive-cursor sequence,
   forward/reverse eviction, overlaps, duplicate keys, epoch/reset and concurrent
   requests. Actual old production loses b in [b,c] -> [c,d] -> [a,c], and allocates
   100 arrays per 100 unchanged reads. Before receipts already admitted on Linux.
4. Real generated WASM in Node and Bun asserts the same transitions, typed errors,
   BigInt widths and callback failures. CLI/library adapters exercise the Rust API.
   Builder tests cover every preset and rejected incomplete/unauthorized setup.
5. Focused suites each <10s, deterministic gate corpus and explicit selected case
   counts. Longer exploration stays separate; no skips or retry-as-proof.

Delete superseded TS paging state, whole-history UI hydration, per-agent eager
follows and TaskVolume-specific client storage/fold code after their shared A5
replacement is integrated. Delete HarnessWireApi and Swarm HTTP consumers in
wave 2; adding credit fields to a path selected for deletion is not A7 work.
Do not delete thin TS StreamSessions or the existing Rust provider registry as
if they were duplicate cores. Current P4 receipts remain at their original pins.

Decisions: shared provider contract replaces the obsolete Cloud REST pagination
dependency; Cloud implements hosted adapters and runs the public endpoint suite.
No access to Q:/cloud is needed. Hosted qualification remains open until those
actual endpoint receipts exist. Native macOS capacity remains a separate explicit
gap in P4; the fresh macOS Node/WASM pass is not native mount qualification.
