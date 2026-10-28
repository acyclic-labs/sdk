# Machines core on Streams (G29)

Supply the application's canonical authenticated
`StreamProvider` and one account path segment. The supplied Streams session
continues to enforce its existing scope; the account string grants no authority.

```rust,ignore
let operations = acyclic_machines::core::OperationRegistry::new(streams, "account")?;
let provider = acyclic_machines_daytona::DaytonaProvider::new(config, operations)?;
```

Native adapters implement `core::Backend` and `RawMachines`; the core supplies
`MachinesProvider` once. `BackendCore` binds the Streams registry, readiness
profile and assurance at composition. Its staged boot input is ephemeral and
redacted; completed create attempts clear it, including replay and refusal.
Call `backend.core().stage_boot_env(key, env)` when boot input is needed, with
`Backend` in scope. Raw list and usage return decoded observations; only core
publishes their canonical state and events. Native pagination is collected once
by `core::collect_pages`, retaining the existing 256-page bound and refusing
a repeated cursor without another request. Provider cursor decoding stays in the
wire adapter. Native labels cannot reconstruct a fork result after a restart. Their portable
encoding, tenant matching and native names are `core::metadata`
projections; operation authority remains in Streams.
Usage identity/interval and console
read bounds are admitted there before transport. Retained command output reads
the core console after canonical machine/driver admission; no raw output read
primitive or second provider history is needed. Image identity, required
capabilities and the effective suspension contract are admitted in core before
native creation. `CreationAdmission` supplies the backend's actual budget and
fixed-expiration enforcement facts and host-enrolled network policies. Pure
refusals precede image qualification; settled replay precedes fresh admission.
`raw_start` receives the admitted contract and image or immutable checkpoint
source. Suspend, wake and suspension policy share `raw_change`; their public
preconditions, readiness, history and result publication remain in core.
An image without SuspendResume records Manual, including in the simulator.
`core::NetworkPolicy` and `admit_network` resolve enrolled 32-byte commitments
and validate domain lists in one owner; adapters only encode their native egress fields.
The HTTP core also admits provider-reported credential destinations against
the enrolled parent origin and proxy hosts before attaching sensitive headers.

Guest adapters prepare one lazy `ExecSession`: an output stream and one stop
future. Preparation performs no guest IO. Core checks the admitted machine and
fixed lifetime, then registers the shared WallAlarm/drop owner before polling
that session. Stdout and stderr remain distinct; output backpressure cannot
prevent cancellation or expiry. The output allowance, terminal exit/stop and
bounded ephemeral console history use the same owner extracted from #1690.
An unsupported raw transport still refuses before guest IO. Daytona binds its toolbox stream to that owner; framing, process-group cleanup,
file commands and HTTP dispatch live in core. The HTTP transport validates
endpoint and sensitive credentials once, refuses redirects and automatic retry,
and retains native status decoding in the wire adapter. Scoped toolbox clients
reuse that admitted transport; relative route segments cannot replace its origin.
A successful HTTP status with malformed JSON remains an unknown native outcome,
so a create key cannot be released for redispatch after acceptance. Provider captures remain
separate qualification work; internal owner vectors
do not establish a live provider pass.

Admission precedes native dispatch. A keyed conditional commit appends the
operation revision to `machines/<account>/ops/<id>`. That one stream owns the
operation history and its current projection; there is no account-wide copy.
An identical successful retry returns the original result; a different intent is
a conflict. A pending or unknown operation refuses redispatch. Recovery never
infers success from a native resource's name or labels. Daytona's former
create/fork name-holder adoption paths are deleted: a native collision is a
conflict, with no follow-up lookup, relabel or deletion claim. Existing settled
operations replay from Streams without another native dispatch.

`watch` and `join` follow the point projection through Streams' checked cursor.
`SettledMutation` includes the operation, final `CommitId`, and exact result.
Recovery reads that immutable terminal publication directly; it consults the
mutable operation point only to distinguish a missing key from a pending result
or an invalid terminal publication. Native targets retain their machine or
checkpoint kind, even when both kinds use the same opaque address. A target is
never acquired as created cleanup debt merely because its address matches.
Core lifecycle methods return this value directly, on first execution and replay;
`MachinesProvider` projects its public outcome at the backend composition boundary.
A terminal transition publishes `ops/<id>/settlement` atomically with the mutable
operation revision. Later deletion acknowledgments cannot change its CommitId.
Protocol 1.2 `Join` takes the original key and operation, and returns the same
canonical publication as a one-result server stream. Send headers immediately;
the single stream item awaits `join_settlement`, then the stream ends. This keeps
the existing RPC setup deadline without a lifecycle wait deadline. `join_settlement` also exposes explicit failed/cancelled
publications; `grpc::encode_settlement` is the server encoding boundary. Verify
the request's key/operation binding in the existing authenticated admission scope
before joining. A transport error preserves the recovery key. The gRPC client
never rebuilds an outcome by inspecting current machines/checkpoints and has no
watch/inspect reconciliation loop.
Large immutable results use bounded canonical records with a length, digest and
prefix anchor; success publishes only after those records are durable. This
preserves Streams' 64 KiB record limit and retains original outcomes independently
of later machine observations. Staged but unpublished bytes remain canonical
Streams data; retention and storage custody belong to the supplied backend.

Machine state and its state event (`machines/<account>/<id>/events`) publish in
one multi-stream commit. Cleanup ownership survives cancellation and process exit.
A created machine's native handle, initial observation/event, and ownership
publish in one conditional commit before readiness. Driver identity is part of
native admission; another backend cannot observe or clean up its resources.
Bindings live in the machine projection, without a second process-local map.
Each acknowledged deletion releases only that resource; a late create cannot be
lost by another cleanup pass replacing the ownership list. A successful settlement
never yields resources for cancellation to delete. Canonical per-operation cleanup
links identify known public projections without an account scan or another copy
of every child in the bounded operation record. Each acknowledged deletion commits
the debt release with Destroyed state/event or revoked checkpoint forkability.
Native addresses occupy individual `ops/<id>/native` records, appended atomically
with their hash-to-position projection and the operation revision. The point
holds ordered compact positions, keeping a maximum-child operation within the
existing 64 KiB bound even with long opaque addresses. Exact typed ledger records
validate the hash projection; it grants no independent custody. Cleanup preserves
the point's order and removes only the acknowledged position.
Links confer no deletion authority: the operation's created-resource custody and
matching native provider/address remain authoritative. Malformed metadata retains
its typed address without inventing a public identity. Native deletion returns
its observation timestamp; a confirmed missing resource retains the last known
time if the provider supplies no new one. Simulation uses its existing virtual
clock. Accepted children already owned by an operation are not also recorded as
external targets when they become sources in a later fork round.

Adapters dispatch each native mutation once. Asynchronous native resources use
the shared bounded `settle` observer, which repeats reads, never mutations.
Creation uses this same owner: raw acceptance carries its handle separately from
observation decoding, so malformed metadata retains cleanup debt and its original
key. Native driver identity is assigned once by the core. Accepted atomic
checkpoints are owned before the first readiness read. Machine
and checkpoint cleanup use typed native addresses in the same operation record.
A native conflict does not establish snapshot ownership. An accepted Daytona
snapshot may not yet be readable by name: a 404 at that point retains deletion
debt. If a deletion reply is lost and its name subsequently disappears, cleanup
conservatively remains indeterminate; no absence poll manufactures an acknowledgment.
Checkpoint results and their original settlement replay after reopening. The
checkpoint projection also retains the provider's opaque accepted address; inspect,
fork and deletion never assume its public UUID is the native snapshot address.
The core sequences checkpoint forks, validates each child before projecting its
identity, retains accepted cleanup addresses on metadata or identity failure, and
waits for children in their admitted index order. Checkpoint inspection requires
its canonical admitted record: an untracked native snapshot cannot reconstruct
its historical contract from the source machine's current state. Creation time
and the original contract stay immutable across later native reads. A conflicting
logical machine ID retains the new native cleanup address without replacing the
original binding.
A different provider is refused before native IO. This draft's unpublished journal
schema changes with that projection; it does not implement a legacy journal shim.

Machine and checkpoint destruction share admission, target binding and terminal
state publication. The core refuses a memory-fork source with live native child
states; deletion receives the canonical opaque address. Failed native deletion
cannot publish destroyed state, and an exact retry of a successful deletion
returns the original settlement without observing or deleting again.

Suspend/wake admission and policy transitions are shared too. Each native mutation
receives the reopened canonical binding and records its opaque native target;
the logical machine UUID need not equal a provider resource address. Native readiness is
separate from public state: a Running VM may still be busy with a fork or snapshot.
A destroyed canonical observation cannot be replaced by a stale native read.
Native VM forks use this same owner for admission, source readiness, tree rounds,
accepted-child custody, provider metadata publication, final readiness and ordered
settlement. Provider metadata is written only after the child binding and cleanup
ownership publish. Daytona reads the child's current labels for this wire update;
this preserves unrelated native labels without a process-local pending-label cache.
That adds one metadata read per native child compared with the previous adapter;
no second lifecycle observer is retained. Fork rounds join every started sibling
before reporting failure, retaining cleanup ownership. Only an explicit definite
native refusal permits dispatch backoff; a lost reply never does. Machine/event page construction belongs to this core.
The operation registry is available without a Cargo feature on native and browser
targets. Public provider ownership and stream aliases use Streams' existing
platform bounds: native calls remain Send/Sync, while browser calls retain their
own event-loop handles. The native readiness helpers still require the native
Tokio runtime; the shared registry itself introduces no browser timer or scheduler.
The simulator now uses this same registry for admission, exact replay, recovery,
cancel, operation watch and state events. Its local world holds only simulated
machines, immutable native snapshots, parent relationships and its deterministic
clock. There is no local outcome or event map. A busy native-world deletion is a
definite refusal; canonical cleanup keeps that debt for the original operation.
Machine/checkpoint identities and successful-mutation clock ticks stay unchanged.
Simulation operation identities now use the same canonical operation derivation
as every other adapter, replacing its former separate digest domain.

The simulator's existing bounds (4096 operations, 1024 machine identities and
4096 events per machine) are enforced by the shared core. Operation reservation
and its operation publication share one conditional Streams commit; definite
pre-dispatch refusals release the reservation in the same commit that forgets
admission. Native machine capacity is checked while the simulator holds its
physical-world lock. Event admission precedes canonical machine/event publication.
Native registries keep their previous unbounded profile unless their composition
root explicitly chooses limits. Reopen a namespace with the same profile.
The raw backend migration is in progress; the operation authority alone does not
establish that all adapters meet the approximately 300-line budget.

Validation runs on bb2 without provider credentials. LocalStream reopen vectors
cover exact replay, stable settlement identity, indeterminate admission, cleanup
retention, atomic state/events, and a maximum-child fork result larger than one
record. MemoryStream vectors cover independent instances racing admission and
following completion. These are internal vectors, not captured provider responses.
The PR receipt records the tested source and results; these tests do not qualify
paid-provider execution or atomic snapshots.

Live Daytona runs require `DAYTONA_MACHINES_JOURNAL` naming a durable
account-specific directory. Restart with that directory and the original keys.

The current Daytona adapter targets the `/sandbox` lifecycle API. Its documented
[TypeScript waitUntilStarted method](https://www.daytona.io/docs/en/typescript-sdk/sandbox/#waituntilstarted)
uses state polling; that is the readiness mechanism currently bound here. The
[newer v2 SDK](https://www.daytona.io/docs/en/v2/typescript-sdk/sandbox/#sandboxeventsconfig)
documents a WebSocket events hub for its different API. This draft does not claim
that no native push mechanism exists, or silently substitute that API for the
selected one. Raw completion-backed adapters should await native completion;
operation watch/join always follows Streams rather than a readiness timer.
