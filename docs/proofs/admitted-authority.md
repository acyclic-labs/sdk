# Invocation-owned authority

The trusted opener obtains one `AdmittedAuthority` from authenticated dispatch.
It binds an immutable original scope, public trust, checkpoint verification key,
original absolute `Context`, platform operation owner and its real Auth trust
publication source. `RuntimeAuthority`
passes that same owner through the server, turn executor and volume factory.
The WebAssembly constructor accepts `WasmAuthorityCapability`; it accepts no
issuer seed. The guest receives sealed Auth evidence, public keys and the
canonical Auth `CallerContext`, including the original customer, recipient,
delegation dependencies and invocation-scoped handle.

The platform must authenticate that dispatch binding. Decoding `CallerContext`
is representation validation, not authentication. Neither a requested agent nor
a command's account field establishes authority. A platform deriving an acting
agent must authorize the actual resource producer and owner. Native enrollment
explicitly requires native custody instead of manufacturing a local issuer.

## Safety

The admitted parent is immutable. Foreign derivation authenticates its parent
and exact returned id, labels, acting agent, issuer and parent proof. Every
certificate key id and epoch and every ancestor revocation dependency must
survive; validity may only narrow. Ordinary attenuation cannot add a label.
Private file and directory sharing use the shared original-owner policy, then
issue one exact read grant. Restricted append tips are rejected at admission,
parent and result boundaries; public export seals directly into its one output
buffer. Typed checkpoint operations bind the original issuer and audience,
checkpoint domain and exact digest. There is no unrestricted signing port.

Retained content grants observe their original owner's current synchronous
guard for the exact retained scope as well as compact Auth evidence. Fresh
scope admission does likewise. Revoking a child must refuse even while its
original caller remains live and the guest holds an older public-trust snapshot.
Foreign callbacks run outside the public trust lock. An asynchronous-only
validity callback cannot qualify synchronous retained access. Public historical
checkpoint verification remains possible after runtime custody closes.

The actual-crypto controls cover changed results, dropped intermediate ancestry,
append-tip export, changed checkpoint domain/digest, foreign private owners,
prefix escape, narrowed grants, revocation and retained access after owner loss.
Counterfactual controls remove the relevant fence and require an assertion
failure, rather than treating a compiler failure as detection.

## Progress

One inline operation future races the original owner stop event and the original
request stop event. Both events come from their existing `Context`; no worker,
readiness probe or guest timer is created. Native producer loss closes its
channel. A foreign owner must settle its original `stopped` promise when its
producer closes, rejects or expires, and settle each operation on producer loss.
Dropping an in-flight WebAssembly call aborts its supplied `AbortSignal`.

Stopping before dispatch refuses without a foreign call. After dispatch, stop
or lost completion returns `Indeterminate` under the original operation id.
Neither side retries an uncertain operation. Tests exercise held futures and
completion in the same poll; a dropped producer must wake the pending waiter.
The separate host-module control closes the original owner while one real guest
operation is pending and requires cancellation and exactly one dispatch.

A turn's journal binds its original interrupt token to the same platform
checkpoint port. Execution, working-context and model-request checkpoints all
observe that event before dispatch and before admitting a ready signature.
Cancel or archive therefore drops a pending platform call even when the first
checkpoint precedes model dispatch. The control uses an actual private-volume
journal and live signing owner: it requires the original interrupt to wake every
held checkpoint, typed cancellation, and one dropped call. A stopped turn cannot
dispatch; an interrupt produced by a ready signature still refuses completion.
A fresh turn under the same owner remains able to sign and verify.

## Required work and cost controls

Counts below describe one operation, excluding its caller's payload construction
and the selected platform's implementation. No touched authority operation adds
a syscall, durability barrier, disk open, filesystem parse or remote token mint.
Authority issuance and typed checkpoint calls are distinct from token mint RPCs;
a fixed token already has zero token mint RPCs.

| Operation | Before | After | Guarantee buying the remaining work |
| --- | --- | --- | --- |
| Warm agent admission | Mint and discard one agent scope | Zero scope mints; zero credential signature verifications | Original grant and actual private owner intersect at admission |
| Admission root binding | Clone fresh and retained bindings into a temporary vector | Borrow the selected private binding | The selected binding alone proves its private owner |
| Native issue/checkpoint dispatch | Synchronous local issuer | Same issuer inline; zero new tasks, timers, channels or boxed futures | Original cancellation checks fence publication |
| Platform scope derivation | Guest holds issuer material | One owner callback, one sealed response authentication | Signing stays with the admitted platform and exact ancestry is checked |
| Platform checkpoint | Guest holds issuer material | One owner callback, one returned signature verification | Typed domain, audience and digest are authenticated |
| Public sealed export | No dedicated public projection | One encoded output allocation and copy; fixed stack signing transcript | Append authority stays with its custodian |
| Retained validity observation | Public trust evidence alone | Exact-scope owner guard plus public evidence, with guards around fresh authentication | Closed or revoked invocation or child cannot expose retained content |

The deterministic admission control observes cryptographic work for 1, 100 and
1,000 warm admissions, then compares the old mint-and-discard operation against
the same issuer, scope and private binding. It requires zero verifications on
every warm run and nonzero work for the old operation. Auth's allocation controls
require zero allocations for dependency comparison and one output allocation for
sealed export. These are work counts, not wall-clock latency claims.

Owner selection, dispatch and stop-event selection have constant work in history
and agent count. Credential authentication and dependency comparison are linear
in credential bytes/dependencies; filesystem grant checks depend on the supplied
grant and binding sizes. These input-dependent operations are not claimed to be
constant time in their input size. No retained history or agent scan is added.
The WebAssembly exact-scope observation encodes public scope evidence for its
existing synchronous callback; that copy is proportional to the scope bytes and
buys current child revocation independent of the guest's older trust snapshot.
Settled foreign operations disarm their cancellation controller: zero abort
dispatches on completion, one abort if an in-flight operation is dropped.
The turn interrupt adds one retained token reference to a platform journal and
one temporary reference/event waiter to a pending checkpoint. Both use the
original token; no allocation, task, timer or additional callback is introduced.
Native checkpoints retain their inline path and create no interrupt waiter.
Journal setup uses the same stop-event selector before awaiting its scope
derivation. A held derivation is dropped when the original turn is interrupted;
a ready result racing that interrupt is refused before the executor is created.
This adds one temporary token reference and event waiter during setup, with zero
new allocations, tasks, timers or calls. The deterministic private-volume control
keeps the invocation owner live, checks wake and drop of a held derivation,
refuses a stopped turn before dispatch, and admits a fresh turn under that owner.

Agent record observation uses the existing Stream replay-to-live cursor through
`WasmAgents.follow`. The adapter resolves the same three paths as `read`, retains
one shared provider reference and encodes one canonical FollowRequest. It opens
no second store and adds no task, timer, syscall, durability barrier or network
request. Cursor delivery, cancellation and provider termination remain the one
Stream implementation; closing the cursor wakes a pending `next`.

The real browser controls formerly reread up to sixteen records after each
20 ms sleep and delayed paused-state assertions by 200 ms. They now open one
cursor per required observation and consume only producer events; periodic reads
and readiness timers are zero. The pause commit itself is the durable fence.
Model dispatch is observed directly at its actual callback. A terminal fixture
producer error ends every outstanding observation with that error, and teardown
closes the owner. Fixed fixture histories are retained only for their assertions.
This is a work-count improvement, not a wall-clock latency claim.

Local-root enrollment still checks every fresh volume with the shared
`VolumeRef::require_writer` predicate before preparation can write objects. A
valid private binding cannot authorize a second volume owned by another agent.
The accepted path adds one constant owner comparison, zero allocations, IO,
barriers or credential mints. The existing admission work control accepts the
agent's own volume, refuses the foreign volume through the real owner predicate,
and observes zero credential signature verifications. Removing that predicate
must fail the foreign-volume assertion. No second owner-policy implementation is
introduced.

Guest-session admission retains the same canonical selected volume, verified
scope, funded work and physical mapping fence. Its existing lifetime monitor
subscribes to the original runtime's trust publications and rechecks that exact
scope on each event. Platform custody also retains the invocation's original
Context; producer loss or cancellation cancels that same native work while idle.
Native custody retains its existing expiry alarm. Platform custody reuses its
original deadline event, adding zero expiry alarms, tasks, syscalls or RPCs.
The one existing monitor gains a shared Context reference and a pending event
waiter; its frame remains charged before spawning. No history or agent scan is
introduced. The real lifetime control checks native revocation, monitor loss,
admitted-owner revocation and original stop-producer loss, with zero volume IO.


`acyclic_auth::trust::ChangeSource` is the one native/hosted publication contract.
`AuthorityVerifier::from_public` requires that source, and
`AdmittedAuthority::from_platform` requires the same source identity. No public
verifier creates a detached sender. The native signer alone owns its existing
watch sender; public consumers retain only a receiver. Signer loss therefore
settles the event as `SourceClosed`, without destroying historical evidence.
A hosted source follows its authenticated owner's trust stream, registers before
the exact synchronous guard, and reports producer loss through that same event.
The WebAssembly `trustChanged(signal)` adapter requires a genuine owner Promise;
throwing, malformed and rejected registrations close the source. Dropping the
pending registration aborts only that observation. The isolated host fixture
adapts the actual native Auth source, with no second publication channel.

The independent-verifier control enters through `from_public` and
`from_platform`, registers the shared monitor, then revokes while the invocation
stop producer remains live. A second case drops only the signing source. Both
must settle the monitor immediately on its next event-driven poll and cancel
the original funded work; the attached memory-owner fence then releases its
private allocation and delivers its completion. Backend reads/writes stay zero.
An unrelated still-live owner's publication channel is the negative control:
that monitor remains pending after the actual owner's revocation or loss. The
assertion occurs before any completion await, so this fault is detected without
a deadline or sleep. A mismatched constructor source is also refused.

Before, an independently constructed verifier allocated one disconnected watch
channel. After, it retains one shared reference to the original source, with no
new channel. Native source construction adds one receiver adapter allocation per
signing owner. Each active monitor holds one boxed publication future at a time;
it replaces that future only after a real publication, never per filesystem
request. This replaces the previous borrowed native watch future with one
allocation per registered publication wait, the cost of the portable erased
owner event. No new task, timer, syscall, durability barrier, credential copy or
RPC is added. Dispatch and normal retained checks add zero publication calls.
Each monitor recheck is bounded by its own credential dependencies; existing
publisher fanout depends on subscribers and is not claimed constant in that
count. No history, account or agent scan is introduced.
