# GraphCoder on Actors

`acyclic_harness::server::Actors::graphcoder` composes the GraphCoder preset,
stock executors, admitted filesystem placement and canonical Actors runtime.
Pass it to `ServerConfig::new` and `server::serve`, the same entry used for a
managed deployment and a self-deployed server. Enable the harness `serve` feature.

The host injects its signing authority, verified caller, `Placement`, stable root
creation intent, current authorization policy and `Execution`. Execution takes
the selected `Model` and `Arc<dyn ModelProvider>` together; production providers
can be resolved through `acyclic_harness_providers::Registry`. Provider credentials
remain in the host. The preset supplies GraphCoder's prompt, tools and approval
policy. Additional workspace tools and task context are explicit host choices.
Storage implementations stay below the shared filesystem and Actors engines.

An admitted workspace executor is the home target in `runtime::Bindings.targets`.
Build those bindings once, then pass `filesystem::Tasks(bindings)` as the turn
context. It binds each turn's original volume grants, content access, execution
journal and approval policy to that turn's operation. Native context manifests
and hooks use the same `Tasks::bind` operation. The workspace executor therefore
receives the original task context rather than an unrelated tool callback.
Workspace availability is selected per tool name: a Machines shell binding
declares `shell`, without also advertising the separate `fs` workspace command.
Unimplemented tools receive neither model definitions nor call grants.

For a portable host, retain the original Machine admission observation and the
selected `VolumeRef`. Construct the public workspace with the same caller and
verifier used to admit the server:

```rust
let workspace = machines.workspace(observation, &volume, scope, verifier)?;
let workspace = acyclic_harness::WasmMachinesWorkspace::from(workspace);
registry.bind_machines_workspace(&workspace);
```

The trusted Rust platform opener returns that opaque handle from the same WASM
module. JavaScript can pass it to `registry.bindMachinesWorkspace(workspace)`
before opening `WasmAgentServer` or `AgentGuest`. Both use the same registry and
`Providers::executors` composition. The handle retains the actual authenticated
provider, selected Machine and volume, parent verifier and execution authority;
each turn supplies its original task context and cancellation. It accepts no
replacement shell callback or serialized credential. The platform must supply
its actual public Machines transport and admitted workspace opener; this handle
alone does not supply that transport or deployment admission.

Keep the same root intent when reopening storage. An uncertain root publication
returns `ActorsError::Unknown` with its `PendingActors` composition. Retain that
handle, then pass it as `ServerConfig::harness` after the provider's recovery
event. Opening it submits only the original prepared commit, including its
publication holds. Startup performs one submission per explicit open; it starts
no retry timer or retry loop. Shutdown cancels both opening and credential
publication. A runtime that ends during credential publication also ends startup
without announcing an endpoint. The endpoint owns its runtime:
dropping the endpoint aborts execution, and a failed runtime ends serving instead
of accepting work without a producer. TLS is required away from loopback.

Native applications use `hosting::Runtime::start(server)` to own that loop.
Both the local composition and `server::Actors` use this owner. Its `lifetime()`
feeds the existing in-process Session engine, and `completion()` ends network
serving. A failed delivery retains its original input for recovery and closes
the application's sessions. A client connecting after failure also sees the
closed lifetime. Local observers receive errors through the Session, without
an additional per-agent failure channel. Native workspace tools, admitted
filesystem authority, approval policy and root reopen remain in their original
compositions; this runtime owner alone does not provide deployment admission.

## Public client qualification

Provision a disposable served tree with a model capable of answering the task.
Create `scenario.json` with an intent unique to that run and an independent
answer oracle, for example:

```json
{"id":"answer-1","prompt":"Reply with the word READY.","expected_text":"READY"}
```

Supply `ACYCLIC_API_KEY` through the process environment, then run:

```sh
cargo run -p acyclic-conformance --features client-session --bin client-conformance -- --scenario scenario.json
```

`--endpoint URL` overrides `ACYCLIC_ENDPOINT`, which overrides
`https://swarm.cloud.acyclic.dev`. Credentials, host routing and expected response
bytes do not belong in the scenario. The client discovers the admitted binding,
root, children and approvals through the public protocol, and checks foreign
tree refusal. It checks exact prompt replay, conflicting
retry refusal, committed reply correlation and complete immutable output reads.
This sends a model request and retains its effects in the served tree.

The deterministic integration check is:

```sh
cargo test -p acyclic-conformance --features actors-hosting --test hosted_actors
```

It runs the public endpoint vectors through the GraphCoder composition and a
controlled model producer, then drops that producer while a reply is pending.
It also drops the current authorization decision's producer at each public
command wait: binding, root, children, approvals, foreign-tree refusal, initial
prompt, exact retry, conflicting retry, settlement, reply and output read. The
owning server is stopped and its task is separately aborted during an in-flight
root command and while the reply subscription is actually pending. These waits must end with typed
`Unavailable`; a closed listener retains its original typed connection error.
Disposing the server during root credential publication returns `Cancelled`
without announcing an endpoint, while the credential publisher remains alive.
It checks the GraphCoder
prompt at model dispatch. This check does not qualify a live inference
provider or a managed deployment; run the endpoint command above against those.

The lower-level observation, control and tool-output fixtures remain available
through `client-fixture-conformance` for hosts that provision those exact vectors.

## Portable session boundary

Select a production model with the public `ModelRegistry`, then call
`selected.install(agentRegistry, compaction)`. The same Rust model provider owns
request admission, content verification, capacity, counting and streamed events;
the host does not reconstruct these through `configureModel` callbacks. Supply
the registry's fetch binding from the platform when outbound authorization belongs
to a protected service binding. The GraphCoder preset supplies the same prompt,
stock filesystem tools and per-turn agent tools in native and portable hosts.

An event host exposes the same canonical protobuf session with
`EventAgentHost.session(next)`. A platform duplex binding supplies complete
incoming frames through `next(signal)` and pulls outgoing frames from the returned
cursor. Return `null` on input EOF, cancel the pending platform read when `signal`
aborts, and call `cursor.close()` on disconnect. Call `EventAgentHost.close()`
when its platform owner is disposed. Failed delivery ends the current generation
of sessions and commands with typed `Unavailable`; subsequent events can recover
from committed state and the retained alarm. Platform disposal is permanent.
Direct storage controls share that lifetime: closing the owner cancels their
pending reads and refuses subsequent work before storage is accessed. Platform
policy and admission callbacks receive the original operation's `AbortSignal`.
Metadata flushes, placement, alarm writes and queued event commands use the same
owner lifetime. Closure releases their callers while the admitted primitive is
still pending, and prevents queued commands from starting storage work. Retained
creation intents and recovery alarms remain available to the next owner.
Stream failures retain their canonical string `code` at the JavaScript boundary.
An alarm update failure retains the healthy host and its previously admitted
recovery alarm, so a later wake can resume without replaying a committed turn.
The shared Rust session engine
owns credentials, admission, flow control, calls and follows. There is no second
agent protocol or background writer. Stream follows are available; filesystem
resource follows require a platform resource provider.

For standard gRPC bodies, import `serveGrpcSession` from
`@acyclic-labs/harness/hosting` and pass the event host and original `Request`.
It returns HTTP status, headers, a pull-based response body, terminal `trailers`
and `close()`. The deployment runtime must publish those trailers through its
public HTTP/2 primitive; a WHATWG `Response` alone cannot express them. The body
is duplex: receiving the Welcome or an ActorCall reply does not wait for upload
EOF. Disconnect and request cancellation close both directions.

`EventAgentHost.grpcSession(next, signal)` and `WasmAgentServer.grpcSession(next)`
use the same session engine with uncompressed five-byte gRPC envelopes. One
shared decoder handles split headers, split bodies and coalesced frames. The
session accepts `SessionSubscription.kind.actor_call` (field 14): canonical
`acyclic.actors.v1.InvokeActorRequest` protobuf bytes. The response is canonical
`InvokeActorResponse`, carried in `SessionData.item.actor_call` (field 14).
Fragments have `after.next` starting at one and an immutable eight-byte
little-endian total in `after.opaque`. Calls are never automatically replayed.

The guest Session controls run against the shipping WASM host:

```sh
bun install --frozen-lockfile
bun scripts/generate-proto.mjs
bun scripts/build-wasm.mjs harness
bun test typescript/packages/harness/test/host-liveness.test.ts typescript/packages/harness/test/host-session.test.ts
```

They exercise duplex replies and typed producer, owner and request loss. A
cancelled opener cannot release the admission fence ahead of its predecessor.
Framing adds no RPC or durability barrier: a complete contiguous message decodes
from its input chunk; only an incomplete message is buffered. Output encodes
directly into its final envelope, without an intermediate payload copy.
Provider RPC cursors carry length-delimited protobuf session frames. The shared
Rust decoder preserves messages when a byte transport splits or coalesces chunks;
a partial frame at EOF is `InvalidResponse`, and an ended live follow is
`Unavailable`. Finite reads end normally between complete frames. This adds only
the protobuf length prefix and retains at most one incomplete frame, without an
additional RPC, storage operation or background task.

Journal configuration retains the core's opaque revision authority in the same
durable store. Reopening replays under that authority, preserving exact commit
receipts and minimum revision cuts. The storage adapter only saves and returns
the admitted bytes; the shared Rust revision engine owns their interpretation.

The public binding announces the same admitted private-volume resource on
native and portable hosts. Constructing its resource name does not require a
native filesystem watcher.

Exercise disposal against the shipping WASM host with:

```sh
bun scripts/build-wasm.mjs harness
bun test typescript/packages/harness/test/host-liveness.test.ts
```

This byte boundary alone is not a deployable Actor application. Public deployment
bindings must supply storage, verified admission, model and Machines I/O, and the
platform's HTTP or socket framing. The native integration check does not qualify
that deployment or its live inference provider.

## Deployable Actor artifact

`@acyclic-labs/harness-actor` is that same event host and session engine as one
Actor application. Build it from an owner configuration with `options`,
`ledger_limits`, `model` and `deployment`:

```sh
bun scripts/build-harness-actor.mjs --config owner.json --out artifact
```

`artifact/` then holds `harness-actor.mjs`, which exports `actor`, every
WebAssembly module it loads, and `agent-deploy.json`. The entry declares each
module in its `assets` with path, SHA-256, size, media type and role. Static module
URLs retain their importing file's exact identity during bundling. Only surviving
references enter the closure; each selected WASM file is read and hashed once per build,
even when several references name it. Equal basenames in different source modules
remain distinct when their bytes differ. Unrelated generated packages are never
inventoried. The entry digest is also computed once. `verifyArtifact` admits an
artifact only when its files are exactly the entry and those declared assets. The
build runs that check, then loads the artifact from only the declared files,
before writing it. The entry selects
the bindings named in `src/bindings.ts`; the platform defines each binding's
resource kind. Its model credential is only a `secrets:/` reference, and the
validator refuses an inline value with the `secret-reference` rule. A deployment
missing any harness binding is refused before deployment, and an environment
without a bound authority is refused on restore. `eventStorage` and
`streamJournal` both select the actor's own state transaction binding: host
metadata, wake rows, journal frames and the alarm commit as one packet, and
journal history is paged, never copied into metadata. `fetch` is the one entry,
`serveActorFetch`: it serves the Stream session as a guest response stream whose
terminal gRPC trailers follow the body, and runs an agent call the platform
routed from a session ActorCall (`/agents/bind`, `/agents/command`, with the
caller's original command as the body) through the same authenticated call,
under the credential of the invocation's `hostAccess` admission. Its tree lives
in that one actor's journal, so the artifact runs no children: it binds a preset
with `max_children: 0`, its spawn tool answers `unsupported capability`, and a
spawn command is refused as `spawn_refused` before anything commits.

`machinesWorkspace` is a `BrowserMachinesWorkspaceSource` (`provider`,
`observation`, `open`). The entry opens it with
`BrowserMachinesWorkspace.open` in its own module, under its platform authority,
and binds the `shell` tool to it. Its grant needs only `machines/exec`. The
platform creates the Machine through the entry's host-only `hostFetch` endpoint,
built from the owner's `deployment.machines` (`origin`, `credential`).
The credential is `{scheme, reference}`. It defaults to `account-leaf` at
`secrets:/account/leaf`, the platform's one account leaf, which every
host-only endpoint shares. With `bearer`, the referenced secret is sent as the
bearer. With `account-leaf`, the referenced secret is the JSON object
`{key, certificate, birth}` in the platform's own leaf encoding: the
leaf's private key, its account-signed certificate, and the account birth.
Only the host reads it. For each exchange the host mints a short-lived bearer
that its own platform verifies. Its lifetime is `min(caller deadline, the
platform's maximum bearer lifetime)`, and where the platform supports scoping,
the bearer is scoped to this endpoint. The host sends it as
`Authorization: Bearer`. Every bearer is minted inside its exchange, so no
stored token is ever refreshed. The key never reaches the guest, a stream, a
volume, a log or a fixture.
Admission refuses a host-only endpoint whose credential any environment secret
declares, or whose origin any guest fetch allow-list names, so the guest never
holds Create authority. The entry's `runtime.actorMachine` names the binding,
that `endpoint`, and the owner's `options.filesystem_provider` as
`volumeProvider`. The Machine's selected
volume for actor `id` is
`filesystem::private_volume(volumeProvider, &AgentId::hosted_root(id))`, the
volume the entry creates. The platform authority must grant that volume's read
and write capabilities and `tool_call("shell")`.

## Required work

| Operation | Required work | Removed work |
| --- | --- | --- |
| Portable model dispatch | One prepared Rust request and the selected provider's HTTP exchange | Serializing the request into a JavaScript model callback, then translating its events back into Rust |
| Endpoint serving | One shared session engine and one runtime future for the admitted tree | A separate agent turn loop or command protocol |
| Uncertain startup | One submission per explicit open; one retained pending handle only on uncertainty | Unbounded immediate root resubmissions and a second prepared request |
| Waiting for an answer | One status subscription at an explicit committed cut, registered before the reply recheck | Periodic status requests |
| Known host shutdown | One terminal control frame per active channel, independent of data credit | Reconnecting to an endpoint whose runtime has already stopped |

These are source-level operation counts. The integration check measures functional
liveness; it does not establish throughput, memory or live model latency.

## Host-supplied bindings

A host adapter outside the artifact's bundle hands the entry plain data and
callbacks only; every WebAssembly object is constructed inside the bundle.
`materializeBindings` from `@acyclic-labs/workers` selects each binding's
producer after the Rust validator admits it, by the product of its
`product:/path` resource name: `objects:/…` selects `objects`,
`machines:/…` `machinesWorkspace` and `streams:/…` `streams`, at any path.
Any other product, including look-alikes such as `objects-other:/x`, is
refused before a producer runs.

`platformAuthority` is a `WasmAuthorityCapability`: the invocation's
`admission` (issuer, audience, sealed scope, public trust snapshot, public
checkpoint key, canonical `CallerContext` bytes, operation and deadline) plus
its `stopped`, `check`, `trustChanged`, `trustClosed`, `derive` and
`checkpoint` callbacks. The entry builds its agent authority from it.

`hostAccess` is an `EventHostAccess`: `{ trust, credential, policy }`. `trust`
is a `SessionTrust`, and the event host builds its session verifier from it:

| Field | Value |
| --- | --- |
| `trust` | v1: UTF-8 JSON of `acyclic_auth::trust::Snapshot` (`environment`, `environment_keys`, `account`, `issuer_keys`, `minimum_key_epoch`, `revoked_key_ids`, `revoked_block_ids`), the value `admission.trust` carries. Unknown fields are refused. |
| `audience` | Canonical recipient resource each session credential must name. |
| `holder` | Presenting account each session credential must name, or `null`. |
| `changed(current, signal)` | Optional. Resolves with the same owner's complete current trust once it differs from `current`. A rejection or a non-monotonic publication closes the host. |

The descriptor holds only public keys, revocations and the signed scope; the
scoped session `credential` is the existing separate field.

`objects` is the admitted account's `ObjectsV1BinaryPort` from
`@acyclic-labs/objects`, `{ open(route, input) }` owning one canonical binary
Objects v1 request; every `ObjectsV1Provider` is one. The harness opens it as
`new WasmObjects({ port })` and its volumes use the same blob client as every
other Objects v1 host: a read is one `GetBlob`, an existence check one
`HeadBlob`, a put one `PutBlob` per object (at most the shared batch window in
flight), and a publication root, when set, one `Retain`. The grant needs
`objects/head`, `objects/get`, `objects/put` and `objects/retain`. The host
serves the binding through the entry's second host-only `hostFetch` endpoint,
`objectsEndpoint`: the same origin as the Machines endpoint, always with the
account leaf. Like the Machines endpoint, neither its origin nor its leaf ever
reaches the guest.
