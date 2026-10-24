# Actors and Workers identity, deployment and routing pin

Written by Codex, 2026-10-24, before implementation of the queued parity steers.
SDK evidence is from addressing head `35de549bb3d561f379be137b582a3470dd60579c`
(merged as `19f451bf99a4d7257917e104065e8615e5ce5949`). Cloud evidence is the
coordinator-supplied `cloud-contract-20261023.md`, sections C1–C6 below. Cloud
source was not inspected. This table records required behavior, not a claim
that the live service already implements it.

## Pinned SDK contract

- Actors have the plain identity `(account, namespace, id)`. Namespaces and actor
  IDs are case-sensitive ASCII strings of 1–128 bytes in `[A-Za-z0-9-_.]`.
  Neither is hashed, Unicode-normalized, escaped or used as a hostname label.
  Accounts retain the existing opaque nonempty byte representation.
- `__default` is reserved for existing account/id actors. An absent legacy
  namespace resolves to this exact value, retaining the existing persisted ID.
  Explicit namespace deployment cannot claim the reserved default namespace.
- `idFromName(name)` retains the name as the plain id and exposes its name
  provenance. `newUniqueId()` uses 128 random bits rendered as 32 lowercase hex
  characters; there is no mathematical collision-free allocation claim.
  `idFromString()` validates an exact id spelling, without recovering name
  provenance. Equality compares namespace and exact id, not provenance.
- Namespace deployment is explicit and versioned, with code digest, config,
  bindings and jurisdiction. `namespace.get(id, { locationHint })` is local and
  performs no existence query or creation RPC. First invocation activates the
  object; first state write materializes it. There is no per-actor create API,
  AlreadyExists result or per-instance deployment config. `deleteAll` clears
  state and alarm; a later call can activate the same address again.
- An actor reference is an address, never authority: eight zero bytes, ASCII
  `acyclic.actor`, version byte `1`, then u64-BE length-prefixed account,
  namespace and id, with no trailing bytes. Eight initial zero bytes are an
  invalid pair encoding (empty account), making the two accepted grammars
  disjoint. Old pair vectors and encoding remain unchanged. Missing namespace
  conversion is explicit at the legacy field boundary, never guessed by a
  binary decoder. Logs carry separate fields; no universal text URI is added.
- Actor access uses namespace bindings/capabilities and invocation, not DNS.
  A deserialized reference grants no ability to call it. Cross-account access
  requires an explicitly authorized binding to that account and namespace.
- Workers are named script deployments `(account, worker-name)`, not actor
  instances. Worker deployment and namespace deployment share versioned code,
  config and binding data; Worker names remain account-scoped. Worker hosts
  remain `<id>.<account>.<domain>`, including `run.cell-1.example` suffixes.
  Edge strips the exact configured suffix, requires exactly two remaining
  labels, re-renders through `hostname()` and compares bytes without folding.

## Parity table

Cloud contract shorthand: C1 identity; C2 owner routing/fencing; C3 bindings;
C4 hosts/custom domains; C5 alarms/sockets; C6 placement.

| Capability | Cloudflare guarantee | SDK and Cloud today (evidence) | Verdict and reason | SDK change if NEED |
| --- | --- | --- | --- | --- |
| Namespace/class scope | IDs belong to a namespace backed by a class [N]. | `actors/src/domain/identity.rs:7,87` has only ActorId and pair projection. C1 live has no namespace; entity has one. | NEED: user selected triple, same id permitted in different namespaces. | Validated namespace and actor-reference triple, namespace-aware request/response fields and bindings. |
| Existing actor namespace | Namespace determines identity [N]. | Old RPCs carry actor_id only (`actors/proto/actors/v1/actors.proto:42,119`); C1 live account/actor. | NEED: preserve persisted IDs without aliasing named namespaces. | Reserved `__default` and explicit absent-field mapping; additive vectors, no ID rewrite. |
| Name-derived IDs | `idFromName` deterministically selects an ID [N]. | `ActorId::new` checks only nonempty (`actors/src/domain/identity.rs:97`); C1 live accepts the stricter ASCII alphabet. | DELIBERATE DIFFERENCE: plain name replaces CF's generated identifier, as ruled. | Add scoped idFromName with exact plain spelling and strict shared validation. |
| Random IDs | `newUniqueId` returns a random ID that callers can save [N]. | No random-ID API (`actors/src/domain/identity.rs:87`); C1 create identity comes from entitlement. | NEED: SDK allocation must not require actor creation. | Generate 128 random bits, lowercase hex; namespace-scoped typed ID; no registry or existence check. |
| Parsing, equality, name | IDs support parsing, equality and optional name provenance [I]. | String ActorId has Eq, no provenance or parser beyond nonempty (`identity.rs:4,97`). | NEED: distinguish identity equality from construction metadata. | Validated idFromString, equals, optional name; equality ignores provenance. |
| Serializable references | Stubs refer to one object; IDs may be saved [N,S]. | C2 explicitly has no serializable stub; SDK Invoke carries only actor_id (`actors.proto:119`). | NEED: address transfer must not transfer capability. | Versioned canonical triple codec and bound-stub reconstruction; no credentials in serialized data. |
| Implicit activation | Obtaining an ID is local; access instantiates lazily [N]. | Explicit CreateActor RPC (`actors.proto:185`; `actors/src/client.rs:390`); C1 live explicit, entity lazy. | NEED: latest user ruling adopts lazy actor activation. | Remove CreateActor API and callers; get returns a stub; invoke targets namespace/id. |
| Namespace code/config | Class deployment supplies implementation [N]. | Per-actor code/config (`actors/src/domain/mutations.rs:8`; C1/C3). | NEED: deployment belongs to namespace, not instance. | Explicit versioned namespace deployment; migrate per-instance code/config APIs and tests. |
| Single live owner | One object owns its state; stub calls address it [S]. | C2 Raft leader and activation-term fences already provide owner discipline; SDK transports invoke a service (`actors/src/client.rs:390`). | HAVE server fencing; NEED SDK namespace-aware targeting. | Carry exact triple; leave routing epochs internal and do not invent a client ownership protocol. |
| Stub call order/failure | Calls on one stub have E-order; a broken stub fails subsequent calls [S]. | Generic independent InvokeActor HTTP requests (`actors/src/http.rs:288`); no bound stub. | NEED: namespace routing alone cannot promise order. | Bound stub with serialized admission and terminal broken state; Cloud must preserve ordered delivery. |
| Delete/recreate fencing | Storage deletion permits later state creation [D]. | DeleteActor RPC (`actors.proto:101`); C2 retains epochs across recreation. | NEED: public operation is deleteAll, not address deletion. | Storage clear API targets triple and clears alarm; expose no generation; Cloud owns epoch fencing. |
| Initialization gate | Constructor plus blockConcurrencyWhile serializes initialization before delivery [D]. | restore/event/checkpoint (`actors/src/guest.rs:23`; `guest/authoring.rs:176`) lacks explicit initialization gate. | NEED: retain cross-stream transactions while gating calls. | Typed activation/initialization authoring API and serialized gate; Cloud enforces before delivering any event. |
| Location hint | get accepts placement advice [N,L]. | C6 absent; old home_region is per-instance (`domain/mutations.rs:12`). | NEED: hint is advisory and belongs to first access. | Typed get options carried on invocation; preserve hint with stub. |
| Jurisdiction | Namespace restriction constrains placement [L]. | C6 absent. | NEED: pinned namespace-level restriction, not a hidden identity field. | Validated stored jurisdiction in namespace deployment and typed binding; Cloud rejects placement outside it. |
| Worker identity/deployment | Named scripts have deployments, not durable addressable instances [B]. | `workers/src/deployment.rs:1,92` calls CreateActor; C3 stateless Actor. | NEED: instance create is wrong abstraction. | Shared versioned deployment shape; explicit worker deploy, invoke service and delete deployment. |
| Default Worker host | A Worker may have a public endpoint [R]. | Pair hostname (`auth/src/addressing.rs:146`); C4 `<deployment>.<account>.run.<domain>`. | HAVE renderer; NEED exact suffix parser/conformance. | Round-trip host parser and additive multi-label suffix vectors; no slug, alias folding or universal text identity. |
| Custom domains | Custom host routes to a Worker [R]. | Manifest RouteSpec (`manifest/src/lib.rs:271`); C4 TXT ownership proof with bounded re-verification. | HAVE Cloud ownership verification; NEED typed deployment route selection. | Preserve canonical host and custom-domain declaration; do not implement DNS proof in client. |
| Route patterns | Optional scheme, prefix host wildcard, trailing path wildcard; most-specific match [R]. | host/path_prefix (`manifest/src/lib.rs:271`), validation only (`validation.rs:290`); C4 Host-only. | NEED: SDK owns grammar and golden matching behavior. | One parsed route type/matcher and deterministic specificity, including negation and query/path cases; Edge consumes vectors. |
| Worker service bindings | Declared binding invokes private target methods/fetch [B]. | Opaque Binding `{name,capability,resource}` (`actors/src/domain/configuration.rs:37`); C3 no service binding. | NEED: target HTTP credentials must not be forwarded. | Typed same-account worker service binding, env materialization and RPC/fetch via binding authority. |
| Worker actor namespace binding | env supplies namespace object [N]. | Generic actor product parameter (`manifest/src/generation.rs:116`); C3 no namespace binding. | NEED: deployment selects namespace, not actor instance. | Declared account/namespace binding materializes typed namespace.get/id helpers. |
| Cross-account references | A reference does not replace authorization [B,S]. | C2 actor_id plus bearer; guest admitted identity has account (`guest/runtime.rs:83`). | NEED: represent account explicitly without ambient-account substitution. | Reference decoding retains account; binding scope/capability required to reconstruct authorized stub. |
| Alarm target and counter | One alarm/object; at-least-once; handler receives retry metadata [A]. | RuntimeHandle only (`guest/runtime.rs:294`); timer event lacks counter (`guest.rs:107`); C5 live durable alarm. | NEED SDK surface/target; HAVE live single-alarm ownership. | Actor-scoped set/get/deleteAlarm, AlarmInfo retryCount/isRetry and handler event. |
| Alarm retry policy | Exponential delay starts at 2s, at most six retries [A]. | C5 entity says 1s and clear after six failures. | NEED Cloud correction: six retries are not six total failures. | Document exact policy and counter; no SDK retransmission loop. Product alarm delivery retries do not waive test proof rules. |
| Hibernating WebSockets | accept/get by tags, auto-response and lifecycle handlers survive hibernation [D]. | Socket events and acceptance results exist (`guest/sockets.rs:52,143`), runtime only opaque handles (`guest/runtime.rs:288`); C5 rejects upgrade. | NEED: existing event types are not a working hibernation API. | Typed accept/get/tags/attachments/auto-response and message/close/error dispatch surfaces, triple targeting. Engine/Edge implementation stays Cloud-owned. |
| Logs and API validation | IDs expose parseable strings and optional names [I]. | SDK nonempty-only; C1 live 1–128 ASCII. | NEED admission parity; DELIBERATE DIFFERENCE plain IDs. | One validator across Rust/WASM/JS and all actor entrypoints; log account/namespace/id separately. |
| Cross-stream commits | No CF cross-stream equivalent. | Existing SDK Stream transaction contract; prior #1412 leaves it unchanged. | DELIBERATE DIFFERENCE: user explicitly keeps it. | No transaction rewrite or per-object commit restriction. |

Paths in the table are relative to `rust/crates/` unless otherwise qualified.

## Implementation grouping and proof

#1412 is already merged. The one remaining PR will carry this entire parity
scope from current main, rather than attempting to append to a closed PR or
opening a PR per capability. The pair codec/vector cases stay byte-for-byte;
new triple, legacy mapping, multi-label host and route cases are appended to
the same vector source. Rust and all runtime WASM/JS entrypoints consume it.

Required negative controls: pair parser rejects every triple; triple parser
rejects pairs/unknown versions/truncation/trailing bytes; namespace substitution
and cross-account binding reconstruction fail; raw concatenation aliases fail;
unvalidated actor IDs and Host labels fail; wrong suffix/extra labels/alias Hosts
fail; late initialization cannot deliver a call; unbound references cannot invoke;
competing alarm schedule does not create a second alarm; wrong WebSocket target
and stale attachment authority fail; least-specific-first route choice fails.
Use proptest on production framing/routing/gates, and fuzz public decoders.

Area debt audit (2026-10-24): `Q:/lanes/flakes.txt` and `Q:/lanes/hangs.txt` are both empty.
No entries, skips, timeout increases or retry-based test outcomes are introduced.

## Cloud-side NEEDs for coordinator relay

1. Replace entity hash identity with plain `(account, namespace, id)` and use
   the SDK canonical triple as a serializable address, never a capability.
2. Resolve absent namespace to reserved `__default` without rewriting persisted
   IDs; keep monotonic incarnation/ownership fencing internal.
3. Deploy namespace code/config/bindings/jurisdiction explicitly and version it;
   activate on first invocation, materialize state on first write, implement
   deleteAll including alarm deletion, and remove instance CreateActor.
4. Worker deploy moves from instance create (CreateActor) to the deployment
   level, alongside namespace deploy. A Worker has no addressable instances.
5. Enforce namespace/service binding authority and materialize live typed env
   objects; add private worker RPC/fetch and worker-to-namespace invocation.
6. Preserve per-stub delivery order/broken-stub semantics, single-writer term
   fences and initialization gating across all delivery kinds.
7. Store/obey namespace jurisdiction; receive first-access locationHint as
   advisory placement input. Do not add jurisdiction as an unruled fourth field.
8. Target alarm/sockets by triple; expose retryCount/isRetry; alarm backoff starts
   at 2s and permits six retries (the supplied 1s/six-failure description differs).
9. Implement actual WebSocket upgrade/hibernation, runtime-held connection
   ownership, tags/attachments/auto-response and lifecycle dispatch; existing
   SDK socket declarations alone do not implement the engine.
10. Match SDK route grammar/specificity/negation vectors at Edge and retain custom
    domain proof checks. Parse default hosts using the exact configured suffix,
    exactly two identity labels and byte-exact hostname() re-rendering.

11. Return the namespace-scoped observation stream locator on ActorObservation (field 92). SDK consumers must not hash account/id to reconstruct an internal stream path; that old derivation omits namespace and aliases separate objects. Existing persisted stream IDs remain unchanged.

## Primary Cloudflare sources

- [N: namespace](https://developers.cloudflare.com/durable-objects/api/namespace/)
- [I: ID](https://developers.cloudflare.com/durable-objects/api/id/)
- [S: stub](https://developers.cloudflare.com/durable-objects/api/stub/)
- [L: data location](https://developers.cloudflare.com/durable-objects/reference/data-location/)
- [D: state](https://developers.cloudflare.com/durable-objects/api/state/)
- [A: alarms](https://developers.cloudflare.com/durable-objects/api/alarms/)
- [R: routes](https://developers.cloudflare.com/workers/configuration/routing/routes/)
- [B: service bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/service-bindings/)

## SDK implementation map (current lane)

- `rust/crates/auth/src/addressing/actor.rs`: exact validated triple, reserved default and disjoint framing. `routes.rs`: shared pattern grammar/matcher and order-independent selection; only ambiguity among winning matches is an error.
- `rust/crates/actors/src/namespace.rs` and `typescript/packages/actors/src/namespace.ts`: local IDs/get, scoped reference reconstruction, bound ordered stubs and terminal failure/cancellation.
- `rust/crates/actors/src/domain/mutations.rs`, `invocation.rs`, `configuration.rs`: versioned namespace/Worker deployment envelope, additive namespace/account targeting, typed binding declarations. Existing instance CreateActor/UpdateActor/DeleteActor operations are removed.
- `rust/crates/actors/src/guest/durable.rs`, `activation.rs`, and `typescript/packages/actors/src/durable.ts`: constructor/exclusive gates, alarm metadata, scoped hibernation adapters and lifecycle delivery. Rust checkpoints quiesce active handlers without serializing handler execution across awaits. The Cloud engine must wire the adapters and enforce ownership.
- `rust/crates/workers/src/service.rs`, `bindings.rs` and TypeScript counterparts: typed env materialization and private service invocation through holder binding authority.
- `rust/crates/manifest/src/validation.rs`: namespace configuration and canonical Worker route declarations. Custom domains and route patterns require Worker targets; actors are invocation-only.
- `rust/crates/auth/tests/fixtures/resource-identity-v1.json`: one additive vector source, including multi-label host parsing, actor triples/default mapping and Worker route matching/selection. Existing pair rows are unchanged.

The old Cloud Workers fixture is retained as a content-publication source record;
its deployment normalization is explicitly labeled a proposed SDK contract, not
a live Cloud acceptance record. Placement, activation, alarms, WebSocket engine
behavior, private binding admission and Edge routing require the Cloud-side work
listed above. No local compile or live credentialed Cloud run qualifies these.

The shared deployment envelope admits exactly one limits variant. Namespaces carry positive handler timeout, memory and checkpoint ceilings; Workers carry positive CPU, memory and request-duration ceilings. Both/neither and operation-kind mismatches are rejected before credentials or transport I/O. Manifest Worker lowering preserves all three Worker limits.

Admitted caller audiences and host activation metadata carry namespace separately from existing local IDs. Missing legacy namespace resolves to `__default`; an explicitly empty namespace is invalid. Activation admission compares the complete triple, including account, before constructor or handler delivery. Cloud must supply the scoped runtime account/namespace and scope its admission to the same triple.
