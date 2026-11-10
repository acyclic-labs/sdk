# Hosted Actor conformance

This fixture runs `acyclic_actors::conformance::verify` against real remote
owners. It contains the same canonical `Package` used by local backends; there
is no second suite or in-process dispatcher in the controller.

Build `acyclic-hosted-actors-conformance` for `wasm32-unknown-unknown`, then run
the workspace's pinned wasm-bindgen tool with `--target web` on that artifact.
The exported `ActorsDomain` and `ActorsPackage` also belong to the existing
Cloudflare fixture, which now imports this shared implementation.

`ActorsDomain.open(port, pause, restart, complete)` retains the original,
host-admitted `StreamProviderPort`. Every remote owner must use that same
admitted consistency domain. A benchmark target additionally routes every
owner call through `provider()` so its request ledger covers the entire sample.
The deployed guest exports functional verification only; its live cursors use
the native Streams binding directly, without a unary Actor fetch proxy.
`created(from)` pages the
original activation stream; it does not infer placement from test expectations.

The platform supplies three real owner controls. `pause(address, paused)` must
durably change that owner's admission. `restart()` must physically replace the
owners while preserving their original storage. `complete(address)` must join
the original owner scheduler and its final registered-input recheck. A host
failure or unavailable binding rejects these controls. No control may replace
the tested owner with a local SDK Runtime.

Each owner runs `ActorsPackage.manifest()` and `onRecord` through the SDK's
existing `WasmActorHost`. This preserves the public delivery, checkpoint,
interrupt and transition contracts. The outer deployment must declare its
complete binding set and run binding-specific acceptance as well: the core
reference package does not itself use every product binding.

`guest.mjs` is the public Actor guest facade. Its controller runs the verifier;
separate named owners run `WasmActorHost` and the canonical package. Owner
addresses and discovery cursors use the native state binding, wake placement
uses the original Streams register operation, and deadlines use native Actor
alarms. Restart deliberately fails each owner and verifies a new incarnation
at the same durable address. Invocation and cursor failures remain failures.

Build it with `bun scripts/build-hosted-actors-conformance.mjs MANIFEST ENVIRONMENT
ENTRY OUTPUT`. The input is the host's complete SDK-validated manifest selection,
including a state binding, shared Streams binding and namespace binding back to
this entry. The producer retains the selected bindings, seals both WebAssembly
modules as declared assets, and verifies the resulting artifact with the SDK's
manifest validator. It writes an ordinary `harness-actor.mjs` application entry.
The host must compare its manifest selection with its pinned SDK census; this
producer never reduces that selection to the three handles its scheduler uses.
The consumer first prepares the ordinary harness and manifest wasm artifacts,
then provides those same verified generated facades in the SDK checkout. This
producer builds only the hosted-conformance crate; it does not rebuild the
harness artifact's dependency closure.
