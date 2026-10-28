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

Keep the same root intent when reopening storage. An uncertain root publication
retains and resubmits its original prepared commit. The endpoint owns its runtime:
dropping the endpoint aborts execution, and a failed runtime ends serving instead
of accepting work without a producer. TLS is required away from loopback.

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
It also drops the current authorization decision's producer during an in-flight
command and the owning server during that wait. These waits must end with typed
`Unavailable`; a closed listener retains its original typed connection error.
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
when its platform owner is disposed. Failed delivery closes the same owner
lifetime, terminating active sessions and commands with typed `Unavailable`.
The shared Rust session engine
owns credentials, admission, flow control, calls and follows. There is no second
agent protocol or background writer. Stream follows are available; filesystem
resource follows require a platform resource provider.

This byte boundary alone is not a deployable Actor application. Public deployment
bindings must supply storage, verified admission, model and Machines I/O, and the
platform's HTTP or socket framing. The native integration check does not qualify
that deployment or its live inference provider.

## Required work

| Operation | Required work | Removed work |
| --- | --- | --- |
| Portable model dispatch | One prepared Rust request and the selected provider's HTTP exchange | Serializing the request into a JavaScript model callback, then translating its events back into Rust |
| Endpoint serving | One shared session engine and one runtime future for the admitted tree | A separate agent turn loop or command protocol |
| Waiting for an answer | One status subscription at an explicit committed cut, registered before the reply recheck | Periodic status requests |
| Known host shutdown | One terminal control frame per active channel, independent of data credit | Reconnecting to an endpoint whose runtime has already stopped |

These are source-level operation counts. The integration check measures functional
liveness; it does not establish throughput, memory or live model latency.
