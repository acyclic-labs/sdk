# Agent host conformance

`acyclic_harness::conformance::verify` drives the public `AgentHost` API over a fresh backend. Enable the published crate's `conformance` feature. This compatibility crate re-exports that same implementation for existing native, browser and hosted adapters.

Downstream backends can register the suite with `acyclic_harness::conformance::suite!(MyBackend::create)`, where `create` is an async factory returning `Result<MyBackend>`. Use `suite!(backend_name, MyBackend::create)` when registering several backends in one test module. Native test crates need Tokio's test macros; browser test crates use `wasm-bindgen-test`.

A backend implements two setup traits. `Backend::fresh` returns disposable storage, and `Storage::open` opens a new session on that same retained data. `Storage::objects` provisions the Objects provider, an empty logical bucket and its declared durability for filesystem-backed root and approval vectors. Native fixtures also supply a private staging directory and retain it for the fixture lifetime. The suite composes the existing immutable cache and native publication staging. Stream-only fixtures explicitly use `Objects::memory()`. `Backend::handler` installs its actual handler transport. `Backend::serving` opens its production hosting endpoint with the requested preset approval policy and shared `protocol` model/tool fixtures. Its endpoint supplies canonical binding/exchange, explicit delivery and observations; it cannot replace a vector. The suite owns all fault injection and assertions.

The native adapter opens a real `LocalStream` directory. It runs twice: direct handlers and handlers carried through `acyclic_actors::guest::wire::Remote`. The browser adapter opens real `BrowserStream` IndexedDB databases and uses the same wire boundary.

`agent_host::fixtures` exposes the same initial adoption, sender, file-free input and deterministic turn driver to endpoint fixtures. The driver accepts ordinary client messages as well as host-side delegated requests: intent `once` publishes `once`, `spawn` forks `child` at the saved cut, and `respawn` conflicts with that child's original prompt. Provision an endpoint with this driver and capture its real binding, command, invocation and reply for the client-protocol suite; use the same intents and retained storage for exact replay. The client lane owns its `ControlFixture` and `ToolOutputFixture` schemas and endpoint runner. These shared host fixtures do not supply a public ingress, credentials or an endpoint qualification receipt.

The vectors cover:

- Creation and send replay after progress, one committed conversation effect, a status cursor covering each consecutive settled turn, changed creation prompt refusal, and archive fencing.
- Unknown publication retries with byte-for-byte equal `CommitRequest`s, one participant open and seal, no settlement even if an uncertain retry is refused, and exactly one terminal settlement. A definitive first refusal and a real conditional conflict settle without consuming input.
- A spawn's inbox prefix and conversation cut, including input queued after the spawn input and a conversation append in the spawning transaction. The child has no checkpoint until hosted. A changed-prompt respawn fails and preserves the original child.
- Local root and delegated workload scope, read-only, wrong audience, wrong agent, revoked, expired and tampered widening admissions. Both valid controls reach nonempty history. Invalid admissions perform zero **turn-provider** reads/writes and invoke no executor. Scheduler reads are separate: the actor runtime must load the committed checkpoint to discover which turn to admit.
- OIDC signature forgery, expiry, issuer/subject/audience mismatch, symmetric/unsigned algorithm refusal, token-supplied key sources, multiple audiences, key rotation/removal, bounded refresh and last-good expiry. Valid identities attenuate an existing agent-bound parent through ordinary `TurnAdmission`; refusals perform zero turn I/O and cached admission performs no JWKS fetch. `scripts/be-edge-oidc-verify.sh` additionally runs the same suite with a disposable real Dex issuer.
- Admitted root preparation over one placement: a covering admission prepares the same request twice and creates exactly that root, and its exact resubmission replays. Read-only fresh private or shared volumes, wrong audience, wrong agent, expired and revoked admissions are refused with zero provider reads/writes.
- Strict manifest, delivery and transition codec round trips and malformed-input refusal. A timed-out handler drops its in-flight guest turn and leaves its input unconsumed.
- Runtime interruption of an in-flight turn by a later Cancel, preserving intervening input and keeping the next turn's token independent (G41). Checkpointed activation discovery resumes its saved cursor after reopening without explicit tracking, keeps the opening append's key, and retries the exact cursor publication after lost acknowledgement (G40).
- The same hosted protocol script on every backend: exact Prompt and Decide replay, preset approvals and wrong-binding refusal, correlated committed Reply replay and wrong-prompt conflict, immutable output, exact Spawn replay and saved cut, changed-prompt conflict, lazy children and typed outside-tree refusal. The DO entry invokes its actual named host and packaged guest, rather than substituting an in-process turn host. `protocol::Endpoint` and the fixture functions are public for client-protocol consumers.
- A deterministic program interrupted immediately before and after every provider API call, including commit acceptance before a lost acknowledgement. Both the host and provider are dropped, storage is reopened, the caller's intents are replayed, and the final state and single conversation effect must match the uninterrupted result. This is an API-boundary crash model; physical torn writes belong to each storage provider's suite.

Run from the repository root on the build box:

```sh
bash scripts/qualify-agent-host.sh
```

`scripts/backends/storage-conformance.sh` additionally runs the same complete
host export over Postgres + memory Objects, local Stream + MinIO, and Postgres +
MinIO. Each case retains its storage across independently opened sessions and
uses the canonical guest wire boundary. The driver binds disposable emulators
to loopback ports 15400 and 15401 and stops both when the run ends. These host
vectors exercise storage composition; they do not establish an OSS endpoint or
real GraphCoder binary qualification.

For G46/G47, `acyclic_harness::conformance::bench(&backend)` sends and settles
64 inputs after warmup, checks every conversation effect, and reports elapsed
time plus data and control request counts. The shared `scale(&backend, Scale)`
vector runs that workload at two independently provisioned idle populations. It
uses the global scheduler drain and rejects more than a factor of two growth in
either request class. A zero control baseline must remain zero. This measures
dispatch work and real runtime registrations. Each settled turn must retain
exactly one registration per provisioned agent and leave none ready after the
global drain. It does not measure physical memory or allocations inside a
provider. Reported control traffic remains visible even if the scale bound passes.

Run the native 1,000 versus 10,000 idle-agent vector explicitly on bb2 or ivar:

```sh
build-slot --remote cargo test --locked -p acyclic-agent-host-conformance --features local --test local agent_scale -- --ignored --nocapture
```

No CI workflow or merge-gate step invokes this workload. Downstream adapters can
call the same public `bench` and `scale` functions with their own `Backend`.

To execute browser tests after compiling them, use the workspace's pinned `wasm-bindgen-test-runner` and Chrome driver:

```sh
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
CHROMEDRIVER=/path/to/chromedriver \
cargo test -p acyclic-agent-host-conformance --features browser --target wasm32-unknown-unknown
```

These wire-handler vectors do not by themselves qualify the generated JavaScript `AgentGuest.open` facade, its `AbortSignal` bridge, or a production model. The harness's `browser-agent-guest.html` artifact test covers that facade separately. Durable Objects must run these same vectors and the unchanged Stream suite inside workerd/Miniflare before claiming conformance.
