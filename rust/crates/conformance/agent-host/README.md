# Agent host conformance

`acyclic_conformance::agent_host::verify` drives the public `AgentHost` API over a fresh backend. The implementation lives in a small independent crate so browser wasm does not pull the native qualification CLI and its process dependencies into the build.

A backend implements two setup traits. `Backend::fresh` returns disposable storage, and `Storage::open` opens a new session on that same retained data. `Backend::handler` installs its actual handler transport. `Backend::serving` opens its production hosting endpoint with the requested preset approval policy and the shared `protocol` model/tool fixtures. Its endpoint supplies canonical binding/exchange, explicit delivery and observations; it cannot replace a vector. The suite owns all fault injection and assertions.

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

To execute browser tests after compiling them, use the workspace's pinned `wasm-bindgen-test-runner` and Chrome driver:

```sh
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
CHROMEDRIVER=/path/to/chromedriver \
cargo test -p acyclic-agent-host-conformance --features browser --target wasm32-unknown-unknown
```

These wire-handler vectors do not by themselves qualify the generated JavaScript `AgentGuest.open` facade, its `AbortSignal` bridge, or a production model. The harness's `browser-agent-guest.html` artifact test covers that facade separately. Durable Objects must run these same vectors and the unchanged Stream suite inside workerd/Miniflare before claiming conformance.
