# @acyclic-labs/harness

The public frontend uses the Rust agent client and one authenticated Streams
session for local, self-hosted Actors and managed Swarm. The TypeScript package
exports the generated client rather than maintaining a second transport or agent
runtime.

Connection configuration uses one Rust policy for native and TS callers:
explicit endpoint, then `ACYCLIC_ENDPOINT`, then
`https://swarm.cloud.acyclic.dev`. The credential is explicit or comes from
`ACYCLIC_API_KEY`. There is no config-file lookup or automatic local fallback.
Invalid endpoints (including HTTP), missing credentials and invalid credentials
have typed error codes before a transport is constructed.

Native callers use `acyclic_harness::client::ConnectionOptions::from_env`.
TS callers first call `initializeClientConnection()`, then the synchronous,
I/O-free `resolveConnectionOptions(endpoint?, credential?, environment?)`.
Initialization loads the SDK's WASM asset; resolution itself does not connect to
the selected endpoint. Browsers pass a credential explicitly or supply the
environment object. These resolved options are configuration, not a session
capability or proof that a server is reachable.

Preset data and defaults also have one Rust definition. Native callers use
`acyclic_harness::client::Preset::graphcoder()` (the original
`acyclic_harness::preset::Preset` path is the same type). TS callers use
`initializeClientPreset()` followed by `graphcoderPreset()`. Its generated
`Preset` and approval-policy types come from those same Rust definitions. For
example, `const preset = graphcoderPreset(); preset.model = "provider/model";`
changes one default without a second configuration builder. All seven fields
remain ordinary configuration data. Producing this value creates no session or
agent and does not apply configuration to a remote host.

Call `initializeClientQueries()` before constructing client objects. `AgentClient`
folds the selected conversation, status and filesystem streams through one bounded
`WireSession`. `PageWindow` retains complete provider pages and their opaque
cursors. Expand child nodes with bounded child pages; select full streams only for
agents the interface needs.

For a scrolling UI, use `client.setViewport(agents, conversationCursors,
statusCursors, outputSubscriptions)`: the first agent is focused, the rest are
visible, and output subscriptions are encoded `SessionSubscription` values for
the visible tool panes. The Rust core replaces the complete viewport atomically;
omitted agents and panes release their follows and cached records. Pass `[]` for
output subscriptions when the tool pane is hidden. Overlapping interests retain
their cursors. The combined cap is 16 interests (two per agent, one per pane);
over-capacity input leaves the previous viewport intact. `setView` remains the
agent-only operation for callers managing output lifetimes separately.

Keep child and historical pages in `PageWindow`, using the server's exclusive
cursors for backfill; read individual displayed records rather than copying a
whole conversation. Closing the client releases all interests. No idle agent
needs its own client, timer, or subscription.

`AgentClient.controls(binding)` binds controls to that client's existing session
pool and credential slot. Prepare a command once and retain its
`PreparedAgentInvocation` through the exchange. A typed uncertain result retains
the exact original request; a transport error does not authorize a fresh intent.
Use the exported Rust `encodeCanonicalJson` to construct command bytes; it accepts
`BigInt` for exact counters and rejects unsafe integer Numbers. Command schema and
byte-limit admission remain in `PreparedAgentCommand`.
`prepareAgentInvocation(controls, command)` composes these Rust boundaries for a
typed `AgentCommand` and releases the temporary command after preparation. It
returns the caller-owned invocation without executing it. Keep that invocation
through `execute()` errors; the helper opens no transport and retries nothing.
Command delivery acknowledges inbox admission, while conversation/status records
establish settlement. Approval answers use the observed immutable approval binding.

The `./browser` entry exports the Rust browser host, authority and model registry.
Call `initializeBrowserRuntime()` before host composition. `configurePiModel` in
`@acyclic-labs/pi` registers a provider at that host callback boundary: Rust supplies
the admitted request bytes and verified file slices. Frontends should not rebuild
file authorization, request projection or execution in JavaScript.

Custom model providers can use the exported `ModelRequest`, `ModelContextCapacity`,
`ModelTokenCount` and compaction policy types directly at that callback boundary.
After `initializeModelProviders()`, `defaultCompactionPolicy`,
`validateThresholdCompaction` and `validateModelTokenCount` use Rust's existing
policy and exact-request accounting checks, including the selected finite output
ceiling. These helpers grant no file or model-option authority.

For declarative model contracts, call `initializeModelContent()`. The exported
`validateModelContent`, `validateModelMessages`, `validateUserInput`,
`prepareModelRequest`, `modelContentInventory` and `admitModelEvent` run the
existing Rust validators on that same WASM instance. Supply finite
`ModelContentLimits`; start event admission with `null`, then pass the returned
state to the next call. Inventory describes immutable references and original
option claims. It grants no file access or authority to admit those claims.

`./react`, `./svelte` and `./worker` expose view adapters. Framework subscriptions
and demand hydration use the same bounded client state. File hydration requires
an authorized, verified host reader; a filesystem follow record is metadata, not
the file's contents.

The one-credential connect/config facade and local-default example still require
the public host bootstrap, root binding, preset application and immutable file reader.
These are not supplied by the low-level session welcome or root-discovery command.
The legacy task workflow, object facade and Harness protobuf subpaths are retired.

The endpoint conformance runner is `bun examples/client-conformance.ts`. Supply
`ACYCLIC_CONFORMANCE_ENDPOINT`, `ACYCLIC_CONFORMANCE_TOKEN` and bounded hex
`ACYCLIC_CONFORMANCE_SESSION_CREDENTIAL`, plus exactly one host-created fixture:
`ACYCLIC_CONFORMANCE_CONTROL_FIXTURE`, `ACYCLIC_CONFORMANCE_TOOL_FIXTURE`, or
`ACYCLIC_CONFORMANCE_AGENT_FIXTURE`. Agent fixtures also require the same
`ACYCLIC_CONFORMANCE_NAMESPACE` as the native runner. Both runners consume the
same fixture bytes, provisioned through the endpoint's ordinary AgentHost.

The public `verifyClientObservation` checks exact conversation/status records,
retained selection, atomic interest-capacity refusal, unrequested authority and
cursor-preserving reselection through the shipping WASM client. The native suite
also validates provider tails before observation; the TS observation entry requires
the supplied settled-cut fixture and does not claim that extra preflight. Controls
and tool output use `verifyClientControls` and `verifyClientToolOutput`. Reports
name only completed cases. These observation cases establish neither turn
execution nor credit/scale/bootstrap or real endpoint qualification by themselves.
