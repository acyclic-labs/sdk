# @acyclic-labs/harness

The public frontend uses the Rust agent client and one authenticated Streams
session for local, self-hosted Actors and managed Swarm. The TypeScript package
exports the generated client rather than maintaining a second transport or agent
runtime.

Call `initializeClientQueries()` before constructing client objects. `AgentClient`
folds the selected conversation, status and filesystem streams through one bounded
`WireSession`. `PageWindow` retains complete provider pages and their opaque
cursors. Expand child nodes with bounded child pages; select full streams only for
agents the interface needs.

`AgentClient.controls(binding)` binds controls to that client's existing session
pool and credential slot. Prepare a command once and retain its
`PreparedAgentInvocation` through the exchange. A typed uncertain result retains
the exact original request; a transport error does not authorize a fresh intent.
Use the exported Rust `encodeCanonicalJson` to construct command bytes; it accepts
`BigInt` for exact counters and rejects unsafe integer Numbers. Command schema and
byte-limit admission remain in `PreparedAgentCommand`.
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
the public host bootstrap, root binding, shared preset and immutable file reader.
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

The `./browser` host module also exposes `BrowserAgentAuthority`,
`BrowserAggregate` and `BrowserHistoryReader` directly from the retained Rust
bindings. History options use the Rust fields `database`, `authority`, `issuer_id`,
`issuer_key`, `maximum_commands`, `maximum_journal_bytes`, and optional `memory`.
`BrowserAggregate.openBrowser(options, schemas, snapshot)` and
`BrowserHistoryReader.openBrowser(options)` use the same owning IndexedDB stream.
These are explicit host composition primitives; they do not bootstrap an endpoint
credential or replace the public agent session. Archival cursors pin their cut,
and page calls require finite event and byte bounds.
