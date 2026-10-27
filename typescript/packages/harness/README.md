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
