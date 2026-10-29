# Machines adapter

The `machines` feature bridges sandbox and checkpoint references to a
`MachinesProvider`. `MachinesHost` captures selected forkable checkpoints and
verifies their retained provider identity before admission.

`MachinesHost::exec` returns the provider's original stdout/stderr and terminal
event stream. `read_file` and `write_file` use the native binary file primitives
with an absolute guest path, exact offset and original deadline. `suspend` and
`destroy` retain the caller's original idempotency key on uncertain outcomes.

Bind `MachinesWorkspace` as `runtime::Bindings::workspace_tools` for the standard
`shell` tool. Construct it from the selected machine, volume, original signed
parent scope and its `AuthorityVerifier`. It checks both parent and task grants,
uses the shared Linux bash invocation, and sends the earlier of authenticated
parent expiry and task deadline. The standard input schema and 300 second
default remain shared with native execution. Interrupted commands retain the
original tool operation as indeterminate and are never submitted again.

The host must bind the selected guest workspace to that volume using its existing
filesystem admission. Immutable file tools keep their authenticated
`ContentBindings`; this shell adapter does not supply the `fs` history facade or
claim a filesystem snapshot. Native ledger execution is explicitly unsupported.

JavaScript hosts initialize `@acyclic-labs/harness/browser`, then call
`BrowserMachinesWorkspace.open(originalAuthority, source)` and pass the result to
`BrowserAgentRegistry.bindMachinesWorkspace`. Use the same initialized module and
authority for the registry, guest and server. `source` is the platform's admitted
`machines:/` binding: canonical ProviderRef and original settled MachineState
protobuf bytes, `createMachinesRpcTransport` over the authenticated duplex
transport, and the original invocation's clock-budget projection. The selected
volume comes from that observation; scope and verification come from the retained
authority. Construction performs no RPC, creates no Machine and reads no current
state. Decoding is validation of evidence, not admission of a replacement Machine.

The budget projection registers the earlier of the original caller deadline,
command-relative timeout and authenticated expiry once, before dispatch. Its
synchronous remaining observation returns zero on cancellation or producer loss;
its stop promise wakes pending work. Releasing the Rust observation aborts only
that registration. This port requires the platform's original clock and stop
owner; neither a guest timer nor a fresh deadline per response frame implements
it. The opener additionally retains the authority's original Auth Context, so
original stop and same-turn revocation take precedence over a ready result.
