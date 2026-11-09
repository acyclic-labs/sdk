# Machines adapter

The `machines` feature bridges sandbox and checkpoint references to a
`MachinesProvider`. `MachinesHost` captures selected forkable checkpoints and
verifies their retained provider identity before admission.

`AgentMachines::binding` and the research placement export share
`MachineBinding::decode(target, record)`. Supply the original authorized record
from `AgentPath::machine(target)` at `from=1, limit=1`; position zero is the
provisioning plan. The decoder checks that original settled position, format
version and target once. It neither authenticates a supplied record nor derives
its account or agent from the payload. Forks retain their original record origin,
sequence and commit. The binding proves the original opaque provider and Machine
reference; physical host, region and resource measurements remain unknown.

`MachinesHost::exec` selects declared binary duplex or textual request/response
execution. Duplex retains separate stdout/stderr bytes; textual execution retains
combined UTF-8 output and its terminal receipt. `read_file` and `write_file` use binary file primitives
with an absolute guest path, exact offset and original deadline. `suspend` and
`destroy` retain the caller's original idempotency key on uncertain outcomes.

Bind `MachinesWorkspace` as the home target of `runtime::Bindings::targets`
(`runtime::InProcessTargets::home`) for the standard `shell` tool. Construct it from the selected machine, volume, original signed
parent scope and its `AuthorityVerifier`. It checks both parent and task grants,
uses the shared Linux bash invocation, and sends the earlier of authenticated
parent expiry and task deadline. The standard input schema and 300 second
default remain shared with native execution. Interrupted commands retain the
original tool operation as indeterminate and are never submitted again.

The host binds the selected guest workspace through its existing filesystem
admission with `FilesystemHost::bind_machine_workspace`. When `AttachVolume` is
Unsupported, the shared workspace layer copies the admitted logical volume into
a private guest directory at bind time. At turn settlement it captures file
changes and publishes through the existing workspace transaction facade under
the original turn identity, rechecking volume access before publication. Native
attachment keeps the admitted physical selection. Immutable file tools keep their
authenticated `ContentBindings` and observe published generations. Native ledger
execution is explicitly unsupported.

JavaScript hosts initialize `@acyclic-labs/harness/browser`, then call
`await BrowserMachinesWorkspace.open(originalAuthority, originalProviders, source)` and pass the result to
`BrowserAgentRegistry.bindMachinesWorkspace`. Use the same initialized module and
authority for the registry, guest and server. `source` is the platform's admitted
`machines:/` binding: canonical ProviderRef and original settled MachineState
protobuf bytes, `createMachinesRpcTransport` over the authenticated duplex
transport, and the original invocation's clock-budget projection. The selected
physical volume comes from that observation. A provider declaring attachment
Unsupported instead receives `source.selectedVolume`, the canonical protobuf of
the original admitted logical VolumeRef. Scope and verification come from the
retained authority; the providers must open that exact volume. Construction
materializes a copied workspace when needed and rechecks the original authority
after asynchronous work. Decoding validates the admitted Machine evidence.

The budget projection registers the earlier of the original caller deadline,
command-relative timeout and authenticated expiry once, before dispatch. Its
synchronous remaining observation returns zero on cancellation or producer loss;
its stop promise wakes pending work. Releasing the Rust observation aborts only
that registration. This port requires the platform's original clock and stop
owner; neither a guest timer nor a fresh deadline per response frame implements
it. The opener additionally retains the authority's original Auth Context, so
original stop and same-turn revocation take precedence over a ready result.
