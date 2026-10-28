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
