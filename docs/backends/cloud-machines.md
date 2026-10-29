# Cloud Machines

Enable `acyclic-harness`'s `machines` feature and compose the public
`acyclic_machines::GrpcProvider` with `machines::MachinesHost`. Supply an explicit
provider identity, hosting preset's optional immutable image and network policy commitment. Keep the
account credential in the client callback; it never enters guest argv, stdin,
files or environment.

Use `GrpcProvider::connect_service(https_endpoint, credential_callback, ca)` for
the public bearer-authenticated service. `ca: None` selects public trust roots;
self-hosted services can add a private CA. Mutual TLS and owner-confined local
socket constructors remain available for those deployments.

JavaScript hosts can import `createMachinesClient` from
`@acyclic-labs/machines/transport` with their authenticated Connect transport.
It must support bidirectional streaming, input backpressure and `AbortSignal`.
The Node/Bun `@acyclic-labs/machines/grpc` entry point uses this same client core
and owns its HTTP/2 connection; the portable entry point imports no Node APIs.

`GrpcProvider::qualify_default_image()` obtains the service's installed immutable
image identity when discovery is needed. `CreateMachine::new(key, None, network_policy)`
sends no image: the server selects its installed default and returns its immutable
identity in the machine contract. An explicit image must match that returned identity.
`MachinesHost::create` accepts the preset's optional artifact directly. No image or
distribution is hardcoded in the SDK. The simulator has no installed default and
refuses omitted-image creation before changing its state.

`MachinesHost` creates and inspects machines, dispatches streamed exec, reads and
writes guest files, suspends and deletes machines. Native `ReadFile`/`WriteFile`
carry exact binary chunks, an absolute guest path and offset, and one absolute
deadline. Both canonical RPCs are bidirectional chunk streams: one native receipt
per requested chunk, with the first path/machine/deadline binding the session.
Generated service clients can retain a stream across chunks; the harness's scalar
file primitive sends one chunk and completes on its receipt without waiting for EOF.
Replies preserve short IO and POSIX errno. A write can be partial after
interruption and is never automatically repeated. Suspend preserves the machine and is distinct
from deletion; there is no separate public stop RPC.

For the harness `shell`, bind `MachinesWorkspace` through
`runtime::Bindings::workspace_tools` using the original verified scope and
selected volume. The host separately owns the authenticated filesystem mount
and content bindings. The standard `fs` history facade is not supplied by this
shell binding. Commands use the shared bash invocation and original input schema.

Protocol 1.2 adds native binary file IO, stdin, cwd, absolute expiry, output
allowance, tagged `Data { stream: STDOUT | STDERR, bytes }` and typed stopped capture. The service must support that revision before dispatch. Each
stream completes at its terminal event; no inspection or reopen occurs during
silence. Dropping exec cancels that RPC. Physical command termination and reaping
belong to the service producer; only its cleanup receipt confirms completion.
Interrupted execution remains indeterminate and is never automatically retried.
The TypeScript client shares cancellation ownership for exec and operation
watches. Returning an iterator interrupts a pending pull, and terminal events
complete without waiting for transport EOF. Watches have no implicit timeout;
an explicit caller deadline or cancellation still applies.

Exec uses one bidirectional RPC: its first `ExecStart` fixes admission, followed
by argument byte chunks with explicit argument boundaries. `command_complete`
alone permits execution; incomplete input cannot launch a process. Separate
stdin chunks and `stdin_eof` preserve the shared `bash -c SOURCE` behavior.
Chunks are at most 64 KiB; arguments and command input have no aggregate SDK
limit. Native OS exec reports actual capacity errors. The native producer must
propagate consumed-input credit through the original stream; the clients keep
no background input queue and never replay interrupted input.

Run the shared lifecycle and protocol checks with:

```sh
cargo test --locked -p acyclic-machines --features conformance --lib
cargo test --locked -p acyclic-harness-core --features machines --lib --no-run
```

The protocol tests use internal HTTP/2 service vectors, not captured provider
responses. They cover channel bytes, stdin/cwd, exit and stop, authority expiry,
drop cancellation, lost outcomes and quiet lifecycle completion. The simulator
lifecycle result does not qualify Cloud execution. Cloud service conformance and
physical cleanup remain unverified until the service supplies its matching
revision and a real disposable provider factory for the unchanged suite.
