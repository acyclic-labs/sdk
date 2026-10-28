# Cloud Machines

Enable `acyclic-harness`'s `machines` feature and compose the public
`acyclic_machines::GrpcProvider` with `machines::MachinesHost`. Supply an explicit
provider identity, immutable image and network policy commitment. Keep the
account credential in the client callback; it never enters guest argv, stdin,
files or environment.

Use `GrpcProvider::connect_service(https_endpoint, credential_callback, ca)` for
the public bearer-authenticated service. `ca: None` selects public trust roots;
self-hosted services can add a private CA. Mutual TLS and owner-confined local
socket constructors remain available for those deployments.

`GrpcProvider::qualify_default_image()` obtains the service's installed immutable
image identity. Use the returned image in `CreateMachine`; explicit qualification
continues to reject substituted identities. No upstream distribution digest is
assumed to identify a provider's guest image.

`MachinesHost` creates and inspects machines, dispatches streamed exec, reads and
writes guest files, suspends and deletes machines. Native `ReadFile`/`WriteFile`
carry exact binary chunks, an absolute guest path and offset, and one absolute
deadline. Replies preserve short IO and POSIX errno. A write can be partial after
interruption and is never automatically repeated. Suspend preserves the machine and is distinct
from deletion; there is no separate public stop RPC.

For the harness `shell`, bind `MachinesWorkspace` through
`runtime::Bindings::workspace_tools` using the original verified scope and
selected volume. The host separately owns the authenticated filesystem mount
and content bindings. The standard `fs` history facade is not supplied by this
shell binding. Commands use the shared bash invocation and original input schema.

Protocol 1.2 adds native binary file IO, stdin, cwd, absolute expiry, output
allowance, stderr and typed stopped capture. The service must support that revision before dispatch. Each
stream completes at its terminal event; no inspection or reopen occurs during
silence. Dropping exec cancels that RPC. Physical command termination and reaping
belong to the service producer; only its cleanup receipt confirms completion.
Interrupted execution remains indeterminate and is never automatically retried.

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
