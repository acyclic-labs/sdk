# Filesystem services and availability

The Rust crate is the source for both the portable filesystem model and the
native service adapters. A consumer uses the crate's default declaration; the
target selects the compatible host integration automatically. The table maps
the public surfaces to their supported targets and implementation profiles.

| Capability | Rust surface | Supported target/profile |
| --- | --- | --- |
| Portable workspace model | `acyclic_fs::workspace`, `model`, `path`, and related modules | Default crate surface, including `wasm32` where target-compatible |
| Embedded local storage | `Fs::local`, `LocalFs`, `LocalOptions` | Native targets in the default profile; target dependencies are selected automatically |
| In-memory distributed test backend | `MemoryFs` | Test and embedded profiles |
| Hosted workspace client | `HostedFs`, `HostedFsOptions` | Native targets; unavailable on `wasm32` |
| Native filesystem watching | `watch` and native capture support | Native targets; selected automatically by the native profile |
| Native mounts | `native_mount` | Native targets; selected automatically by the native profile |
| S3 HTTP support | `s3_http` | Native targets; selected automatically by the native profile |
| gRPC wire service | `FilesystemWireService` and generated `wire::filesystem::v2` bindings | Native targets; included by the native build |

The public wire contract is `acyclic.filesystem.v2`. The packaged
`FILE_DESCRIPTOR_SET` is the compatibility artifact used by conformance checks;
it includes the shared `acyclic.protocol.v1` handshake. Applications should
negotiate that protocol before sending filesystem operations.

The service adapter is transport-neutral around filesystem policy. Its limits
and credential types are public (`FilesystemWireLimits`,
`FilesystemCredentialIssuer`, and `FilesystemSourceProvider`), while storage
and authorization remain owned by the application. The descriptor and hosted
client preserve the same typed contract.

For hosted access, configure `HostedFsOptions` and use the hosted types exposed
by `acyclic_fs::hosted`; for an embedded native process, start with `Fs::local`.
The [embedded example](../examples/embedded_workspace.rs) is the executable
smoke test for the local route.
