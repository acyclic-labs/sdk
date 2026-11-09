# @acyclic-labs/auth

Shared capability contracts, credentials, resource identities, guest admission,
and the protected-secrets client for the Acyclic SDK.

The package exports `CredentialProvider`, `CredentialSnapshot`, and
`CredentialCache` for source-owned credential resolution and validity. Credential
watchers report source changes through an `AbortSignal`; credential material
belongs only to the admitted request and must not be logged or persisted.

`ActorReference` and `ResourceIdentity` provide shared addressing. Wire schemas
are available from `@acyclic-labs/auth/proto`; the public TypeScript contracts and
helpers are exported from `@acyclic-labs/auth`.

`SecretsClient` uses the shared Rust implementation for put, paged list, and
delete operations. Supply the endpoint, credential provider, and the original
operation context. Generation and version fields are `bigint`. Protected values
are never returned by listing; generation conflicts require an explicit new
operation. The client does not retry an overwrite automatically. See the
[shared Auth contract](../../../rust/crates/auth/README.md) for the wire contract
and conformance requirements.

`materializeGuestAdmission` selects an existing binding from the authenticated
invocation's `GuestAdmissionHost`. The host remains outside guest code; request
data and deployment descriptors cannot supply admission.

Run `bun run build` in this package to generate its public artifacts, rebuild the
Rust/WASM bridge, and compile its TypeScript declarations.
