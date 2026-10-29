Catalog authentication may name a host-protected credential:

```json
{"authentication":{"bearer":{"secret_reference":"secrets:/models/primary"}}}
```

Use `api_key_secret` instead of `bearer` for the adapter's API-key header.
The remaining `ProviderSpec` fields still select the ordinary endpoint dialect,
model capacities and verified token counter. A reference never falls back to
native provider keys or a CLI login.

Native hosts inject `CredentialResolver` through
`Registry::with_credential_resolver`. Browser and Worker hosts call
`ModelRegistry.bindCredentials(resolver)` before resolving models. The resolver
receives the original endpoint origin, canonical reference and header scheme.
It selects existing deployment authority without IO, returning an authenticated
32-byte account scope and a protected fetch capability. Rotation within that
account preserves scope; a different account must have a different scope.
The fetch function is called with its binding as `this`.

The host checks authority, expiry, revocation and origin on every physical
exchange, including counting and retries, then injects the current credential
outside guest memory. It must retain the original abort signal, refuse redirects
and keep the response body lazy. Closing the binding owner must reject pending
fetches and body reads. Errors carry the numeric `acyclic.harness.v2.ErrorCode`;
unrecognized failures become `Unavailable`. Private diagnostics are discarded.
The resolver must never return a raw secret value.

Existing native API-key selection uses the same Rust binding boundary and keeps
its catalog-generation observations and funded header producer. The default
native path adds no URL parsing, network request or credential lookup. A
protected binding adds one local authorization/injection per physical exchange,
zero network control requests and zero guest secret copies. Counting, retries,
deadlines and cancellation retain their shared implementations.

Validate the native exchange controls with:

```sh
cargo test --locked -p acyclic-harness-providers --lib authorization::tests
```

Run the WASM controls with `wasm-bindgen-test-runner` installed:

```sh
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
  cargo test --locked -p acyclic-harness-providers \
  --target wasm32-unknown-unknown --features browser --lib
```

The controls use the captured llama.cpp response bodies through real HTTP. They
check request-time rotation for generation, counting and retries, refusal before
outbound IO, account separation and cancellation when the owner disappears.
The WASM binding controls additionally cover malformed scopes, bound receivers,
typed fetch/body producer loss and diagnostic redaction. These checks qualify
the SDK boundary; each deployed host must supply and validate its protected
credential capability.
