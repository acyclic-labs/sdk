# @acyclic-labs/workers

Workers v1 generated public contract and authenticated HTTP client. Publish
exact JavaScript module bytes, select a deployment alias by expected revision,
and submit durable jobs. Accepted jobs pin the resolved code digest even if the
alias later changes. Invocation has ordinary HTTP ambiguity and does not
acknowledge a durable job. The live Cloud executor and public route are not yet
qualified against this contract.

An ES module may implement `default.fetch(Request)` for HTTP, `default.run(bytes,
context)` for durable jobs, or both. The package exports Rust-generated
`WorkerModule` and `WorkerJobContext` types. Retried jobs receive the same job ID
and input with a higher attempt number; external effects need application
idempotency.

Object inputs name an S3 bucket and key. The service privately retains the
accepted bytes for retries. Job results contain exact bytes bounded by the
accepted output budget (at most 1 MiB), without public Object version references.

The Rust crate `acyclic-workers` and `proto/workers/v1/workers.proto` own the
contract. `WorkersClient` uses the canonical Rust binding; import generated
request schemas from the package or `@acyclic-labs/workers/proto`. Configure
`endpoint` and either a static `token` or a `credentialProvider`; optional
`caCertificate` adds a private PEM CA and `maximumMessageBytes` bounds requests
and responses. A provider returns an opaque own-leaf bearer and its actual
`expiresAtUnixMillis: bigint`, propagating the supplied abort signal. Refresh
shares the existing client/channel; permission and revocation errors are not
masked or retried, and accepted mutations are not redispatched. Reconnecting
watches must retain their original durable cursor and command/fence.

Workerd and Cloudflare Pages must import the compiled WASM asset and initialize
the binding before creating clients:

```ts
import compiledModule from "@acyclic-labs/workers/wasm/module.wasm";
import { initializeWorkersWasm, WorkersClient } from "@acyclic-labs/workers";

await initializeWorkersWasm(compiledModule);
const client = new WorkersClient({ endpoint, token, transport: "wasm" });
```

Configure the deployment bundler to load `*.wasm` as compiled WebAssembly modules.
This path does not read Node files or compile WASM bytes at runtime. Native-only
loaders remain lazy and are not initialized on the WASM path.

Omit `expectedRevision` only to create an absent alias; replacing an existing
selection requires its positive current revision.

## Customer-held credentials

The Rust `acyclic-native-runtime::account` codec owns canonical bearer encoding
and expiry. `inspectAccountHolder` inspects only the caller's own public
birth/certificate tuple (including an expired certificate for renewal); it
does not authorize permissions. Root signature, current-keyring, account and
permission verification remain server responsibilities. `prepareAccountBearer`
returns Rust-owned signing bytes; finishing checks the actual signature and
finish-time window. JavaScript never parses or re-encodes this account wire.

`generateNativeCustomerLeaf` and `openNativeCustomerCredential` use the actual
native addon and OS vault. A device login supplies a transient distinct SQL
session to `commit`; the restored handle has no private-key/session getter or
serializer. `requestIdentity` attaches that sealed session inside Rust only to
the configured HTTPS origin's `/v1/identity/` routes, without redirects or
permission retries. `renew` retains the same own leaf/session and returns a
generation-bound handle. Lost-ACK retries retain an existing matching pair's
actual generation; same-namespace renewal does not replace the pair. Different
pairs, corruption and superseded originals fail closed. Persist the new public
tuple and its Rust-encoded nonsecret `custodyReference` before explicitly
deleting the old namespace. Persist each retired handle's original reference
too; pass it to `openNativeCustomerCredential` after restart so cleanup cannot
adopt or erase a newer OS generation. `close` only releases the local handle;
local deletion retries accept actual absence, not a newer pair, and do not
revoke previously emitted bearers or already dispatched requests.

Browser custody requires a secure origin with real WebCrypto Ed25519/AES-GCM
and IndexedDB CryptoKey structured cloning; it is not a Workerd storage path.
Both keys are nonextractable and SQL-session bytes are encrypted in IndexedDB.
This prevents key export, not signing by malicious same-origin code: protect
the application origin against XSS. Account namespaces and persisted revision
fences prevent stale handles overwriting a newer login. Issuance/renewal callbacks
must call the real identity service; local custody and conformance fixtures do
not establish live service availability. Browser-local deletion is not server
revocation.
