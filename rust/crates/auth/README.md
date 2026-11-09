# Shared auth and protected secrets

`acyclic-auth` owns the capability contract, signature verification and authority
intersection. Enable `client` for the native HTTP secrets adapter. The browser
adapter and CLI call the same `secrets::Client`; transports do not implement
permission matching, CAS handling, response interpretation or retries.

Secrets management uses only these POST routes:

- `/v1/identity/secrets/put`: `{scope,name,kind,value,expectedGeneration}`
- `/v1/identity/secrets/list`: `{scope,cursor,pageSize}`; `cursor` is `null` or `{generation,after}`
- `/v1/identity/secrets/delete`: `{scope,name,expectedGeneration}`

Put and delete return `{generation,name,version,kind}`. Delete's kind is
`deleted`. The version is the scope generation of the write that produced it and is never
reused, so a version names exactly one value. List returns one page,
`{generation,variables,nextCursor}`, in ascending byte order of name: plain rows have
`{kind:"plain",name,version,value}`, protected rows have
`{kind:"protected",name,version}`. A protected value in any reply is rejected.
`nextCursor` is `null` when the listing is complete. Pass it back unchanged to
read the next page. Pages are fenced by generation. If the scope changed since
the cursor's generation, list fails `listing_changed` and the caller restarts with
a null cursor; a listing is never silently mixed across generations. `pageSize`
is a chunking bound for one call, with zero meaning the provider's choice. Every
row is reachable, and each call costs O(page).
There is no get, reveal or export route. Strict requests reject account selectors;
the authenticated issuer chain supplies the owner.

Authorization uses the shared typed resource grant: the catalog actions
`secrets/put`, `secrets/list` and `secrets/delete` on the percent-encoded
resources `secrets:/<scope>/<name>` (put and delete) and `secrets:/<scope>`
(list). `secrets::put_target`, `list_target` and `delete_target` name the
`(action, resource)` a provider checks. A prefix grant on `secrets:/<scope>`
covers every name in the scope. There is no read action. Grants on other
products authorize no secrets operation. Ancestor restrictions, attenuation and
owner policy intersect exactly as for every other resource.

Native callers construct `secrets::Client` with `HttpTransport::new(origin)` and
an existing `credential::CredentialProvider`, plus `Some(context)` carrying the
original invocation's stop event and clock. Missing context refuses before
credential resolution or network access. Credentials resolve on every call.
Only HTTPS origins and loopback HTTP origins are admitted. Redirects are
refused. A generation conflict requires a new list and an explicitly chosen new
mutation; the client never retries an overwrite automatically.

`@acyclic-labs/auth` exports `SecretsClient` over this Rust implementation. Its
request/reply API uses generated protobuf shapes; generation/version fields are
`bigint`. The HTTP JSON contract is generated from the same Rust declarations.
Native semantic and wire Debug output and every client error are redacted.
Plain list values are deliberate results, so applications must not log results
indiscriminately.

The portable `secrets::conformance::verify` accepts two clients and a fresh,
disposable scope. The first client needs all three secrets permissions; the
second must have product permissions and no secrets permissions. It checks CAS,
plain/protected visibility, one-row paging, the generation fence across a
mid-listing mutation, independent denial of every operation, and deletion.
After both delete receipts it lists the scope again, requiring no remaining rows
and the final receipt's generation; receipts alone do not prove deletion.
`conformance/secrets-v1.json` publishes the normative case inventory and vectors;
these are contract examples, not captured external-provider fixtures.

## Protected incoming HMAC bindings

The Auth binding contract is `secrets::binding::SecretBindingHost::verify_hmac`
with the Rust-owned `VerifyHmacRequest`:
`{secretBinding: string, alg: string, body: Uint8Array, signature: Uint8Array}`
and a boolean result. Auth's existing generator projects the request into
TypeScript; no separate Cloud schema is needed. This is a protected host call,
not an HTTP secrets-management route. No secret bytes or computed MAC cross
the guest boundary. There is no read, sign or export operation.

`alg` must be exactly `HMAC-SHA256`. `body` is the untouched incoming byte
sequence; `signature` is the full raw 32-byte tag. Apps decode hex/base64 and
remove provider prefixes before calling. Incorrect tags, lengths and algorithms
return false. Unknown selections fail `UnknownBinding`; resolver failures
(including stale versions and denied/revoked authority) fail `Unavailable`,
without provider diagnostics. Equal-length tags use RustCrypto's constant-time
verification. Unsupported algorithms and wrong lengths never resolve a key.

Native hosts install `LocalSecretBindings::new` with admitted `(environment
name, SecretRef)` selections and the original scoped `Resolver`. Resolution
uses that exact reference on every call; it never falls back to a newer version.
The host retains the resolver and protected, zero-on-drop resolved value.

Cloud implements the `SecretBindingHost` TypeScript port once, keeping the
selected name-to-reference map, resolver, authorization and key on its host
side. Pass that original port as `BindingAuthority.secretBindings` and pass the
admitted deployment `secrets` as the third argument to Workers'
`materializeBindings`. The resulting `env.NAME` exposes only
`verifyHmac(alg, body, signature): Promise<boolean>`. Auth also exports
`verifyHmac(secretBinding, alg, body, signature)`. The port is never put in env.
The TS projection captures byte views before dispatch and suppresses host
exceptions and non-boolean replies to prevent provider-data disclosure.

`secrets::conformance::verify_hmac` is the portable host suite. Install the
public RFC 4231 case 2 fixture as `WEBHOOK` and leave `MISSING` unselected. It
covers valid/wrong tags, short/long tags, unknown selections, algorithm mismatch
and changed body bytes; SDK tests run it against the native local host. The
guest custody negative control plants readable host-only data and checks that
property access, serialization, failed calls and malformed replies cannot
reveal it.

## Remote account-root certification

`trust::issuance::Client` retains one original `operation::Context` and immutable
`Request`. Supply the operation key, pinned account-root public key, environment
and existing `CertifiedKeyProto` claims once. `issue` dispatches once; after an
unknown result, `recover` looks up that same request without generating another
operation or leaf key. A renewal uses a new operation key.

The `Provider` adapter uses the issuer's authenticated transport and existing
transaction. It retains current caller admission and custody through dispatch,
deduplicates concurrent work under the original keyed owner, and atomically binds
`(issuer, account, operation_key)`, request digest and terminal decision before
disclosing a reply. An identical request returns its retained result without
signing again; a different digest returns `Refusal::Conflict`. Recovery of an
absent record returns `Decision::Unknown` and must not issue or reserve a key.
`AccountRoot::certify_request` only signs the admitted public claims; the original
serving transaction establishes acceptance.

`Request::wire` borrows the retained canonical v1 protobuf envelope. Its digest
is SHA256 of `b"acyclic.auth.v1/issuance\0"` followed by that complete envelope.
`Request::decode`, `Reply::decode` and `Reply::into_wire` share the generated Auth
contract. Decoders reject unsupported versions and noncanonical encodings.
Public wire decoding supplies neither caller authority nor durable acceptance.

The client admits an accepted certificate only against the exact request, pinned
signer, current trust, revocation state and original clock. A refusal proves that
this exact request did not commit. Transport loss, caller loss or a stop after
dispatch leaves the result unknown. The provider must end source loss with a
typed error, including before EOF. Neither issue nor recovery polls or retries
automatically. This contract defines no HTTP route or new credential store;
serving adapters attach it to their original admission and transaction owners.

## Secret references

`secrets::SecretRef {scope, name, version}` selects an exact host credential
through `secrets::Resolver`, either the bytes that version wrote or a typed
`ResolveError`. The resolved `SecretValue` is redacted and zeroed on drop.
Protected values remain with the host credential adapter; they never enter
guest environments, streams, volumes, tool journals or logs.

Actors selections, manifest host credentials and model authentication use this
same recorded reference and its shared `acyclic.harness.v2.SecretRef` wire form.
Native configuration variable names remain explicit host inputs. They do not
claim the immutable version of a published secret, and file content hashes do
not manufacture one.
