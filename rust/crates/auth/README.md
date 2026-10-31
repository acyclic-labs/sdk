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
an existing `credential::CredentialProvider`. Credentials resolve on every call.
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

## Secret references

`secrets::SecretRef {scope, name, version}` selects an exact host credential
through `secrets::Resolver`, either the bytes that version wrote or a typed
`ResolveError`. The resolved `SecretValue` is redacted and zeroed on drop.
Protected values remain with the host credential adapter; they never enter
guest environments, streams, volumes, tool journals or logs.
