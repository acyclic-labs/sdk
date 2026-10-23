# Resource identity views

`ResourceIdentity` wraps the existing `(account, id)` fields. It does not create,
rename or migrate a resource. Existing RPC fields, storage fields and filesystem
ID derivation remain unchanged. Filesystem and machine views retain their existing
16-byte IDs, rather than substituting workspace names or printable UUIDs.

The canonical view encoding is `u64be(account length) || account bytes ||
u64be(id length) || id bytes`. Both fields are nonempty opaque bytes; the view
performs no Unicode normalization and adds no kind, digest or generation.
Existing product admission rules still apply. `into_parts()` in Rust and
`toFields()` in TypeScript recover the original account-scoped fields.

DNS is `<id>.<account>.<domain>`, with strict lowercase LDH labels and no alias
folding. Only IDs that are already valid labels have a DNS representation.
The returned canonical hostname is the spelling for URL authority, TLS SNI and
signed Host bytes.

JavaScript callers must supply `Uint8Array` fields and string DNS text; wrong
argument shapes raise `TypeError` before WASM can coerce them. Byte-content and
DNS failures carry the shared Rust `IdentityError`/`DnsError` codes.

The single vector source is
[`resource-identity-v1.json`](../rust/crates/auth/tests/fixtures/resource-identity-v1.json).
Rust tests consume it directly; `conformance/addressing/shared-vectors.ts` reads that same file for the
Rust WASM-backed TypeScript tests in auth, Actors, Stream, Objects, filesystem,
Machines, Workers and the umbrella SDK. Each public product package exposes `./addressing` as a
re-export of `@acyclic-labs/auth/addressing`, so no second codec or validator can
diverge. The subpath keeps existing package imports and resource APIs unchanged.

Run the shared tests after building auth WASM and the package declarations:
`bun test typescript/packages/{auth,actors,stream,objects,filesystem,machines,workers,sdk}/test/addressing.test.ts`.
`node --test conformance/addressing/type-controls.test.mjs` checks every public
entrypoint's accepted byte fields and rejected string/mutable-field consumers.

```ts
import { ResourceIdentity } from "@acyclic-labs/objects/addressing";

const view = ResourceIdentity.fromFields({ account, id }); // Uint8Array fields
const encoded = view.encode();
const original = ResourceIdentity.decode(encoded).toFields();
```

The other language directories under `tools/sdk-generator/backends` generate
protobuf transport stubs. They do not currently contain SDK runtime bindings to
the Rust identity codec; `languages/README.md` and `ffi/README.md` describe that
boundary. This change does not introduce a native ABI or independently implement
validation in those generators. Their existing protobuf fields stay unchanged.
