# Resource identity views

`ResourceIdentity` wraps the existing `(account, id)` fields. It does not create,
rename or migrate a resource. Existing pair-scoped RPC fields, storage fields and filesystem
ID derivation remain unchanged; the actor API transition is described below. Filesystem and machine views retain their existing
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
Invalid IDNA A-labels are rejected by the shared URL host parser; valid ASCII
A-labels are retained byte for byte. No input is converted to another spelling.

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

Actor namespaces extend the view without changing existing pair encodings.
`ActorReference` is the plain `(account, namespace, id)` triple. Its binary form
starts with eight zero bytes, ASCII `acyclic.actor`, version byte `1`, and three
u64-BE length-prefixed raw fields. Empty accounts are invalid in both grammars,
so a triple cannot be parsed as a pair. Actor IDs and namespaces are exact,
case-sensitive ASCII `[A-Za-z0-9-_.]`, 1–128 bytes. An absent legacy namespace
maps to reserved `__default`; an explicit empty namespace is invalid. Namespace
deployment cannot claim `__default`. Existing persisted actor IDs are retained.

Actor references serialize addresses only. To invoke one, reconstruct it through
an admitted namespace binding with the same account and namespace. `get` is local;
first invocation activates, and the first write materializes state. Namespace
code/configuration/bindings/jurisdiction are deployed explicitly. `deleteAll`
clears storage and alarms; later access can activate the same address again.
Cloud owns single-writer placement and incarnation fencing. Actors have no direct
DNS form. Workers are named script deployments with the pair identity, rather
than actor instances. Worker deployment and namespace deployment share a
versioned deployment envelope.

`fromHostname(host, domain)` strips the exact configured domain suffix, including
multi-label suffixes such as `run.cell-1.example`, accepts exactly two identity
labels, and requires byte-exact re-rendering. Uppercase aliases, trailing dots,
ports, extra labels and mismatched suffixes are rejected. No text URI for pairs
or triples is defined; logs carry the fields separately.

Worker route patterns and selection also execute in Rust through WASM. Patterns
support an optional HTTP(S) scheme, a leading hostname wildcard and a trailing
path wildcard. Matching includes the query string, preserves path case, and
chooses the most-specific match independently of declaration order. A targetless
route suppresses broader routes. Equally specific winning routes with different
targets are rejected. Only valid Worker labels are targets; an empty string is
not a named target. Custom domains and pattern routes target Workers, while
actors are invoked through namespace bindings.

The full parity pin and Cloud implementation obligations are recorded in
[Actors/Workers parity](actors-workers-parity.md). The SDK contracts are proposed
interfaces for Cloud's namespace front door; fixture tests do not claim that the
older live CreateActor service implements them.
