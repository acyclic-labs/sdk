# Rust-owned guide source coverage

Status: source guides added for Actors v1, Workers v1, and Objects v2 on
2026-10-03. This report records the source audit and the availability wording
that must survive website projection. It does not remove or redirect website
routes; the website route ledger remains the authority for that migration.

## Ownership and verification boundary

The new guides are crate-owned Markdown:

| Family | Guide | Canonical sources audited |
| --- | --- | --- |
| Actors v1 | `rust/crates/actors/docs/guide.md` | `README.md`, `src/lib.rs`, `src/generated/acyclic.actors.v1.rs`, `examples/actors-http-routes.rs` |
| Workers v1 | `rust/crates/workers/docs/guide.md` | `README.md`, `src/lib.rs`, `src/generated/acyclic.workers.v1.rs`, `examples/workers-http-routes.rs` |
| Objects v2 | `rust/crates/objects/docs/guide.md` | `README.md`, `src/v2/mod.rs`, `src/v2/request.rs`, `src/v2/response.rs`, `src/v2/memory.rs`, `src/v2/local.rs`, `src/generated/acyclic.objects.v2.rs` |

The guides use Rust-owned route constants and generated wire types as the
source of API names. They do not copy a second TypeScript contract or invent
service behavior. The Actors guide covers eight routes from
`acyclic_actors::HTTP_ROUTES`; the Workers guide covers seven routes from
`acyclic_workers::HTTP_ROUTES`; the Objects guide covers all 13 operations from
`acyclic_objects::v2::HTTP_ROUTES`.

The docs agent still owns the `lib.rs` `include_str!`/discovery changes that
make these files appear in rustdoc and the generated website bundle. This lane
does not edit those shared library files.

## Audited topic coverage

The route ledger lists nine Objects topics: overview, conditional operations,
durability limits, lifecycle, listing consistency, object metadata, quickstart,
S3 compatibility, and snapshots/forks. The Objects guide maps all nine:

| Legacy topic | Rust source result | Guide treatment |
| --- | --- | --- |
| Overview and quickstart | `MemoryObjects`, `ObjectsProvider`, and `HTTP_ROUTES` are present | Executable in-memory Rust example; transport availability remains separate |
| Conditional operations | `wire::Preconditions`, mutation identity, and atomic provider methods | Current-value conditions and retry identity semantics |
| Durability limits | `LocalObjectsLimits` and local journal replay | Native `local` feature, explicit limits, reopen behavior |
| Lifecycle | `LocalObjects::open` and `collect_garbage` | Private journal/body lifecycle and bounded reclamation |
| Listing consistency | `ObjectsProvider::list` and `response::listing` | Live bounded lexical pages; may lag mutations; no captured snapshot |
| Objects metadata | `wire::ObjectInfo`, `response::object_info`, timestamp checks | Current metadata and selected body bounds |
| S3 compatibility | No S3 adapter, endpoint, or route in the Objects crate | Explicitly marked unavailable; no compatibility claim |
| Snapshots/forks | README and v2 module explicitly reject public history | Explicitly marked not applicable; v2 has no public versions, snapshots, or forks |

Actors and Workers were not separate entries in the current website route
ledger, but their crate READMEs and route constants were audited as public
family topics. Their guides cover request validation, retry and revision
semantics, service transport names, route inventories, and the generated
module contract.

## Executable snippets and release wording

Each guide includes a Rust snippet that uses the public generated types and
validators/providers:

* Actors constructs `wire::CreateActorRequest` and calls `validate_create`.
* Workers hashes exact module bytes and calls `validate_publish`.
* Objects uses `MemoryObjects::with_default_bucket`, `ObjectsProvider::put`,
  and `ObjectsProvider::get`.

The snippets intentionally describe source-preview qualification. Workspace
metadata currently declares version `0.2.0` and publishable package settings,
but that does not prove a registry artifact or hosted service availability.
Objects is stronger: its README explicitly calls v2 an unreleased transition
and says it is not a published v2 release. The guides therefore avoid `cargo
add`, hosted endpoint instructions, and release claims until the package and
service gates qualify an exact artifact.

The in-memory Objects provider is clearly labeled as a local test composition;
it cannot establish hosted credentials, retention, or service acceptance. The
same distinction applies to generated route tables and local validation: they
are Rust contract projections, not evidence that a remote service is live.

## Source-bound examples and receipt integration

The existing examples owner pipeline is in
`rust/crates/sdk-examples/src/lib.rs` and `src/main.rs`. It is the correct
source-bound authority: `SCENARIOS` (lines 189-217) owns typed operations,
`transport_fixtures` (lines 266-317) owns encoded requests and expected
results, and the renderer and validator dispatch tables reject unknown
scenario IDs. The current registry contains `actors-create-roundtrip` and
`stream-append-read` only. Its manifest depends on Actors and Stream, not
Workers or Objects, so the three new guide snippets are not yet represented
by generated bundle receipts.

This is the precise handoff for the examples owner:

| Guide | Required source-bound scenario | Required receipt evidence |
| --- | --- | --- |
| Actors | `actors-create-roundtrip` | Already present: wire roundtrip and `validate_create`; no hosted transport claim. |
| Workers | `workers-publish-roundtrip` | Hash exact module bytes, encode/decode `PublishVersionRequest`, call `validate_publish`; mark package and hosted execution separately. |
| Objects | `objects-memory-put-get` | Execute `MemoryObjects::with_default_bucket`, `put`, and bounded `get`; mark v2 package/service as source preview. |

For Workers and Objects, integration must add the typed registry operation,
Rust renderer, fixture request/expected result, Rust receipt dispatch, and
crate dependency in `sdk-examples/Cargo.toml`. The existing no-placeholder
tests in the examples crate (`every_projection_has_imports_and_provenance`
and `transport_fixtures_are_derived_from_wire_types`) should cover the new
entries; the generated bundle must then be checked with `sdk-examples check`
and the fixture bundle with `sdk-examples fixtures check`. A temporary
consumer compile can prove public API shape, but it cannot substitute for
these source-bound receipts.

The current source-bound pipeline was inspected on 2026-10-03. Its test
command could not run in this checkout because Cargo tried to rewrite the
untracked nested `sdk-examples/Cargo.lock` and the managed worktree denied that
lock-file update; this does not change the registry audit above. The guides'
public API snippets were separately compiled against the three crate path
dependencies before this report was updated.

## Validation record

The crate-owned contract tests passed with
`cargo test -p acyclic-actors -p acyclic-workers -p acyclic-objects --lib`
(19 tests total). A temporary path-dependency consumer compiled and ran the
Actors, Workers, and Objects guide snippets with Cargo offline; it was removed
after the check. The authored guide links were then resolved against their
target files and exact `#L` anchors, and a placeholder scan plus `git diff
--check` passed. These checks prove source and public API shape. They do not
replace the pending `sdk-examples` Workers and Objects scenario receipts
described above.

## Remaining integration check

After the docs agent includes these files from each crate root, run the pinned
Rust docs/bundle command and inspect the generated guide inventory. The
website route ledger still requires explicit route preservation, redirect
validation, availability labels, and source-bundle checks. This report is a
coverage input for that gate, not a declaration that website migration is
complete.
