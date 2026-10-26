# Provider pages in browser clients

`PageWindow` in the TypeScript package is the generated `WasmPageWindow`.
It uses `acyclic_harness_client::PageWindow`; there is no TS item cache or
cursor reducer. Construct it after `initializeClientQueries()` with finite
`(pages, items, bytes)` budgets. Hard ceilings are 64 pages, 4096 items and
16 MiB. The byte budget also limits each encoded input before its Rust copy.

`reset(bytes)` and `finish(request, bytes)` accept UTF-8 JSON described by the
Rust-generated `Page` type. Entries contain `{ key, value }`. Every page has
an opaque generation, exclusive `before` and `after` cursor strings, and
`hasMoreBefore` / `hasMoreAfter`. A provider must preserve its query cut and
return exclusive, ordered pages. This adapter does not authorize a query or
mint a provider cursor.

`begin("before" | "after")` returns one `PageRequest`, or `undefined` at a
closed edge. Its `cursor()` supplies the exact provider token. The caller
owns the physical fetch and calls `finish` when it settles. Pass `undefined`
on a provider error or after cancellation has physically drained; preserve
the original provider error for the caller. Free each request after finishing.
Reset fences an old response but retains its fetch custody until settlement.
A second fetch receives `page_busy`, including a fetch in the other direction.

Eviction releases complete pages. The surviving pages own both cursors, so
backfill starts at the retained edge. Oversized pages, duplicate identities,
changed generations and nonadvancing nonterminal responses are typed errors;
they never truncate a provider page or silently update an overlapping item.

`items()` returns the same frozen array until the revision changes. Values
use the existing exact-integer JSON projection: ordinary JSON objects retain
property access, 64-bit integers remain `bigint`, and validated file-descriptor
lengths use the existing exact-number conversion. The array is immutable;
caller-held values are presentation data and cannot mutate the retained bytes.
`retainedBytes()` counts Rust page backing and metadata, not JS heap usage.
The client owns at most one cached JS projection of its finite retained pages;
caller-held older arrays have caller-owned lifetimes.

These source contracts and the migrated browser fixtures are uncompiled and
unrun. They do not establish hosted endpoint, allocation, or performance proof.
