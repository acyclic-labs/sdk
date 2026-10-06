# Rust-owned website documentation migration

Status: website audit and generated-reference consumer preview, 2026-10-03

This note records the website boundary for the Rust SDK and documentation
source-of-truth work. It is an implementation guide and audit record; it does
not declare the website migration complete.

## Existing website surface

The acyclic.dev checkout currently contains 50 documentation route modules:

| Area | Route modules | Source shape |
| --- | ---: | --- |
| `/docs` | 1 | Landing page |
| filesystem | 12 | Overview plus 11 authored pages |
| harness | 2 | Overview plus a dynamic slug route backed by 11 TS data pages |
| inference | 2 | Overview plus a dynamic slug route backed by 19 TS data pages |
| machines | 11 | Overview plus 10 authored pages |
| managed-agent-runtime | 1 | Authored page |
| objects | 9 | Overview plus 8 authored pages |
| plugins | 1 | Authored page |
| roadmap | 1 | Authored page |
| stream | 10 | Overview plus 9 authored pages |

The site has 32 documentation data modules under `src/lib/docs`, with
handwritten Svelte/TypeScript content and hardcoded area navigation. Harness
and Inference use dynamic pages, and `/docs/harness/overview` has an explicit
308 redirect to `/docs/harness`; that behavior must remain part of the route
parity test. The current site also contains 94 examples (66 Rust, 8
TypeScript, 19 text, and 1 shell), which are an inventory to migrate and
validate rather than independent documentation authority.

The docs layout is development-only: `src/routes/docs/+layout.ts` returns 404
outside `dev`, and `static/robots.txt` disallows `/docs`. This is a deliberate
release gate while generated content is qualified. A generated bundle must
not silently make planned or unavailable products appear released; changing
the gate belongs to a later reviewed release milestone.

## Claims and release guards

The website currently publishes `claims.v1.json`, with the website claims
source pinned to a repository revision and route/source hashes. It contains
2,157 inventory claims over 55 route entries. `ProductContractNotice.svelte`
renders product status from this contract, while the claims checker also
contains product-specific artifact assertions (including the Objects release
candidate). This is a second authority that must be reconciled with the Rust
bundle before handwritten product claims are removed.

The generated bundle consumer must carry, or receive through a checked
compatibility projection, the following fields for every product and package:

* maturity (`planned`, `preview`, `released`, or an explicitly documented
  equivalent);
* deployment and service availability;
* qualification state and supported capabilities;
* package/crate identity and exact version or source revision;
* artifact and source hashes, generator version, and generator input
  revision; and
* release channel, when a branch preview differs from released documentation.

The renderer should refuse install commands, published-package claims, or
hosted-service instructions when availability is absent. Unknown qualification
must render as unknown and block release checks rather than being interpreted
as supported. The claims checker should consume the generated status
projection (or verify an exact projection hash) so status cannot drift between
Rust and the website.

## Bundle and renderer boundary

The Rust `sdk-docs` prototype emits a deterministic `DocsBundle` with schema
version, source revision, crate inventories, public items, source file hashes,
rustdoc provenance when available, diagnostics, and a bundle BLAKE3 digest. Its
source scanner explicitly marks fallback analysis and unresolved conditional
or re-export cases; the website must preserve those diagnostics and never
present fallback data as a complete API inventory.

The isolated website worktree contains a small preview consumer at
`/docs/reference/[family]` and a checked-in fixture at
`src/lib/generated/sdk-reference-bundle.json`. The Svelte page is only a
renderer: it displays the source repository, revision, generator provenance,
status fields, and public symbols. The adapter accepts the current family
projection and can normalize the Rust `DocsBundle` shape while the final
generated artifact path is wired into the build. It does not replace the
existing authored pages yet.

The intended build boundary is:

```text
Rust crates + Rust doc comments + crate-owned Markdown/examples
        -> pinned rustdoc/sdk-docs generator
        -> content-addressed generated bundle and status projection
        -> website route/search/navigation renderer
```

The website may retain TypeScript/Svelte for presentation, accessibility,
responsive layout, and client navigation. Shared API types, operation names,
wire identities, examples, availability claims, and reference prose must be
generated or projected from Rust-owned inputs. A separate TypeScript contract
or behavior implementation is not an acceptable replacement source.

## URL, navigation, and search requirements

Before replacing an authored page, build a route map from the current route
inventory and generated family/guide identifiers. Preserve existing URLs;
where a generated slug differs, emit a permanent redirect and record it in a
checked-in redirect manifest. Keep the Harness overview redirect and all
navigation links in the parity set. Do not infer route coverage from the
number of crates: guides, capability pages, package pages, and roadmap pages
need explicit source ownership or an explicit exclusion reason.

The bundle should emit a search document for each reference item and guide,
with stable IDs, title, headings, URL, language, product, source revision, and
availability metadata. Build the search index from that document, test that
unavailable products are labelled in results, and verify that no stale
handwritten page is indexed after a regeneration.

## Verification gates

The website migration is reviewable only when a clean checkout can regenerate
the same bundle and digest. CI should check:

1. source revision, generator version, schema version, and artifact digests;
2. generated-file drift and route/redirect parity;
3. completeness of public symbol and source attribution coverage;
4. links, anchors, code highlighting, language switching, and search;
5. responsive desktop/mobile layout and keyboard/screen-reader semantics;
6. example compilation or execution against the matching generated package;
7. claims parity, including planned/released and service availability gates;
8. branch preview versus released-documentation labels; and
9. absence of independently authored shared SDK contracts and generated-file
   edits outside the generation boundary.

Rustdoc JSON is still an experimental rustdoc output. Pin the toolchain and
keep JSON extraction isolated from the stable product build; record its exact
format/provenance in the bundle. A source-fallback bundle may support research
and preview rendering, but it cannot satisfy release completeness by itself.

Production deployment, registry publication, and merging to `main` are out of
scope for this loop. The isolated preview and this report are intended for
review while the Rust generator, compatibility projection, and language
qualification work continue.

## Generated guide and reference preview — iteration 3

The Rust `sdk-docs` command now writes the website projection itself through
`--website-output`; the website no longer has a TypeScript projection script.
The projection carries crate-owned README/guide contents, Cargo install
instructions, profile statuses, source paths, bundle digests, and branch
preview provenance. The generated reference family pages expose keyboard
search, source-file grouping, stable symbol anchors, optional signatures, and
source paths. Working-tree previews deliberately show recorded paths and
digests without linking to a potentially different remote checkout.

The existing `/docs/stream/quickstart` URL now consumes the Rust Stream README
from the generated bundle. It keeps the existing Stream navigation and URL,
while removing the independently authored SDK snippets from that route. The
generated guide remains raw Markdown in this renderer until the website’s
trusted Markdown component is selected and pinned; the source and provenance
remain exact in the meantime.

This is a bounded migration step, not completion: current profile statuses
remain partial and source-fallback until the bound rustdoc JSON graphs and
receipts cover the relevant targets. Other authored family routes remain in
the route-coverage backlog until their Rust-owned guide and executable example
coverage is qualified.

## Rendered preview review — iteration 2

Browser checked `http://127.0.0.1:7878/docs/reference/actors` with desktop 1280x900 and mobile 390x844 viewports. Single H1, legible responsive content, and no measured document horizontal overflow at these sizes. The route renders source revision, digest, and the source-fallback/unqualified warning correctly.

Not accepted as pristine/completed documentation: the current fallback list has truncated source-scanner names, duplicate unqualified symbol names, missing summaries, no useful module/type grouping, and the Rust-reference breadcrumb incorrectly selects Objects as its destination. The website owner is replacing the input with resolved rustdoc metadata and adding generated navigation, signatures, search, and source-bound guide/snippet rendering. Existing handwritten pages and source modules remain migration backlog. Website check/build and negative digest/revision checks passed according to lane evidence; root rendered review confirms the current preview is diagnostic only.
