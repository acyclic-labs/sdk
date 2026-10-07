# Declared instructions and skills

Harness resolves declared repository instructions and skill frontmatter through
the ordinary context pipeline. The host supplies exact `ContextRoot` volumes,
signed read boundaries, a `ContentResidencyVerifier`, and finite
`ContextDiscoveryLimits`. A root declaration is data, not a grant.

`ContextDiscoveryPolicy::default()` selects `AGENTS.md` in root-to-active-directory
order and `SKILL.md` in immediate child directories of declared skill roots.
Roots retain declaration order; skill directories retain lexical order. No home
directory, host path, parent outside a declared root, or implicit global skill
catalog is consulted. Consumers can change filenames, clear instruction names,
disable skills, or use their own `ContextSource`, selection and renderer.

Constructing declarations and binding snapshots is pure. An authorized caller
explicitly invokes `ContextDiscovery::capture`, then binds the complete snapshot
with `HarnessBuilder::declared_context` or `DiscoveredContext::stage`. These are
the existing `SourceStage`, `ContextPipeline`, and executor binding digest, not a
second composition engine. A later builder `context` call replaces the stages.

The generated browser API uses the same Rust capture and validation code.
`captureDiscoveredContext` and `contextForRequest` receive a call-scoped
`ContextDiscoveryReader` whose bound owner supplies authenticated directory
pages, bounded prefixes and retained path reads. `readPinnedContextPath` loads a
body through those same owner operations. The reader does not grant authority,
store callbacks, open a host path or maintain a catalog. TypeScript's
`HarnessBuilder.declaredContext` binds a detached snapshot through the ordinary
context builder; later `context` calls replace it. Configure limits before
binding. Final model admission still applies the active request limits.

The Filesystem owner can retain a generation with `generation.pin(identity)`
and reopen it after restart with `workspace.generation(id)`. The latter exposes
the existing Rust exact-generation lookup in browser, native and hosted consumers;
it authenticates the original workspace and leaves its current head unchanged.
Pin identities name retention records. Reopening uses the original workspace
name and generation ID carried by the owner, not the pin identity.

Instructions are bounded UTF-8 text retained as immutable `FileRef` values with
the ordinary `bounded_full` projection policy. Skill metadata contains validated
name, description, additional declarative YAML fields, and an exact retained
generation/path. Metadata does not authorize tools or interpret frontmatter as
executable configuration. Discovery reads an authenticated bounded prefix, never
the complete skill body. `PinnedContextPath::read` loads the body later through
the ordinary owner-authorized pinned path read and verifies the returned descriptor.
Applications can then select/render that file with the existing context APIs.

## Reload and admission

`ContextReloadPolicy::default()` is `Explicit`: reuse the identified snapshot
until the caller captures a replacement. `NextRequest` opts into a capture at the
next caller-controlled admitted request boundary. There is no watcher or hidden
mutable callback. Capture returns a complete replacement or an error. The caller
retains its previous snapshot and pipeline on failure; clones already used by an
in-flight request keep their original immutable values.

Serialize the snapshot through the embedding workflow's existing journal when
restart requires reconstructing the binding. The executor's existing admission
records the exact prepared model request; supported replay reads that artifact
and does not repeat discovery or reinterpret a newer filesystem head. A changed
binding is a new admission, not a replacement for an existing operation identity.

## Bounds and authority

Each capture has combined directory and entry budgets, a frontmatter-prefix
budget selected by the caller, and an instruction-file byte budget. Pagination
pins one generation and checks ordered advancing cursors, page sizes and provider
identity. Instruction capture rejects oversize before requesting a complete
read. Missing declared directories, missing retained data, malformed frontmatter,
duplicate skill names, named entries with the wrong file kind, invalid text,
unsupported provider operations and exceeded
budgets have explicit errors; absence of an optional filename is ordinary absence.
The complete snapshot and projected request also obey their existing protocol
and rendering limits.

The owner's prefix bound must permit `instruction_bytes + 1` for the oversize
probe and the declared `header_bytes` for skill metadata. Complete instruction
reads remain bounded by `instruction_bytes`; a smaller owner read limit returns
an explicit error.

Admitted frontmatter has no additional fixed byte, name, description, event,
node, scalar or nesting ceilings. Discovery enforces the caller's prefix budget;
direct parsing consumes only the supplied slice. The YAML format requires one
document; aliases,
anchors, merge keys, duplicate keys and unsupported tags are rejected. Include
and property-expansion features are not enabled. This retains ordinary YAML
frontmatter without a custom parsing language or filesystem side effects.

Built-ins use separate read-only roots. Editable session skills use independently
declared volumes and grants. Capture and reload cannot write either root. Skill
fork/import and project integration reuse [volume composition](volume-composition.md):
project integration never imports skills, and skill root writeback requires the
explicit exact destination write grant supplied by the caller's approval workflow.

## Verification boundary

| Invariant | Mechanism and assumption | Focused verification |
|---|---|---|
| No reads or authority from construction | Plain declarations and immutable source stage; the embedding owner authenticates reads | Empty/disabled discovery with a denied reader; explicit denied capture |
| Work stays bounded and metadata remains lazy | Pinned pages and authenticated bounded prefix; provider honors retained generations | Entry and instruction limits; discovery succeeds with a body larger than the full-read bound |
| Invalid replacements cannot alter active context | Complete replacement values and existing pipeline reload | Changed/invalid skill, retained old pipeline, serialized snapshot reconstruction |
| Metadata validation applies after restart | Shared metadata and snapshot validators | Forged names/fields, duplicate keys, aliases, invalid UTF-8 and folded CRLF YAML; accepted deep nesting and metadata beyond former fixed ceilings |
| Admitted replay retains exact revision | Existing Stream/Filesystem journal and executor request artifact; trusted provider durability | Reconstructed journal/executor after invalid source update, one model dispatch, metadata without body, old pinned body readable |
| Skills and project writeback remain separate | Existing C volume grants and explicit fork/import | Existing `skills_are_explicitly_forked_and_imported_with_read_only_builtins` consumer |

These tests establish the exercised paths under the stated provider assumptions;
they are not a proof for arbitrary custom providers. Native focused execution is
an intermediate receipt. Final-source generated-consumer and platform execution
must be recorded before qualification or merge.
