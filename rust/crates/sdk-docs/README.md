# sdk-docs

`sdk-docs` is the standalone Rust library that turns pinned typed Rustdoc JSON
into deterministic, versioned SDK documentation data. Rust declarations and
their Rustdoc identities are the input authority; the library does not parse
generated TypeScript or protobuf files.

The library API is intentionally small:

```rust
use std::path::PathBuf;

use sdk_docs::{build_data, BuildInput, Channel, DocsData, Error};

fn build_preview(root: PathBuf, rustdoc_json: PathBuf) -> Result<DocsData, Error> {
    build_data(&BuildInput {
        version: "0.1.0".into(),
        channel: Channel::Preview,
        revision: "f".repeat(40),
        source_state: "working-tree".into(),
        source_sha256: None,
        repository_root: root,
        rustdoc_files: vec![rustdoc_json.clone()],
        package_metadata: vec![sdk_docs::PackageMetadata {
            rustdoc_file: rustdoc_json,
            package_name: "example-package".into(),
            crate_name: "example".into(),
            version: "0.1.0".into(),
        }],
        generated_sources: Vec::new(),
        mark_latest: false,
    })
}
```

`build_data` validates the Rustdoc format and source identity, projects public
items, docs, source spans, reexports, and same-crate public documentation links,
then returns `DocsData`. Call `write_bundle` to persist the data file, generated
schema, and guarded release or preview version index. Published release data
files are immutable; the index is validated and replaced atomically.

Rustdoc source spans inside `repository_root` are emitted as stable
repository-relative paths with normalized separators. A span whose physical
file is outside that checkout requires a transient `BuildInput.generated_sources`
entry. Each [`GeneratedSource`](src/lib.rs) entry identifies the exact physical
file, a relative logical path such as `generated/actors/wire.rs`, and its
SHA-256 digest (with or without the `sha256:` prefix). The library canonicalizes
the physical file, rejects symlink or reparse-point paths, rejects absolute or
escaping logical paths, rejects duplicate physical mappings and conflicting
logical digests, and verifies the digest
before projection and again after projection. The resulting `SourceSpan.path`
uses the logical path while retaining Rustdoc's line and column coordinates;
generated-source inputs are transient and do not add fields to the output
schema. Distinct physical files with identical bytes may share one logical
path; every physical alias is independently attested and resolves to that
same published source identity.

Crate-root `//!` documentation becomes a guide at the crate path. Public module
documentation becomes a guide at that module's Rust path; its title is the
first level-one Markdown heading, falling back to the final path component.
Public item comments, reexports, and same-crate Rustdoc links remain attached to
their projected API items, with reexported definitions using the effective
definition's documentation when the reexport has none.

The publication root contains `sdk-docs-versions.v1.json` and its
`sdk-docs-versions.v1.schema.json`. The index's `latest` entry is always the
registry-released stable version with the greatest SemVer whose retained
package evidence contains no yanked owner. Candidates and prereleases cannot
become `latest`; it is absent when no stable release is eligible. `releases`
contains stable release entries, `historicalPrereleases` preserves released
prereleases, `releaseCandidates` contains candidates, and `preview` is one
independent optional preview entry.
Resolve each entry's `dataFile` relative to the index root rather than
reconstructing a path from an untrusted version string. Release data is stored
under `releases/<version>/sdk-docs-data.v1.json`; candidate data is stored under
`release-candidates/<version>/sdk-docs-data.v1.json`; preview data is stored under
`preview/<version>/sdk-docs-data.v1.json`. Each version directory also contains
its `sdk-docs-data.v1.schema.json`.

The selected data file contains its own `version`, `channel`, `source`,
`navigation`, and `families`. The `version`, `channel`, and
`source.revision` must agree with the selected index entry, and `navigation`
describes that file's families.

`write_bundle` serializes publication with the persistent
`.sdk-docs-versions.v1.lock` file in the output directory. Concurrent
publishers may target the same output directory; each publisher holds this
lock across index validation, bundle writes, and index replacement so entries
are not lost.

The canonical contract is `sdk-docs-data.v1`. Every bundle requires Rust/Cargo-derived package and search records alongside navigation and families. `packageMetadata` identifies the Cargo package, Rust crate, and Rust crate version for every Rustdoc input; the preview label never becomes an installable package version. The `sdk-docs-versions.v1` index selects immutable version bundles using this same contract.
