# sdk-docs

`sdk-docs` is an isolated prototype for building the Rust-owned input bundle
used by SDK and website generators. It collects crate Markdown, Rust source,
module docs, and examples, then attaches the resolved public graph from
compiled rustdoc JSON when one is supplied.

Source scanning is intentionally conservative and reports unresolved
re-exports and conditional declarations. Website release builds should pass
`--strict-rustdoc-json` after generating JSON with a pinned rustdoc toolchain:

```text
cargo run --manifest-path rust/crates/sdk-docs/Cargo.toml -- \
  --repo-root . \
  --rustdoc-json target/doc \
  --output target/sdk-docs.json \
  --website-output target/sdk-reference-bundle.json \
  --examples-bundle target/sdk-examples-bundle \
  --strict-rustdoc-json
```

When `docs/rustdoc-profiles.json` exists it is loaded automatically. Pass
`--profile-manifest FILE` to select another manifest. Each profile lists every
package with its target selector and feature set. The `host` selector resolves
to the pinned toolchain's `rustc -vV` host triple during generation; explicit
targets such as `wasm32-unknown-unknown` remain exact. Receipts record the
resolved target, so a graph generated on one host cannot qualify a different
host profile. Strict mode requires the exact
source-bound graph for every crate entry; source scanning, a graph from a
different target, and a stale receipt cannot qualify a profile.
Set `default_features` explicitly for packages whose Cargo defaults are not
part of the graph.

The optional website projection is `sdk-reference-bundle.v1`. It contains the
Rust-derived navigation, guides, examples, package instructions, resolved API
items, source revision, bundle digest, and profile qualification status for the
Svelte presentation layer. Raw sources and diagnostics remain in the full
bundle for provenance and release checks.

`--examples-bundle` imports the Rust-owned `acyclic.sdk.examples.bundle.v1`
manifest. Each rendered language file is carried with its source revision,
source hash, validation receipt, and content digest; the loader recomputes the
declared SHA-256 values from the repository source and bundle bytes, verifies
every declared file exists, and rejects any stale or incomplete producer
closure. A directory source is hashed from every listed repository-relative
source file, including its path separators and exact bytes. The website
projection does not author or regenerate those snippets.

Receipts with `status: "qualified"` must also carry a bundle-relative
`artifact` object (`path` plus `sha256`) whose non-empty bytes exist in the
declared `files` set and match the digest. The expanded Rust receipt form may
instead provide all three `compile_artifact_*`, `runtime_artifact_*`, and
`package_artifact_*` path/hash pairs; every stage is checked. A qualified
receipt must carry bound stdout or stderr evidence (an output artifact object,
path/hash pair, or inline output whose digest matches) plus a non-empty
`assertions` list, or a positive `assertion_count`. Output files may be empty
when their bytes and hashes are present; silence alone does not qualify a run.
Hash-only output fields for missing files are rejected. Use `status:
"pending"` while an installable artifact, execution evidence, or assertion
record is unavailable; the importer never upgrades pending metadata.

The bundle has no timestamp. Its source revision, per-file BLAKE3 digests,
rustdoc format version, pinned toolchain, profile matrix digest, and canonical
bundle digest identify the exact inputs that a website or language SDK
generator consumed. Every crate also includes a generated semantic Markdown
reference, source-relative navigation, coverage counters, and an explicit
availability value. Registry publication is not inferred from Cargo's
`publish` flag.

Released documentation requires an explicit `acyclic.sdk.docs.release-qualification.v1`
manifest passed with `--release-manifest`. It must mark `qualified: true` and bind a
version and tag to the full Git revision at `HEAD`; the tag is resolved with Git and
the worktree must be clean. Release builds must also pass `--strict-rustdoc-json`
so a source-scanned fallback cannot be labeled as released documentation. Without that manifest, the bundle remains a branch
preview or source-only export and cannot be relabeled as a release by the website.
