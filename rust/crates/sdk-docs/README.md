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
        rustdoc_files: vec![rustdoc_json],
        mark_latest: false,
    })
}
```

`build_data` validates the Rustdoc format and source identity, projects public
items, docs, source spans, reexports, and same-crate public documentation links,
then returns `DocsData`. Call `write_bundle` to persist the data file, generated
schema, and immutable release or preview index.

The current contract is `sdk-docs-data.v1`; optional fields such as `links` are
omitted when empty and default during deserialization so older v1 bundles remain
readable.
