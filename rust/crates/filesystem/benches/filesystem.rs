//! Filesystem-only entry point for the shared read and accounting benchmarks.
#[path = "../../conformance/benches/filesystem.rs"]
mod filesystem;

#[path = "support/admission.rs"]
mod admission;

#[path = "support/decoded_pages.rs"]
mod decoded_pages;

#[path = "support/read_admission.rs"]
mod read_admission;

#[path = "support/encoding.rs"]
mod encoding;

#[path = "support/workspace_names.rs"]
mod workspace_names;

#[path = "support/extents.rs"]
mod extents;
#[path = "support/lazy_nodes.rs"]
mod lazy_nodes;

#[path = "support/merge_diff.rs"]
mod merge_diff;

fn main() {
    filesystem::main();
}
