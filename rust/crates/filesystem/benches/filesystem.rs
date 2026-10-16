//! Filesystem-only entry point for the shared read and accounting benchmarks.
#[path = "../../conformance/benches/filesystem.rs"]
mod filesystem;

#[path = "support/admission.rs"]
mod admission;

#[path = "support/lazy_nodes.rs"]
mod lazy_nodes;

#[path = "support/merge_diff.rs"]
mod merge_diff;

fn main() {
    filesystem::main();
}
