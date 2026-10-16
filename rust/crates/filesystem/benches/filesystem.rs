//! Filesystem-only entry point for the shared read and accounting benchmarks.
#[path = "../../conformance/benches/filesystem.rs"]
mod filesystem;

#[path = "support/admission.rs"]
mod admission;

#[path = "support/extents.rs"]
mod extents;
#[path = "support/lazy_nodes.rs"]
mod lazy_nodes;

fn main() {
    filesystem::main();
}
