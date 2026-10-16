//! Allocation instrumentation stays in its own benchmark executable.
#[path = "../../conformance/benches/filesystem.rs"]
mod filesystem;

#[path = "support/admission.rs"]
mod admission;

#[path = "support/extents.rs"]
mod extents;
#[path = "support/lazy_nodes.rs"]
mod lazy_nodes;

#[path = "support/merge_diff.rs"]
mod merge_diff;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    filesystem::main();
}
