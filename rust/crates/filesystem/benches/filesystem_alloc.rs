//! Allocation instrumentation stays in its own benchmark executable.
#[path = "../../conformance/benches/filesystem.rs"]
mod filesystem;

#[path = "support/admission.rs"]
mod admission;

#[path = "support/decoded_pages.rs"]
mod decoded_pages;

#[path = "support/extents.rs"]
mod extents;
#[path = "support/lazy_nodes.rs"]
mod lazy_nodes;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    filesystem::main();
}
