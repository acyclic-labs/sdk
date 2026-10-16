//! Allocation instrumentation stays in its own benchmark executable.
#[path = "../../conformance/benches/filesystem.rs"]
mod filesystem;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    filesystem::main();
}
