//! Allocation profiling for the filesystem benchmarks.

#[path = "filesystem.rs"]
mod filesystem;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    filesystem::main();
}
