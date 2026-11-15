//! Lease path benchmarks, with allocation profiling.
#[path = "support/lease_paths.rs"]
mod lease_paths;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    lease_paths::main();
}
