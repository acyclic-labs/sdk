#[path = "support/object_batches.rs"]
mod object_batches;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    object_batches::main();
}
