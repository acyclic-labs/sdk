//! Filesystem-only entry point for the shared read and accounting benchmarks.
#[path = "../../conformance/benches/filesystem.rs"]
mod filesystem;

fn main() {
    filesystem::main();
}
