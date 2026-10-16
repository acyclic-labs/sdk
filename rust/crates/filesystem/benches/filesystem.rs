//! Filesystem-only entry point for the shared read and accounting benchmarks.
#[path = "../../conformance/benches/filesystem.rs"]
mod filesystem;

#[path = "support/admission.rs"]
mod admission;

fn main() {
    filesystem::main();
}
