//! Declare the crate-local synchronization-model configuration.
fn main() {
    println!("cargo::rustc-check-cfg=cfg(acyclic_harness_tui_loom)");
}
