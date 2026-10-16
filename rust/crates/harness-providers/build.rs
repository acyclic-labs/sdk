//! Register the crate-local model-checking configuration.
fn main() {
    println!("cargo:rustc-check-cfg=cfg(loom)");
}
