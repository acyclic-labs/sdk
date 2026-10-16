fn main() {
    // Instrument only this verification crate, leaving every dependency's cfg alone.
    println!("cargo:rustc-cfg=acyclic_sdk_docs_loom");
    println!("cargo:rerun-if-changed=build.rs");
}
