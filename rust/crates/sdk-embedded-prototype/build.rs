use std::path::PathBuf;

fn main() {
    // Keep every Rust-owned ABI surface in the build dependency graph. The C
    // header is emitted from lib.rs today, while the optional UniFFI surface
    // is declared in uniffi_polling.rs; changing either must invalidate the
    // generated binding package and its provenance receipt.
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=build.rs");
    let output = PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR is set"))
        .join("acyclic_embedded_prototype.h");
    cbindgen::Builder::new()
        .with_crate(std::env::var("CARGO_MANIFEST_DIR").expect("manifest directory is set"))
        .with_language(cbindgen::Language::C)
        // Keep the single Rust-owned C header directly consumable from C++20 as
        // well as C: fixed-width enums remain ABI-stable and exported symbols
        // retain C linkage under __cplusplus.
        .with_cpp_compat(true)
        .generate()
        .expect("the embedded prototype C header must generate")
        .write_to_file(output);
}
