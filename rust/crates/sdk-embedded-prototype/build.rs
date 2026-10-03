use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=src/lib.rs");
    let output = PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR is set"))
        .join("acyclic_embedded_prototype.h");
    cbindgen::Builder::new()
        .with_crate(std::env::var("CARGO_MANIFEST_DIR").expect("manifest directory is set"))
        .with_language(cbindgen::Language::C)
        .generate()
        .expect("the embedded prototype C header must generate")
        .write_to_file(output);
}
