#[path = "src/catalog_build.rs"]
mod catalog_build;

fn main() {
    println!("cargo:rerun-if-changed=src/catalog_build.rs");
    println!("cargo:rerun-if-changed=build.rs");
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("build output"));
    // The manifest directory of this run, not of the compile that built this script.
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let root = std::path::Path::new(&manifest).join(catalog_build::ROOT);
    catalog_build::generate(&root, &output).expect("repository Rust source catalog");
}
