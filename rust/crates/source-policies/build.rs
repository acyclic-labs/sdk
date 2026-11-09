#[path = "src/catalog_build.rs"]
mod catalog_build;

fn main() {
    println!("cargo:rerun-if-changed=src/catalog_build.rs");
    println!("cargo:rerun-if-changed=build.rs");
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("build output"));
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    catalog_build::generate(&root, &output).expect("repository Rust source catalog");
}
