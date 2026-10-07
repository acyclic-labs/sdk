//! Build the Workers descriptor and transport directly from the Rust contract.

#[path = "src/codegen.rs"]
mod codegen;
#[path = "src/contract.rs"]
mod contract;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-check-cfg=cfg(kani)");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=src/contract.rs");
    println!("cargo:rerun-if-changed=src/contract_definitions.rs");
    println!("cargo:rerun-if-changed=src/codegen.rs");
    let out_dir =
        std::env::var_os("OUT_DIR").ok_or_else(|| std::io::Error::other("OUT_DIR is not set"))?;
    codegen::generate(out_dir)?;
    Ok(())
}
