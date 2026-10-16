//! Build Actors transport from the packaged protocol.
#[path = "src/codegen.rs"]
mod codegen;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-check-cfg=cfg(kani)");
    println!("cargo:rustc-check-cfg=cfg(loom)");
    let out_dir =
        std::env::var_os("OUT_DIR").ok_or_else(|| std::io::Error::other("OUT_DIR is not set"))?;
    codegen::generate(out_dir)?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
