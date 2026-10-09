//! Build Workers descriptors and tonic directly from executable Rust declarations.
#[path = "src/admission.rs"]
mod admission;
pub use admission::{
    ContractError, MAX_INLINE_BYTES, MAX_JOB_ATTEMPTS, MAX_MODULE_BYTES, validate_publish,
    validate_result, validate_select, validate_submit,
};
#[path = "src/contract.rs"]
mod contract;
mod wire {
    pub use crate::contract::*;
}
#[path = "src/codegen.rs"]
mod codegen;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "node-binding")]
    if std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() != Ok("wasm32") { napi_build::setup(); }
    for source in [
        "build.rs",
        "Cargo.toml",
        "src/contract.rs",
        "src/domain.rs",
        "src/admission.rs",
        "src/codegen.rs",
    ] {
        println!("cargo:rerun-if-changed={source}");
    }
    codegen::generate(std::env::var_os("OUT_DIR").ok_or("OUT_DIR is missing")?)?;
    Ok(())
}
