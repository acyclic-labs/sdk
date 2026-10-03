//! Generates the public hierarchical Stream bindings from the Rust contract model.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::path::PathBuf::from(
        std::env::var_os("OUT_DIR").ok_or("Cargo did not provide OUT_DIR")?,
    );
    let grpc = std::env::var_os("CARGO_FEATURE_GRPC").is_some();
    acyclic_sdk_contract_wire::generate_rust_bindings(
        acyclic_sdk_contract_wire::BindingFamily::Stream,
        output,
        acyclic_sdk_contract_wire::BindingTransport::Tonic {
            client: grpc,
            server: grpc,
        },
    )?;
    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}
