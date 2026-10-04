//! Generates the public hierarchical Stream bindings from the Rust contract model.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::path::PathBuf::from(
        std::env::var_os("OUT_DIR").ok_or("Cargo did not provide OUT_DIR")?,
    );
    // Native clients are always available. Browser builds use the Rust HTTP
    // provider and must not pull Tokio's native socket stack into wasm.
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    // Compatibility feature names never change the platform contract: a
    // browser build always emits the HTTP-only wire module, even when an
    // older manifest still spells `--features grpc`.
    let grpc = target_arch != "wasm32";
    acyclic_sdk_contract_wire::generate_rust_bindings(
        acyclic_sdk_contract_wire::BindingFamily::Stream,
        output,
        acyclic_sdk_contract_wire::BindingTransport::Tonic {
            client: grpc,
            server: grpc,
        },
    )?;
    println!("cargo:rerun-if-changed=build.rs");
    for source in [
        "../sdk-contract-wire/src/bindings.rs",
        "../sdk-contract-wire/src/lib.rs",
        "../sdk-contract-wire/src/protocol.rs",
        "../sdk-contract-wire/src/stream.rs",
        "../sdk-contract-options/src/lib.rs",
    ] {
        println!("cargo:rerun-if-changed={source}");
    }
    Ok(())
}
