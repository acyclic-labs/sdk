//! Builds Machines messages and documentation directly from their Rust model.

use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_sdk_contract_wire::{BindingFamily, descriptor_set_with_docs};
    let model = acyclic_sdk_contract_wire::machines::machines_descriptor();
    let documented = descriptor_set_with_docs(BindingFamily::Machines, &model)?;
    let descriptors = prost_types::FileDescriptorSet::decode(documented.as_slice())?;
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("wire");
    let messages_dir = out_dir.join("messages");
    let tonic_dir = out_dir.join("tonic");
    std::fs::create_dir_all(&messages_dir)?;
    let mut messages = prost_build::Config::new();
    messages.out_dir(&messages_dir);
    messages.compile_fds(descriptors.clone())?;

    if std::env::var("CARGO_CFG_TARGET_ARCH")? != "wasm32" {
        std::fs::create_dir_all(&tonic_dir)?;
        tonic_prost_build::configure()
            .out_dir(&tonic_dir)
            .build_client(true)
            .build_server(true)
            .extern_path(".acyclic.machines.v1", "crate::wire")
            .compile_fds_with_config(descriptors, tonic_prost_build::Config::new())?;
    }
    acyclic_sdk_contract_wire::transport_control::generate_control_bindings(
        &out_dir.join("control"),
        acyclic_sdk_contract_wire::BindingTransport::Tonic {
            client: std::env::var("CARGO_CFG_TARGET_ARCH")? != "wasm32",
            server: std::env::var("CARGO_CFG_TARGET_ARCH")? != "wasm32",
        },
    )?;
    for source in [
        "build.rs",
        "../sdk-contract-wire/src/machines.rs",
        "../sdk-contract-wire/src/protocol.rs",
        "../sdk-contract-wire/src/transport_control.rs",
        "../sdk-contract-wire/src/lib.rs",
        "../sdk-contract-wire/src/bindings.rs",
        "../sdk-contract-options/src/lib.rs",
    ] {
        println!("cargo:rerun-if-changed={source}");
    }
    Ok(())
}
