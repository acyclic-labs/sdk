//! Generates only the native tonic service adapter. Messages are owned by the
//! transport-independent contract crate and referenced through `extern_path`.

use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptors = prost_types::FileDescriptorSet::decode(
        include_bytes!("inference_descriptor.bin").as_slice(),
    )?;
    if std::env::var_os("CARGO_FEATURE_HOST").is_some() {
        let mut config = tonic_prost_build::Config::new();
        config.extern_path(".inference.customer.v1", "::acyclic_inference_contract::wire");
        tonic_prost_build::configure()
            .build_client(true)
            .build_server(true)
            .compile_fds_with_config(descriptors, config)?;
    }
    println!("cargo:rerun-if-changed=inference_descriptor.bin");
    Ok(())
}
