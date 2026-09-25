//! Generates the exact customer transport from the committed descriptor set.

use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptors = prost_types::FileDescriptorSet::decode(
        include_bytes!("inference_descriptor.bin").as_slice(),
    )?;
    if std::env::var_os("CARGO_FEATURE_HOST").is_some() {
        tonic_prost_build::configure()
            .build_client(true)
            .build_server(true)
            .compile_fds_with_config(descriptors, tonic_prost_build::Config::new())?;
    } else {
        prost_build::Config::new().compile_fds(descriptors)?;
    }
    println!("cargo:rerun-if-changed=inference_descriptor.bin");
    Ok(())
}
