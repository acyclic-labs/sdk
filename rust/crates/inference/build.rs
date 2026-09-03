//! Generates the exact public Inference transport from its released descriptor set.

use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptors = prost_types::FileDescriptorSet::decode(
        include_bytes!("inference_descriptor.bin").as_slice(),
    )?;
    let mut prost = tonic_prost_build::Config::new();
    prost.bytes(["."]);
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_fds_with_config(descriptors, prost)?;
    println!("cargo:rerun-if-changed=inference_descriptor.bin");
    Ok(())
}
