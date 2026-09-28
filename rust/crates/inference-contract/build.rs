//! Generates transport-independent customer protobuf messages from the committed descriptor.

use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor_path = "inference_descriptor.bin";
    let descriptors =
        prost_types::FileDescriptorSet::decode(std::fs::read(descriptor_path)?.as_slice())?;
    prost_build::Config::new().compile_fds(descriptors)?;
    println!("cargo:rerun-if-changed={descriptor_path}");
    Ok(())
}
