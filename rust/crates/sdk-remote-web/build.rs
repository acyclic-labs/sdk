use prost::Message;

const MODEL_DESCRIPTOR: &[u8] =
    include_bytes!("../filesystem/src/generated/rust-model-filesystem-v2.bin");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor = acyclic_sdk_contract_wire::descriptor_set_with_docs(
        acyclic_sdk_contract_wire::BindingFamily::Filesystem,
        MODEL_DESCRIPTOR,
    )?;
    let descriptors = prost_types::FileDescriptorSet::decode(descriptor.as_slice())?;
    tonic_prost_build::configure()
        .build_client(true)
        .build_transport(false)
        .build_server(false)
        .compile_fds_with_config(descriptors, tonic_prost_build::Config::new())?;
    println!("cargo:rerun-if-changed=../filesystem/src/generated/rust-model-filesystem-v2.bin");
    println!("cargo:rerun-if-changed=../sdk-contract-wire/src");
    Ok(())
}
