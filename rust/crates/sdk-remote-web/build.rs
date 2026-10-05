use prost::Message;
use prost_types::FileDescriptorSet;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build the browser contract directly from the Rust model. The checked-in
    // model descriptor remains a reproducibility artifact for package
    // consumers; it is not an input authority for this generation step.
    let filesystem_family = acyclic_sdk_contract_wire::BindingFamily::Filesystem;
    let filesystem_model = filesystem_family.model_descriptor();
    let filesystem =
        acyclic_sdk_contract_wire::descriptor_set_with_docs(filesystem_family, &filesystem_model)?;
    let harness_family = acyclic_sdk_contract_wire::BindingFamily::Harness;
    let harness_model = harness_family.model_descriptor();
    let harness =
        acyclic_sdk_contract_wire::descriptor_set_with_docs(harness_family, &harness_model)?;
    let mut descriptors = FileDescriptorSet::decode(filesystem.as_slice())?;
    let harness = FileDescriptorSet::decode(harness.as_slice())?;
    for file in harness.file {
        if !descriptors
            .file
            .iter()
            .any(|existing| existing.name == file.name)
        {
            descriptors.file.push(file);
        }
    }
    tonic_prost_build::configure()
        .build_client(true)
        .build_transport(false)
        .build_server(false)
        .compile_fds_with_config(descriptors, tonic_prost_build::Config::new())?;
    println!("cargo:rerun-if-changed=../filesystem/src/generated/rust-model-filesystem-v2.bin");
    println!("cargo:rerun-if-changed=../harness/src/generated/rust-model-harness-v2.bin");
    println!("cargo:rerun-if-changed=../sdk-contract-wire/src");
    Ok(())
}
