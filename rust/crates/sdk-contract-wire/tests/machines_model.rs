use acyclic_sdk_contract_wire::machines::{MACHINES, machines_descriptor};
use prost::Message;
use prost_types::FileDescriptorSet;

const MACHINES_GOLDEN_DESCRIPTOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/machines-v1.descriptor.bin"
));

#[test]
fn machines_model_matches_compatibility_descriptor() {
    let mut descriptor = FileDescriptorSet::decode(MACHINES_GOLDEN_DESCRIPTOR)
        .expect("decode Machines golden descriptor");
    // The runtime baseline carries compiler source info and unknown source
    // metadata. The Rust exporter intentionally emits the canonical schema
    // descriptor without those non-wire annotations; compare that normalized
    // baseline byte-for-byte while retaining the immutable fixture itself.
    for file in &mut descriptor.file {
        file.source_code_info = None;
    }
    assert_eq!(machines_descriptor(), descriptor.encode_to_vec());

    let file = descriptor.file.first().expect("Machines file descriptor");
    assert_eq!(file.name.as_deref(), Some(MACHINES.file_name));
    assert_eq!(file.package.as_deref(), Some(MACHINES.package));
    assert_eq!(file.syntax.as_deref(), Some(MACHINES.syntax));
    assert_eq!(file.message_type.len(), 47);
    assert_eq!(file.enum_type.len(), 9);
    assert_eq!(file.service.len(), 1);
    assert_eq!(file.service[0].name.as_deref(), Some("MachinesService"));
    assert_eq!(file.service[0].method.len(), 19);

    let contract = file
        .message_type
        .iter()
        .find(|message| message.name.as_deref() == Some("MachineContract"))
        .expect("MachineContract descriptor");
    assert_eq!(contract.reserved_name, vec!["performance"]);
    assert_eq!(contract.reserved_range[0].start, Some(5));
    assert_eq!(contract.reserved_range[0].end, Some(6));

    let image = file
        .message_type
        .iter()
        .find(|message| message.name.as_deref() == Some("Image"))
        .expect("Image descriptor");
    assert_eq!(
        image.oneof_decl[0].name.as_deref(),
        Some("immutable_reference")
    );
    assert_eq!(image.field.len(), 4);
    assert!(
        image
            .field
            .iter()
            .all(|field| field.oneof_index == Some(0) || field.name.as_deref() == Some("kind"))
    );

    let watch = file.service[0]
        .method
        .iter()
        .find(|method| method.name.as_deref() == Some("WatchOperation"))
        .expect("WatchOperation descriptor");
    assert_eq!(watch.server_streaming, Some(true));
}
