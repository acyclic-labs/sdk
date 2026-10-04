use acyclic_sdk_contract_wire::machines::{MACHINES, machines_descriptor};
use prost::Message;
use prost_types::FileDescriptorSet;

const MACHINES_GOLDEN_DESCRIPTOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/machines-v1.descriptor.bin"
));
const MACHINES_DOC_DESCRIPTOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../machines/src/generated/acyclic-machines-v1.model.docs.bin"
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

#[test]
fn machines_product_descriptor_overlay_is_source_info_only() {
    let canonical = FileDescriptorSet::decode(
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../machines/src/generated/acyclic-machines-v1.model.bin"
        ))
        .as_slice(),
    )
    .expect("decode Machines model descriptor");
    let overlaid = FileDescriptorSet::decode(MACHINES_DOC_DESCRIPTOR)
        .expect("decode Machines documentation descriptor");
    let canonical_file = canonical
        .file
        .iter()
        .find(|file| file.package.as_deref() == Some("acyclic.machines.v1"))
        .expect("Machines canonical file");
    let overlaid_file = overlaid
        .file
        .iter()
        .find(|file| file.package.as_deref() == Some("acyclic.machines.v1"))
        .expect("Machines overlaid file");
    assert!(canonical_file.source_code_info.is_none());
    let locations = overlaid_file
        .source_code_info
        .as_ref()
        .expect("Machines source docs overlay")
        .location
        .as_slice();
    assert!(locations.iter().any(|location| {
        location
            .leading_comments
            .as_deref()
            .is_some_and(|text| text.contains("protocol version"))
    }));
    assert_eq!(canonical_file.name, overlaid_file.name);
    assert_eq!(canonical_file.message_type, overlaid_file.message_type);
    assert_eq!(canonical_file.service, overlaid_file.service);
}

#[test]
fn machines_policy_metadata_binds_rpc_only_operations_and_provider_errors() {
    let policies = MACHINES.operation_policies();
    assert_eq!(policies.len(), 19);
    assert!(
        MACHINES.routes.is_empty(),
        "Machines has no HTTP route projection"
    );

    let expected_errors = [
        "invalid",
        "not_found",
        "conflict",
        "unsupported",
        "rejected",
        "unavailable",
        "operation_indeterminate",
        "operation_observation_indeterminate",
        "operation_failed",
        "operation_cancelled",
    ];
    for policy in policies {
        assert_eq!(policy.errors, expected_errors);
    }

    for method in ["Recover", "Cancel", "InspectOperation", "WatchOperation"] {
        let policy = policies
            .iter()
            .find(|policy| policy.rpc.ends_with(&format!("MachinesService/{method}")))
            .unwrap_or_else(|| panic!("missing MachinesService/{method} policy"));
        assert_eq!(policy.capabilities, &["machines.operations"]);
    }

    let watch = MACHINES.services[0]
        .methods
        .iter()
        .find(|method| method.name == "WatchOperation")
        .expect("MachinesService.WatchOperation");
    assert!(!watch.client_streaming);
    assert!(watch.server_streaming);
}
