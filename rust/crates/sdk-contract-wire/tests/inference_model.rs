use acyclic_sdk_contract_wire::inference::{
    inference_descriptor, inference_raw_options, INFERENCE, INFERENCE_OPTIONS, INFERENCE_ROUTES,
};
use prost::Message;
use prost_types::{field_descriptor_proto, FileDescriptorSet};
use sha2::{Digest, Sha256};

const INFERENCE_GOLDEN_DESCRIPTOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/inference-v1.descriptor.bin"
));
const INFERENCE_DOC_DESCRIPTOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../inference/inference_model_descriptor_docs.bin"
));

struct WireField<'a> {
    number: u32,
    wire_type: u8,
    value: &'a [u8],
    encoded: &'a [u8],
}

fn descriptor_without_source_info(bytes: &[u8]) -> Vec<u8> {
    // Preserve every unknown field, including validation extensions. Only
    // source locations are removed; the semantic validator handles producer
    // image metadata and protobuf field ordering independently.
    let mut output = Vec::with_capacity(bytes.len());
    for field in wire_fields(bytes) {
        if field.number == 1 && field.wire_type == 2 {
            let mut file = Vec::with_capacity(field.value.len());
            for nested in wire_fields(field.value) {
                if nested.number != 9 {
                    file.extend_from_slice(nested.encoded);
                }
            }
            append_length_delimited(&mut output, 1, &file);
        } else {
            output.extend_from_slice(field.encoded);
        }
    }
    output
}

fn wire_fields(mut bytes: &[u8]) -> Vec<WireField<'_>> {
    let mut fields = Vec::new();
    while !bytes.is_empty() {
        let start = bytes;
        let tag = read_varint(&mut bytes).expect("descriptor field tag");
        let number = u32::try_from(tag >> 3).expect("descriptor field number");
        let wire_type = u8::try_from(tag & 7).expect("descriptor wire type");
        let value = match wire_type {
            0 => {
                read_varint(&mut bytes).expect("descriptor varint");
                &[]
            }
            1 => {
                let value = bytes.get(..8).expect("descriptor fixed64");
                bytes = &bytes[8..];
                value
            }
            2 => {
                let length = usize::try_from(read_varint(&mut bytes).expect("descriptor length"))
                    .expect("descriptor length fits usize");
                let value = bytes.get(..length).expect("descriptor payload");
                bytes = &bytes[length..];
                value
            }
            5 => {
                let value = bytes.get(..4).expect("descriptor fixed32");
                bytes = &bytes[4..];
                value
            }
            other => panic!("unsupported descriptor wire type {other}"),
        };
        let encoded_length = start.len() - bytes.len();
        fields.push(WireField {
            number,
            wire_type,
            value,
            encoded: &start[..encoded_length],
        });
    }
    fields
}

fn append_length_delimited(output: &mut Vec<u8>, number: u32, value: &[u8]) {
    write_varint((u64::from(number) << 3) | 2, output);
    write_varint(value.len() as u64, output);
    output.extend_from_slice(value);
}

fn read_varint(bytes: &mut &[u8]) -> Option<u64> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let byte = *bytes.first()?;
        *bytes = &bytes[1..];
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

fn write_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn collect_extension_fields(bytes: &[u8], output: &mut Vec<Vec<u8>>) {
    let mut remaining = bytes;
    while !remaining.is_empty() {
        let start = remaining;
        let Some(tag) = read_varint(&mut remaining) else {
            return;
        };
        let number = u32::try_from(tag >> 3).expect("descriptor field number");
        let wire_type = u8::try_from(tag & 7).expect("descriptor wire type");
        let value = match wire_type {
            0 => {
                if read_varint(&mut remaining).is_none() {
                    return;
                }
                None
            }
            1 => {
                let Some(value) = remaining.get(..8) else {
                    return;
                };
                remaining = &remaining[8..];
                Some(value)
            }
            2 => {
                let Some(length) =
                    read_varint(&mut remaining).and_then(|length| usize::try_from(length).ok())
                else {
                    return;
                };
                let Some(value) = remaining.get(..length) else {
                    return;
                };
                remaining = &remaining[length..];
                Some(value)
            }
            5 => {
                let Some(value) = remaining.get(..4) else {
                    return;
                };
                remaining = &remaining[4..];
                Some(value)
            }
            _ => return,
        };
        if number >= 50_000 {
            output.push(start[..start.len() - remaining.len()].to_vec());
        }
        if wire_type == 2 {
            if let Some(value) = value {
                collect_extension_fields(value, output);
            }
        }
    }
}

#[test]
fn inference_model_matches_compatibility_descriptor_semantics() {
    let normalized_archive = descriptor_without_source_info(INFERENCE_GOLDEN_DESCRIPTOR);
    let report = acyclic_sdk_contract_validation::compare_bytes(
        &normalized_archive,
        &inference_descriptor(),
    )
    .expect("compare Inference descriptor compatibility");
    assert!(
        report.semantic_compatible,
        "Inference descriptor drift: {:?}",
        report.differences
    );
    // Keep the immutable archived handshake fixture itself under review even
    // though compiler source locations, image metadata, and protobuf field
    // ordering are intentionally ignored by the semantic comparison.
    let digest = Sha256::digest(INFERENCE_GOLDEN_DESCRIPTOR);
    assert_eq!(
        format!("{digest:x}"),
        "21c35707beb7d3aa8c87f63ceb129083ad092010a64d9b9e82924a0f5661bf15"
    );
}

#[test]
fn inference_model_preserves_every_extension_wire_payload() {
    let mut archived = Vec::new();
    collect_extension_fields(INFERENCE_GOLDEN_DESCRIPTOR, &mut archived);
    let mut emitted = Vec::new();
    let generated = inference_descriptor();
    collect_extension_fields(&generated, &mut emitted);

    assert!(
        !archived.is_empty(),
        "archived Inference extensions missing"
    );
    assert_eq!(
        archived, emitted,
        "Rust Inference generation dropped or rewrote an extension option payload"
    );
}

#[test]
fn inference_wire_inventory_and_routes_are_complete() {
    assert_eq!(INFERENCE.file_name, "inference/v1/inference.proto");
    assert_eq!(INFERENCE.package, "inference.customer.v1");
    assert_eq!(INFERENCE.dependencies, &["validation/v1/options.proto"]);
    assert_eq!(INFERENCE.messages.len(), 56);
    assert_eq!(INFERENCE.enums.len(), 6);
    assert_eq!(INFERENCE.services.len(), 5);
    assert_eq!(INFERENCE_ROUTES.len(), 14);

    let method_count: usize = INFERENCE
        .services
        .iter()
        .map(|service| service.methods.len())
        .sum();
    assert_eq!(method_count, INFERENCE_ROUTES.len());

    for route in INFERENCE_ROUTES {
        let (qualified_service_name, method_name) = route
            .rpc
            .rsplit_once('/')
            .expect("qualified Inference route RPC");
        let service_name = qualified_service_name
            .rsplit_once('.')
            .map(|(_, name)| name)
            .unwrap_or(qualified_service_name);
        let method = INFERENCE
            .services
            .iter()
            .find(|service| service.name == service_name)
            .expect("route service exists in Inference services")
            .methods
            .iter()
            .find(|method| method.name == method_name)
            .expect("route RPC exists in Inference services");
        assert_eq!(route.request, method.input);
        assert_eq!(route.response, method.output);
        assert_eq!(route.docs, method.docs);
    }

    let watch = INFERENCE.services[3]
        .methods
        .iter()
        .find(|method| method.name == "Watch")
        .expect("RunsService.Watch");
    assert!(watch.server_streaming);
}

#[test]
fn inference_custom_options_are_retained_and_typed() {
    assert_eq!(INFERENCE_OPTIONS.len(), 138);
    for assignment in INFERENCE_OPTIONS {
        let options = inference_raw_options(assignment.subject);
        assert!(
            options
                .fields()
                .iter()
                .any(|field| field.spec.name == assignment.name),
            "missing {} on {}",
            assignment.name,
            assignment.subject
        );
    }
}

#[test]
fn inference_model_preserves_signed_and_optional_wire_fields() {
    let descriptor = FileDescriptorSet::decode(inference_descriptor().as_slice())
        .expect("decode Inference descriptor");
    let file = descriptor
        .file
        .iter()
        .find(|file| file.name.as_deref() == Some("inference/v1/inference.proto"))
        .expect("Inference file descriptor");

    let rational = file
        .message_type
        .iter()
        .find(|message| message.name.as_deref() == Some("ExactRational"))
        .expect("ExactRational descriptor");
    let numerator = rational
        .field
        .iter()
        .find(|field| field.name.as_deref() == Some("numerator"))
        .expect("ExactRational.numerator");
    assert_eq!(
        numerator.r#type,
        Some(field_descriptor_proto::Type::Sint64 as i32)
    );

    let run_view = file
        .message_type
        .iter()
        .find(|message| message.name.as_deref() == Some("RunView"))
        .expect("RunView descriptor");
    let result = run_view
        .field
        .iter()
        .find(|field| field.name.as_deref() == Some("result"))
        .expect("RunView.result");
    assert_eq!(result.number, Some(6));
    assert_eq!(result.proto3_optional, Some(true));
    assert_eq!(result.oneof_index, Some(0));
}

#[test]
fn inference_product_descriptor_overlay_is_source_info_only() {
    let canonical = FileDescriptorSet::decode(
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../inference/inference_model_descriptor.bin"
        ))
        .as_slice(),
    )
    .expect("decode Inference model descriptor");
    let overlaid = FileDescriptorSet::decode(INFERENCE_DOC_DESCRIPTOR)
        .expect("decode Inference documentation descriptor");
    let canonical_file = canonical
        .file
        .iter()
        .find(|file| file.package.as_deref() == Some("inference.customer.v1"))
        .expect("Inference canonical file");
    let overlaid_file = overlaid
        .file
        .iter()
        .find(|file| file.package.as_deref() == Some("inference.customer.v1"))
        .expect("Inference overlaid file");
    assert!(canonical_file.source_code_info.is_none());
    let locations = overlaid_file
        .source_code_info
        .as_ref()
        .expect("Inference source docs overlay")
        .location
        .as_slice();
    assert!(locations.iter().any(|location| {
        location
            .leading_comments
            .as_deref()
            .is_some_and(|text| text.contains("model capabilities"))
    }));
    assert_eq!(canonical_file.name, overlaid_file.name);
    assert_eq!(canonical_file.message_type, overlaid_file.message_type);
    assert_eq!(canonical_file.service, overlaid_file.service);
}

#[test]
fn inference_policy_metadata_binds_capability_discovery_and_client_errors() {
    let policies = INFERENCE.operation_policies();
    assert_eq!(policies.len(), 14);

    let models = policies
        .iter()
        .find(|policy| policy.rpc.ends_with("ModelsService/List"))
        .expect("ModelsService/List policy");
    assert_eq!(models.capabilities, &["inference.models.read"]);
    assert_eq!(
        models.validations,
        &["model_capabilities.bounded", "retention_profiles.valid"]
    );

    let expected_errors = [
        "inference.invalid",
        "inference.transport",
        "inference.observation",
    ];
    for policy in policies {
        assert_eq!(policy.errors, expected_errors);
    }

    let generate = policies
        .iter()
        .find(|policy| policy.rpc.ends_with("RunsService/Generate"))
        .expect("RunsService/Generate policy");
    assert_eq!(
        generate.capabilities,
        &["inference.runs.write", "inference.idempotent_mutation"]
    );
}
