//! Pins the shipped Machines protocol and the tombstones for retired fields.

#![allow(
    clippy::unwrap_used,
    reason = "missing or malformed checked-in descriptors must fail conformance"
)]

use acyclic_machines::{
    FILE_DESCRIPTOR_SET, MAX_EVENT_PAGE_SIZE, MAX_OUTPUT_READ_BYTES, MAX_PAGE_SIZE,
};
use prost::Message;
use prost_types::{
    DescriptorProto, FileDescriptorProto, FileDescriptorSet,
    field_descriptor_proto::{Label, Type},
};

const PACKAGE: &str = ".acyclic.machines.v1.";

fn descriptor() -> FileDescriptorProto {
    FileDescriptorSet::decode(FILE_DESCRIPTOR_SET)
        .unwrap()
        .file
        .into_iter()
        .find(|file| file.package.as_deref() == Some("acyclic.machines.v1"))
        .unwrap()
}

fn message<'a>(file: &'a FileDescriptorProto, name: &str) -> &'a DescriptorProto {
    file.message_type
        .iter()
        .find(|message| message.name.as_deref() == Some(name))
        .unwrap()
}

fn shape(file: &FileDescriptorProto, name: &str, expected: &[(&str, i32, Type, &str)]) {
    let descriptor = message(file, name);
    let actual: Vec<_> = descriptor
        .field
        .iter()
        .map(|field| {
            let kind = field.type_name.as_deref().unwrap_or("");
            (
                field.name.as_deref().unwrap_or(""),
                field.number.unwrap_or_default(),
                field.r#type.unwrap_or_default(),
                kind.strip_prefix(PACKAGE).unwrap_or(kind),
            )
        })
        .collect();
    let expected: Vec<_> = expected
        .iter()
        .map(|&(name, tag, kind, reference)| (name, tag, kind as i32, reference))
        .collect();
    assert_eq!(actual, expected, "{name}");
    for field in &descriptor.field {
        let repeated = matches!(
            (name, field.name.as_deref()),
            ("ExecRequest", Some("argv")) | ("MachineContract", Some("capabilities"))
        );
        assert_eq!(
            field.label,
            Some(if repeated {
                Label::Repeated
            } else {
                Label::Optional
            } as i32)
        );
        let output = name == "ExecOutput" && field.name.as_deref() != Some("machine");
        assert_eq!(field.oneof_index, output.then_some(0));
        assert!(!field.proto3_optional.unwrap_or(false));
    }
}

#[test]
fn lifecycle_field_tags_types_and_cardinality_are_pinned() {
    let file = descriptor();
    assert_eq!(file.syntax.as_deref(), Some("proto3"));
    for name in ["MachineId", "OperationId", "IdempotencyKey", "CheckpointId"] {
        shape(&file, name, &[("value", 1, Type::Bytes, "")]);
    }
    shape(
        &file,
        "ProtocolVersion",
        &[
            ("major", 1, Type::Uint32, ""),
            ("minor", 2, Type::Uint32, ""),
        ],
    );
    shape(
        &file,
        "CreateMachineRequest",
        &[
            ("protocol", 1, Type::Message, "ProtocolVersion"),
            ("idempotency_key", 2, Type::Message, "IdempotencyKey"),
            ("image", 3, Type::Message, "Image"),
            ("compatibility", 4, Type::Message, "CompatibilityPolicy"),
            ("suspension", 6, Type::Message, "SuspensionPolicy"),
            ("expiration", 7, Type::Message, "ExpirationPolicy"),
            ("network_policy_digest", 8, Type::Bytes, ""),
            ("budgets", 9, Type::Message, "Budgets"),
        ],
    );
    shape(
        &file,
        "MachineContract",
        &[
            ("image", 1, Type::Message, "Image"),
            ("capabilities", 2, Type::Enum, "Capability"),
            ("compatibility", 3, Type::Message, "CompatibilityPolicy"),
            ("compatibility_revision", 4, Type::Bytes, ""),
            ("suspension", 6, Type::Message, "SuspensionPolicy"),
            ("expiration", 7, Type::Message, "ExpirationPolicy"),
            ("network_policy_digest", 8, Type::Bytes, ""),
            ("budgets", 9, Type::Message, "Budgets"),
        ],
    );
    shape(
        &file,
        "MachineAdmission",
        &[
            ("machine", 1, Type::Message, "MachineId"),
            ("operation", 2, Type::Message, "OperationId"),
            ("contract", 3, Type::Message, "MachineContract"),
        ],
    );
    shape(
        &file,
        "MachineMutationRequest",
        &[
            ("protocol", 1, Type::Message, "ProtocolVersion"),
            ("idempotency_key", 2, Type::Message, "IdempotencyKey"),
            ("machine", 3, Type::Message, "MachineId"),
        ],
    );
}

#[test]
fn guest_io_field_tags_types_and_cardinality_are_pinned() {
    let file = descriptor();
    shape(
        &file,
        "ExecRequest",
        &[
            ("protocol", 1, Type::Message, "ProtocolVersion"),
            ("machine", 2, Type::Message, "MachineId"),
            ("argv", 3, Type::String, ""),
            ("timeout_ms", 4, Type::Uint32, ""),
        ],
    );
    shape(
        &file,
        "ExecOutput",
        &[
            ("machine", 1, Type::Message, "MachineId"),
            ("data", 2, Type::Bytes, ""),
            ("exit_code", 3, Type::Int32, ""),
        ],
    );
    assert_eq!(message(&file, "ExecOutput").oneof_decl.len(), 1);
    assert_eq!(
        message(&file, "ExecOutput")
            .oneof_decl
            .first()
            .unwrap()
            .name
            .as_deref(),
        Some("output")
    );
    shape(
        &file,
        "ReadOutputRequest",
        &[
            ("protocol", 1, Type::Message, "ProtocolVersion"),
            ("machine", 2, Type::Message, "MachineId"),
            ("offset", 3, Type::Uint64, ""),
            ("limit", 4, Type::Uint32, ""),
        ],
    );
    shape(
        &file,
        "MachineOutput",
        &[
            ("machine", 1, Type::Message, "MachineId"),
            ("offset", 2, Type::Uint64, ""),
            ("data", 3, Type::Bytes, ""),
        ],
    );
    assert_eq!(
        (MAX_PAGE_SIZE, MAX_EVENT_PAGE_SIZE, MAX_OUTPUT_READ_BYTES),
        (256, 1024, 65536)
    );
}

#[test]
fn retired_performance_and_lineage_fields_remain_reserved() {
    let file = descriptor();
    for (name, retired, tag) in [
        ("CreateMachineRequest", "performance", 5),
        ("MachineContract", "performance", 5),
        ("ForkCheckpointRequest", "performance", 5),
        ("UsageReceipt", "lineage_shared_bytes", 8),
    ] {
        let descriptor = message(&file, name);
        assert_eq!(descriptor.reserved_name, [retired]);
        let ranges: Vec<_> = descriptor
            .reserved_range
            .iter()
            .map(|range| (range.start, range.end))
            .collect();
        assert_eq!(ranges, [(Some(tag), Some(tag + 1))]);
        assert!(
            descriptor
                .field
                .iter()
                .all(|field| field.number != Some(tag) && field.name.as_deref() != Some(retired))
        );
    }
    assert!(
        file.enum_type
            .iter()
            .all(|kind| kind.name.as_deref() != Some("PerformanceProfile"))
    );
    let receipt = message(&file, "UsageReceipt");
    let commitment = receipt
        .field
        .iter()
        .find(|field| field.name.as_deref() == Some("lineage_receipt_sha256"))
        .unwrap();
    assert_eq!(commitment.number, Some(11));
    assert_eq!(commitment.r#type, Some(Type::Bytes as i32));
}

const METHODS: [(&str, &str, &str, bool); 21] = [
    ("Exec", "ExecRequest", "ExecOutput", true),
    ("ReadOutput", "ReadOutputRequest", "MachineOutput", false),
    (
        "QualifyImage",
        "QualifyImageRequest",
        "ImageQualification",
        false,
    ),
    ("Create", "CreateMachineRequest", "MachineAdmission", false),
    (
        "Checkpoint",
        "CheckpointMachineRequest",
        "CheckpointAdmission",
        false,
    ),
    ("Fork", "ForkCheckpointRequest", "ForkAdmission", false),
    (
        "ForkMachine",
        "ForkMachineRequest",
        "ForkMachineAdmission",
        false,
    ),
    (
        "Suspend",
        "MachineMutationRequest",
        "MutationAdmission",
        false,
    ),
    ("Wake", "MachineMutationRequest", "MutationAdmission", false),
    (
        "SetSuspensionPolicy",
        "SetSuspensionPolicyRequest",
        "PolicyAdmission",
        false,
    ),
    (
        "DestroyMachine",
        "MachineMutationRequest",
        "MutationAdmission",
        false,
    ),
    (
        "DestroyCheckpoint",
        "CheckpointMutationRequest",
        "MutationAdmission",
        false,
    ),
    ("Recover", "RecoverRequest", "RecoveredAdmission", false),
    (
        "InspectMachine",
        "InspectMachineRequest",
        "MachineState",
        false,
    ),
    (
        "InspectCheckpoint",
        "InspectCheckpointRequest",
        "CheckpointState",
        false,
    ),
    ("ListMachines", "ListMachinesRequest", "MachinePage", false),
    ("Events", "EventsRequest", "EventPage", false),
    ("Usage", "UsageRequest", "UsageReceipt", false),
    ("Cancel", "OperationRequest", "OperationState", false),
    (
        "InspectOperation",
        "OperationRequest",
        "OperationState",
        false,
    ),
    ("WatchOperation", "OperationRequest", "OperationState", true),
];

#[test]
fn machines_service_admissions_and_streaming_shape_are_pinned() {
    let file = descriptor();
    assert_eq!(file.service.len(), 1);
    let service = file.service.first().unwrap();
    assert_eq!(service.name.as_deref(), Some("MachinesService"));

    assert_eq!(service.method.len(), METHODS.len());
    for (method, (name, input, output, streaming)) in service.method.iter().zip(METHODS) {
        assert_eq!(method.name.as_deref(), Some(name));
        assert_eq!(
            method.input_type.as_deref(),
            Some(format!("{PACKAGE}{input}").as_str())
        );
        assert_eq!(
            method.output_type.as_deref(),
            Some(format!("{PACKAGE}{output}").as_str())
        );
        assert!(!method.client_streaming.unwrap_or(false));
        assert_eq!(method.server_streaming.unwrap_or(false), streaming);
    }
}

#[test]
fn typescript_limits_binary_exports_only_live_rust_limits() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_machines-limits"))
        .output()
        .unwrap();
    assert!(output.status.success());
    let actual: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        actual,
        serde_json::json!({
            "maxPageSize": MAX_PAGE_SIZE,
            "maxEventPageSize": MAX_EVENT_PAGE_SIZE,
            "maxOutputReadBytes": MAX_OUTPUT_READ_BYTES,
        })
    );
}

fn golden<M: Message + Default + PartialEq + std::fmt::Debug>(message: &M, encoded: &str) {
    let bytes = hex::decode(encoded).unwrap();
    assert_eq!(message.encode_to_vec(), bytes);
    assert_eq!(&M::decode(bytes.as_slice()).unwrap(), message);
}

fn protocol() -> acyclic_machines::wire::ProtocolVersion {
    acyclic_machines::wire::ProtocolVersion {
        major: acyclic_machines::PROTOCOL_MAJOR,
        minor: acyclic_machines::PROTOCOL_MINOR,
    }
}

fn machine() -> acyclic_machines::wire::MachineId {
    acyclic_machines::wire::MachineId { value: vec![1; 16] }
}

#[test]
fn create_and_destroy_keep_their_golden_wire_encodings() {
    use acyclic_machines::wire;

    golden(
        &wire::CreateMachineRequest {
            protocol: Some(protocol()),
            idempotency_key: Some(wire::IdempotencyKey { value: vec![2; 16] }),
            image: Some(wire::Image {
                kind: wire::ImageKind::Custom as i32,
                immutable_reference: Some(wire::image::ImmutableReference::CustomDigest(vec![
                    3;
                    32
                ])),
            }),
            compatibility: Some(wire::CompatibilityPolicy {
                mode: wire::CompatibilityMode::BestEffort as i32,
                required: vec![],
            }),
            suspension: Some(wire::SuspensionPolicy {
                policy: Some(wire::suspension_policy::Policy::Manual(true)),
            }),
            expiration: Some(wire::ExpirationPolicy {
                kind: wire::ExpirationKind::Never as i32,
                value_ms: 0,
            }),
            network_policy_digest: vec![4; 32],
            budgets: Some(wire::Budgets {
                spend_micros: 300,
                concurrency: 2,
            }),
        },
        concat!(
            "0a0408011001",
            "12120a1002020202020202020202020202020202",
            "1a2408021a20",
            "03030303030303030303030303030303",
            "03030303030303030303030303030303",
            "22020801320208013a0208014220",
            "04040404040404040404040404040404",
            "04040404040404040404040404040404",
            "4a0508ac021002",
        ),
    );
    golden(
        &wire::MachineMutationRequest {
            protocol: Some(protocol()),
            idempotency_key: Some(wire::IdempotencyKey { value: vec![2; 16] }),
            machine: Some(machine()),
        },
        concat!(
            "0a0408011001",
            "12120a1002020202020202020202020202020202",
            "1a120a1001010101010101010101010101010101",
        ),
    );
}

#[test]
fn exec_preserves_argv_boundaries_data_and_a_zero_exit_frame() {
    use acyclic_machines::wire::{self, exec_output::Output};

    golden(
        &wire::ExecRequest {
            protocol: Some(protocol()),
            machine: Some(machine()),
            argv: vec!["echo".into(), "hi".into()],
            timeout_ms: 1_000,
        },
        concat!(
            "0a0408011001",
            "12120a1001010101010101010101010101010101",
            "1a046563686f1a02686920e807",
        ),
    );
    for (output, suffix) in [
        (Output::Data(b"hi\n".to_vec()), "120368690a"),
        (Output::ExitCode(0), "1800"),
        (Output::ExitCode(-1), "18ffffffffffffffffff01"),
    ] {
        golden(
            &wire::ExecOutput {
                machine: Some(machine()),
                output: Some(output),
            },
            &format!("0a120a1001010101010101010101010101010101{suffix}"),
        );
    }
}

#[test]
fn console_cursors_keep_64_bits_and_output_remains_binary() {
    use acyclic_machines::wire;

    golden(
        &wire::ReadOutputRequest {
            protocol: Some(protocol()),
            machine: Some(machine()),
            offset: 4_294_967_297,
            limit: acyclic_machines::MAX_OUTPUT_READ_BYTES,
        },
        concat!(
            "0a0408011001",
            "12120a1001010101010101010101010101010101",
            "18818080801020808004",
        ),
    );
    golden(
        &wire::MachineOutput {
            machine: Some(machine()),
            offset: 4_294_967_297,
            data: b"hi\r\n\xff".to_vec(),
        },
        concat!(
            "0a120a1001010101010101010101010101010101",
            "1081808080101a0568690d0aff",
        ),
    );
}
