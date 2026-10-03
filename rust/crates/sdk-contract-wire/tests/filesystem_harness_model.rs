use acyclic_sdk_contract_options::{
    OptionSpec, OptionTarget, RawOptionError, RawOptionValue, RawOptions, option_spec,
};
use acyclic_sdk_contract_wire::{filesystem, harness};
use prost::Message;
use prost_types::FileDescriptorSet;

#[test]
fn rust_models_match_the_source_info_free_migration_oracles() {
    let filesystem_fixture = include_bytes!("fixtures/filesystem-v2.descriptor.bin");
    let filesystem_set = FileDescriptorSet::decode(filesystem_fixture.as_slice()).unwrap();
    let mut filesystem_file = filesystem_set
        .file
        .into_iter()
        .find(|file| file.name.as_deref() == Some(filesystem::FILE_NAME))
        .unwrap();
    filesystem_file.source_code_info = None;
    assert_eq!(filesystem::filesystem_file_descriptor(), filesystem_file);

    let harness_fixture = include_bytes!("fixtures/harness-v2.descriptor.bin");
    let harness_set = FileDescriptorSet::decode(harness_fixture.as_slice()).unwrap();
    let mut harness_file = harness_set
        .file
        .into_iter()
        .find(|file| file.name.as_deref() == Some(harness::FILE_NAME))
        .unwrap();
    harness_file.source_code_info = None;
    assert_eq!(harness::harness_file_descriptor(), harness_file);
}

#[test]
fn descriptor_mutation_is_detected_without_mutating_fixtures() {
    let mut changed = filesystem::filesystem_file_descriptor();
    changed.message_type[0].field[0].number = Some(999);
    let changed = FileDescriptorSet {
        file: vec![changed],
    }
    .encode_to_vec();
    assert_ne!(changed, filesystem::filesystem_descriptor());

    let mut changed = harness::harness_file_descriptor();
    let previous_streaming = changed.service[0].method[0]
        .server_streaming
        .unwrap_or(false);
    changed.service[0].method[0].server_streaming = Some(!previous_streaming);
    let changed = FileDescriptorSet {
        file: vec![changed],
    }
    .encode_to_vec();
    assert_ne!(changed, harness::harness_descriptor());
}

#[test]
fn map_and_presence_shape_survives_the_rust_model() {
    let harness_file = harness::harness_file_descriptor();
    let message = harness_file
        .message_type
        .iter()
        .find(|message| message.name.as_deref() == Some("ConversationMessage"))
        .unwrap();
    let extensions = message
        .field
        .iter()
        .find(|field| field.name.as_deref() == Some("extensions"))
        .unwrap();
    assert_eq!(
        extensions.label,
        Some(prost_types::field_descriptor_proto::Label::Repeated as i32)
    );
    let map_name = extensions
        .type_name
        .as_deref()
        .unwrap()
        .rsplit('.')
        .next()
        .unwrap();
    assert!(message.nested_type.iter().any(|nested| {
        nested.name.as_deref() == Some(map_name)
            && nested
                .options
                .as_ref()
                .and_then(|options| options.map_entry)
                == Some(true)
    }));

    let filesystem_file = filesystem::filesystem_file_descriptor();
    assert!(filesystem_file.message_type.iter().any(|message| {
        message
            .oneof_decl
            .iter()
            .any(|oneof| oneof.name.as_deref().unwrap_or_default().starts_with('_'))
    }));
    assert!(
        filesystem_file
            .message_type
            .iter()
            .flat_map(|message| message.field.iter())
            .any(|field| field.proto3_optional == Some(true))
    );
    assert!(filesystem::filesystem_proto().contains("service FilesystemService"));
    assert!(harness::harness_proto().contains("map<string, "));
}

#[test]
fn custom_option_mutations_are_rejected_by_the_typed_exporter() {
    let known = option_spec(51001).unwrap();
    let mut options = RawOptions::new();
    options.try_push(known, RawOptionValue::U32(16)).unwrap();
    assert_eq!(
        options.try_push(known, RawOptionValue::U32(32)),
        Err(RawOptionError::Duplicate { number: 51001 })
    );

    let mut wrong_type = RawOptions::new();
    assert_eq!(
        wrong_type.try_push(known, RawOptionValue::Bool(true)),
        Err(RawOptionError::TypeMismatch {
            name: "nonzero_fixed_bytes",
            scalar_type: "uint32"
        })
    );

    let mut unknown = RawOptions::new();
    let unknown_spec = OptionSpec {
        target: OptionTarget::Field,
        name: "fabricated",
        number: 51999,
        scalar_type: "uint32",
    };
    assert_eq!(
        unknown.try_push(unknown_spec, RawOptionValue::U32(1)),
        Err(RawOptionError::UnknownSpec { number: 51999 })
    );
}

#[test]
fn filesystem_branch_snapshot_and_cancel_capabilities_are_wire_pinned() {
    let file = filesystem::filesystem_file_descriptor();
    let names: Vec<_> = file.service[0]
        .method
        .iter()
        .filter_map(|method| method.name.as_deref())
        .collect();
    for required in [
        "ForkWorkspace",
        "Diff",
        "PlanJoin",
        "ApplyJoin",
        "Checkpoint",
        "Pin",
        "Observe",
        "Cancel",
    ] {
        assert!(
            names.contains(&required),
            "missing Filesystem capability {required}"
        );
    }
    for message_name in [
        "WorkspaceContextSnapshot",
        "FileRecordSnapshot",
        "TreeEntrySnapshot",
    ] {
        assert!(
            file.message_type
                .iter()
                .any(|message| message.name.as_deref() == Some(message_name))
        );
    }
}

#[test]
fn harness_recovery_snapshot_and_cancel_capabilities_are_wire_pinned() {
    let file = harness::harness_file_descriptor();
    let names: Vec<_> = file.service[0]
        .method
        .iter()
        .filter_map(|method| method.name.as_deref())
        .collect();
    for required in ["Handshake", "Submit", "Replay", "Observe", "Cancel"] {
        assert!(
            names.contains(&required),
            "missing Harness capability {required}"
        );
    }
    assert!(
        file.message_type
            .iter()
            .any(|message| message.name.as_deref() == Some("SnapshotEnvelope"))
    );
    assert!(
        file.message_type
            .iter()
            .any(|message| message.name.as_deref() == Some("OperationStatus"))
    );
}
