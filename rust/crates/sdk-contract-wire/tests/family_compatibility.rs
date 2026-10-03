use acyclic_sdk_contract_validation::compare_bytes;
use acyclic_sdk_contract_wire::{
    ACTORS, ContractSpec, INFERENCE, MACHINES, OBJECTS_V2, STREAM, WORKERS, actors_descriptor,
    filesystem::filesystem_descriptor, harness::harness_descriptor,
    inference::inference_descriptor, machines::machines_descriptor, objects::objects_descriptor,
    stream::stream_descriptor, workers::workers_descriptor,
};
use prost::Message;
use prost_types::{FileDescriptorSet, field_descriptor_proto};
use std::collections::BTreeSet;

fn fixture(path: &str) -> &'static [u8] {
    match path {
        "actors" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/actors-v1.descriptor.bin"
        )),
        "stream" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/stream-v2.descriptor.bin"
        )),
        "objects" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/objects-v2.descriptor.bin"
        )),
        "workers" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/workers-v1.descriptor.bin"
        )),
        "filesystem" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/filesystem-v2.descriptor.bin"
        )),
        "harness" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/harness-v2.descriptor.bin"
        )),
        "inference" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/inference-v1.descriptor.bin"
        )),
        "machines" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/machines-v1.descriptor.bin"
        )),
        _ => panic!("unknown family fixture {path}"),
    }
}

fn without_source_info(bytes: &[u8]) -> FileDescriptorSet {
    let mut set = FileDescriptorSet::decode(bytes).expect("descriptor set");
    for file in &mut set.file {
        file.source_code_info = None;
    }
    set
}

fn assert_routes_cover_services(contract: &ContractSpec) {
    let method_count: usize = contract
        .services
        .iter()
        .map(|service| service.methods.len())
        .sum();
    assert_eq!(
        contract.routes.len(),
        method_count,
        "{} route count does not cover every modeled RPC",
        contract.package
    );

    let mut operation_ids = BTreeSet::new();
    for route in contract.routes {
        assert!(
            !route.method.is_empty(),
            "{} route method is empty",
            contract.package
        );
        assert!(
            !route.path.is_empty(),
            "{} route path is empty",
            contract.package
        );
        assert!(
            !route.operation_id.is_empty(),
            "{} route operation ID is empty",
            contract.package
        );
        assert!(
            !route.docs.is_empty(),
            "{} route docs are empty",
            contract.package
        );
        assert!(
            operation_ids.insert(route.operation_id),
            "{} route operation ID is duplicated: {}",
            contract.package,
            route.operation_id
        );

        let (qualified_service, method_name) = route
            .rpc
            .rsplit_once('/')
            .expect("route RPC has service and method");
        let service_name = qualified_service
            .rsplit_once('.')
            .map(|(_, name)| name)
            .expect("route RPC has qualified service");
        let service = contract
            .services
            .iter()
            .find(|service| service.name == service_name)
            .unwrap_or_else(|| panic!("{} route service missing: {}", contract.package, route.rpc));
        let method = service
            .methods
            .iter()
            .find(|method| method.name == method_name)
            .unwrap_or_else(|| panic!("{} route method missing: {}", contract.package, route.rpc));
        assert_eq!(route.request, method.input, "{} request drifted", route.rpc);
        assert_eq!(
            route.response, method.output,
            "{} response drifted",
            route.rpc
        );
        assert_eq!(route.docs, method.docs, "{} docs drifted", route.rpc);
    }
}

#[test]
fn routed_contracts_cover_every_rpc_with_explicit_projection_metadata() {
    for contract in [&ACTORS, &STREAM, &WORKERS, &OBJECTS_V2, &INFERENCE] {
        assert_routes_cover_services(contract);
    }
    assert!(
        MACHINES.routes.is_empty(),
        "Machines must not gain an implicit HTTP projection"
    );
}

#[test]
fn modeled_operation_policies_cover_each_policy_backed_rpc() {
    for contract in [&ACTORS, &STREAM, &WORKERS] {
        let policies = contract.operation_policies();
        let method_count: usize = contract
            .services
            .iter()
            .map(|service| service.methods.len())
            .sum();
        assert_eq!(
            policies.len(),
            method_count,
            "{} policy count does not cover every modeled RPC",
            contract.package
        );
        let mut rpcs = BTreeSet::new();
        for policy in policies {
            assert!(
                rpcs.insert(policy.rpc),
                "duplicate policy RPC: {}",
                policy.rpc
            );
            assert!(
                !policy.capabilities.is_empty(),
                "{} has no capabilities",
                policy.rpc
            );
            assert!(!policy.errors.is_empty(), "{} has no errors", policy.rpc);
            assert!(
                !policy.validations.is_empty(),
                "{} has no validations",
                policy.rpc
            );
            let (qualified_service, method_name) = policy
                .rpc
                .rsplit_once('/')
                .expect("policy RPC has service and method");
            let service_name = qualified_service
                .rsplit_once('.')
                .map(|(_, name)| name)
                .expect("policy RPC has qualified service");
            let service = contract
                .services
                .iter()
                .find(|service| service.name == service_name)
                .expect("policy service exists");
            assert!(
                service
                    .methods
                    .iter()
                    .any(|method| method.name == method_name),
                "policy RPC is not a modeled method: {}",
                policy.rpc
            );
        }
    }
}

#[test]
fn every_emitted_family_matches_its_immutable_descriptor_semantics() {
    let cases = [
        ("actors", actors_descriptor()),
        ("stream", stream_descriptor()),
        ("objects", objects_descriptor()),
        ("workers", workers_descriptor()),
        ("filesystem", filesystem_descriptor()),
        ("harness", harness_descriptor()),
        ("inference", inference_descriptor()),
        ("machines", machines_descriptor()),
    ];
    for (name, emitted) in cases {
        assert_eq!(
            without_source_info(&emitted),
            without_source_info(fixture(name)),
            "{name} descriptor semantics drifted"
        );
    }
}

#[test]
fn emitted_descriptors_preserve_presence_json_names_and_enum_alias_policy() {
    let cases = [
        ("actors", actors_descriptor()),
        ("stream", stream_descriptor()),
        ("objects", objects_descriptor()),
        ("workers", workers_descriptor()),
        ("filesystem", filesystem_descriptor()),
        ("harness", harness_descriptor()),
        ("inference", inference_descriptor()),
        ("machines", machines_descriptor()),
    ];
    for (name, bytes) in cases {
        let set = FileDescriptorSet::decode(bytes.as_slice()).expect("emitted descriptor");
        for file in &set.file {
            for enum_ in &file.enum_type {
                let mut numbers = enum_.value.iter().map(|value| value.number);
                let mut seen = Vec::new();
                while let Some(number) = numbers.next() {
                    if seen.contains(&number) {
                        assert_eq!(
                            enum_
                                .options
                                .as_ref()
                                .and_then(|options| options.allow_alias),
                            Some(true),
                            "{name} enum {} has an undeclared alias",
                            enum_.name.as_deref().unwrap_or_default()
                        );
                    }
                    seen.push(number);
                }
            }
            for message in &file.message_type {
                for field in &message.field {
                    assert!(field.json_name.is_some(), "{name} field lacks json_name");
                    if field.proto3_optional == Some(true) {
                        let oneof_index =
                            field.oneof_index.expect("synthetic oneof index") as usize;
                        assert!(
                            message.oneof_decl[oneof_index]
                                .name
                                .as_deref()
                                .unwrap_or_default()
                                .starts_with('_'),
                            "{name} optional field has non-synthetic oneof"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn raw_option_extensions_survive_independent_semantic_validation() {
    let cases = [
        ("actors", actors_descriptor()),
        ("stream", stream_descriptor()),
        ("objects", objects_descriptor()),
        ("workers", workers_descriptor()),
        ("filesystem", filesystem_descriptor()),
        ("harness", harness_descriptor()),
        ("inference", inference_descriptor()),
        ("machines", machines_descriptor()),
    ];
    for (name, emitted) in cases {
        let report = compare_bytes(fixture(name), &emitted)
            .unwrap_or_else(|error| panic!("{name} descriptor comparison: {error:?}"));
        assert!(
            report.semantic_compatible,
            "{name} raw descriptor options drifted: {:?}",
            report.differences
        );
    }
}

#[test]
fn inference_signed_fields_and_semantic_edits_are_wire_distinct() {
    let canonical = inference_descriptor();
    let mut set = FileDescriptorSet::decode(canonical.as_slice()).expect("Inference descriptor");
    let file = set
        .file
        .iter_mut()
        .find(|file| file.name.as_deref() == Some("inference/v1/inference.proto"))
        .expect("Inference file");
    let rational = file
        .message_type
        .iter_mut()
        .find(|message| message.name.as_deref() == Some("ExactRational"))
        .expect("ExactRational message");
    let numerator = rational
        .field
        .iter_mut()
        .find(|field| field.name.as_deref() == Some("numerator"))
        .expect("signed numerator");
    assert_eq!(
        numerator.r#type,
        Some(field_descriptor_proto::Type::Sint64 as i32)
    );
    assert_eq!(numerator.json_name.as_deref(), Some("numerator"));
    numerator.r#type = Some(field_descriptor_proto::Type::Int64 as i32);
    assert_ne!(set.encode_to_vec(), canonical);
}
