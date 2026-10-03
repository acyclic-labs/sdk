use acyclic_sdk_contract_validation::compare_bytes;
use acyclic_sdk_contract_wire::{
    actors_descriptor, family_view, filesystem::filesystem_descriptor,
    filesystem::filesystem_file_descriptor, filesystem::FILESYSTEM, harness::harness_descriptor,
    harness::harness_file_descriptor, harness::HARNESS, inference::inference_descriptor,
    machines::machines_descriptor, objects::objects_descriptor, stream::stream_descriptor,
    workers::workers_descriptor, ContractSpec, FamilyModel, HttpProjection, ACTORS, FAMILY_VIEWS,
    INFERENCE, MACHINES, OBJECTS_V2, STREAM, WORKERS,
};
use prost::Message;
use prost_types::{field_descriptor_proto, FileDescriptorSet};
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
    for contract in [
        &ACTORS,
        &STREAM,
        &WORKERS,
        &OBJECTS_V2,
        &INFERENCE,
        &MACHINES,
    ] {
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

    for (name, contract, method_count) in [
        (
            "filesystem",
            FILESYSTEM.operation_policies(),
            filesystem_file_descriptor()
                .service
                .iter()
                .map(|service| service.method.len())
                .sum::<usize>(),
        ),
        (
            "harness",
            HARNESS.operation_policies(),
            harness_file_descriptor()
                .service
                .iter()
                .map(|service| service.method.len())
                .sum::<usize>(),
        ),
    ] {
        assert_eq!(contract.len(), method_count, "{name} policy count drifted");
        assert!(contract.iter().all(|policy| {
            !policy.capabilities.is_empty()
                && !policy.errors.is_empty()
                && !policy.validations.is_empty()
        }));
    }
}

#[test]
fn registry_policies_cover_every_descriptor_method_and_protocol_is_dependency_only() {
    for view in FAMILY_VIEWS {
        let descriptor = FileDescriptorSet::decode(view.model.descriptor().as_slice())
            .expect("registered family descriptor");
        let descriptor_rpcs = descriptor
            .file
            .iter()
            .flat_map(|file| {
                let package = file.package.as_deref().unwrap_or_default();
                file.service.iter().flat_map(move |service| {
                    let service_name = service.name.as_deref().unwrap_or_default();
                    service.method.iter().map(move |method| {
                        format!(
                            "{package}.{service_name}/{}",
                            method.name.as_deref().unwrap_or_default()
                        )
                    })
                })
            })
            .collect::<BTreeSet<_>>();
        let policy_rpcs = view
            .operation_policies
            .iter()
            .map(|policy| policy.rpc.to_owned())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            policy_rpcs, descriptor_rpcs,
            "{} policy and descriptor RPC coverage drifted",
            view.name
        );
        if matches!(
            view.model,
            FamilyModel::Filesystem(_) | FamilyModel::Harness(_)
        ) {
            assert!(
                descriptor.file.iter().any(|file| file.name.as_deref()
                    == Some(acyclic_sdk_contract_wire::protocol::FILE_NAME)),
                "{} must retain the Protocol dependency in its descriptor closure",
                view.name
            );
        }
    }
}

#[test]
fn unified_family_registry_covers_all_models_and_transport_projections() {
    let expected = [
        ("actors", "acyclic.actors.v1"),
        ("workers", "acyclic.workers.v1"),
        ("objects", "acyclic.objects.v2"),
        ("stream", "acyclic.stream.v2"),
        ("inference", "inference.customer.v1"),
        ("machines", "acyclic.machines.v1"),
        ("filesystem", "acyclic.filesystem.v2"),
        ("harness", "acyclic.harness.v2"),
    ];
    assert_eq!(FAMILY_VIEWS.len(), expected.len());
    for (family, package) in expected {
        let view = family_view(family).expect("family is registered");
        assert_eq!(view.package(), package);
        assert!(!view.file_name().is_empty());
        assert!(!view.model.syntax().is_empty());
        assert!(!view.model.descriptor().is_empty());
    }

    for view in FAMILY_VIEWS {
        match view.name {
            "actors" | "workers" | "objects" | "stream" | "inference" => {
                assert!(
                    matches!(view.http, HttpProjection::Explicit(routes) if !routes.is_empty())
                );
                assert!(view.has_http_projection());
                assert!(matches!(view.model, FamilyModel::ContractSpec(_)));
            }
            "machines" | "filesystem" | "harness" => {
                assert!(matches!(view.http, HttpProjection::Unavailable));
                assert!(!view.has_http_projection());
                assert!(!view.operation_policies.is_empty());
            }
            other => panic!("unrecognized registered family: {other}"),
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
