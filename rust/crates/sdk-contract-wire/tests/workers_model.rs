use acyclic_sdk_contract_validation::compare_bytes;
use acyclic_sdk_contract_wire::workers::WORKERS;
use acyclic_sdk_contract_wire::workers::{WORKERS_ROUTES, workers_descriptor};
use prost::Message;
use prost_types::FileDescriptorSet;
use sha2::{Digest, Sha256};

const WORKERS_GOLDEN_DESCRIPTOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/workers-v1.descriptor.bin"
));

#[test]
fn workers_model_matches_archived_wire_descriptor() {
    let emitted = workers_descriptor();
    let report = compare_bytes(WORKERS_GOLDEN_DESCRIPTOR, &emitted)
        .expect("compare Workers descriptor semantics");
    assert!(
        report.semantic_compatible,
        "Workers descriptor semantics drifted: {:?}",
        report.differences
    );

    // Source locations are generator provenance, not wire contract semantics.
    // Normalize only that field for the typed model assertion; the independent
    // raw validator above still compares every option and unknown extension.
    let mut expected = FileDescriptorSet::decode(WORKERS_GOLDEN_DESCRIPTOR)
        .expect("decode Workers golden descriptor");
    let mut actual =
        FileDescriptorSet::decode(emitted.as_slice()).expect("decode emitted Workers descriptor");
    for file in &mut expected.file {
        file.source_code_info = None;
    }
    for file in &mut actual.file {
        file.source_code_info = None;
    }
    assert_eq!(
        actual, expected,
        "Workers typed descriptor semantics drifted"
    );

    let file = actual.file.first().expect("Workers file descriptor");
    assert_eq!(file.name.as_deref(), Some(WORKERS.file_name));
    assert_eq!(file.package.as_deref(), Some(WORKERS.package));
    assert_eq!(file.syntax.as_deref(), Some(WORKERS.syntax));
    assert_eq!(file.message_type.len(), 24);
    assert_eq!(file.enum_type.len(), 2);
    assert_eq!(file.service.len(), 1);
    assert_eq!(file.service[0].name.as_deref(), Some("WorkersService"));
    assert_eq!(file.service[0].method.len(), 7);

    let object_ref = file
        .message_type
        .iter()
        .find(|message| message.name.as_deref() == Some("ObjectRef"))
        .expect("ObjectRef descriptor");
    assert_eq!(object_ref.reserved_name, vec!["version_id"]);
    assert_eq!(object_ref.reserved_range[0].start, Some(3));
    assert_eq!(object_ref.reserved_range[0].end, Some(4));

    let payload = file
        .message_type
        .iter()
        .find(|message| message.name.as_deref() == Some("Payload"))
        .expect("Payload descriptor");
    assert_eq!(payload.oneof_decl.len(), 1);
    assert_eq!(payload.oneof_decl[0].name.as_deref(), Some("source"));
    assert!(
        payload
            .field
            .iter()
            .all(|field| field.oneof_index == Some(0))
    );

    let select = file
        .message_type
        .iter()
        .find(|message| message.name.as_deref() == Some("SelectDeploymentRequest"))
        .expect("SelectDeploymentRequest descriptor");
    let expected_revision = select
        .field
        .iter()
        .find(|field| field.name.as_deref() == Some("expected_revision"))
        .expect("expected_revision descriptor");
    assert_eq!(expected_revision.number, Some(3));
    assert_eq!(expected_revision.proto3_optional, Some(true));
    assert_eq!(
        expected_revision.json_name.as_deref(),
        Some("expectedRevision")
    );

    let expected_routes = [
        ("publishVersion", "/v1/workers/versions/publish"),
        ("selectDeployment", "/v1/workers/deployments/select"),
        ("submitJob", "/v1/workers/jobs/submit"),
        ("inspectJob", "/v1/workers/jobs/inspect"),
        ("cancelJob", "/v1/workers/jobs/cancel"),
        ("invokeVersion", "/v1/workers/versions/{sha256hex}/invoke"),
        ("invokeDeployment", "/v1/workers/deployments/{alias}/invoke"),
    ];
    assert_eq!(WORKERS_ROUTES.len(), expected_routes.len());
    assert_eq!(WORKERS_ROUTES.len(), WORKERS.services[0].methods.len());
    for (route, (operation_id, path)) in WORKERS_ROUTES.iter().zip(expected_routes) {
        assert_eq!(route.method, "POST");
        assert_eq!(route.operation_id, operation_id);
        assert_eq!(route.path, path);
        let method = WORKERS.services[0]
            .methods
            .iter()
            .find(|method| route.rpc.ends_with(method.name))
            .expect("route RPC exists in Workers service");
        assert_eq!(route.request, method.input);
        assert_eq!(route.response, method.output);
        assert_eq!(route.docs, method.docs);
    }
}

#[test]
fn workers_fixture_bytes_and_runtime_identity_are_immutable() {
    assert_eq!(WORKERS_GOLDEN_DESCRIPTOR.len(), 10_799);
    assert_eq!(
        format!("{:x}", Sha256::digest(WORKERS_GOLDEN_DESCRIPTOR)),
        "851b6cd37b8cb4baa6d3a111efdad655b89936b2e1057ecb74e62825715bd7d8"
    );

    let method_names = WORKERS.services[0]
        .methods
        .iter()
        .map(|method| method.name)
        .collect::<Vec<_>>();
    assert_eq!(
        method_names,
        vec![
            "PublishVersion",
            "SelectDeployment",
            "SubmitJob",
            "InspectJob",
            "CancelJob",
            "InvokeVersion",
            "InvokeDeployment",
        ]
    );
    assert!(
        WORKERS_ROUTES
            .iter()
            .all(|route| route.rpc.starts_with("acyclic.workers.v1.WorkersService/"))
    );
}
