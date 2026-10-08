//! Admission and wire identity gates for the Rust-owned Workers contract.
use acyclic_workers::{domain, wire};
use prost::Message;
use prost_reflect::DescriptorPool;

#[test]
fn preserves_descriptor_contract_without_source_locations() {
    let old = DescriptorPool::decode(include_bytes!("fixtures/workers-v1.bin").as_slice())
        .expect("baseline");
    let current =
        DescriptorPool::decode(acyclic_workers::FILE_DESCRIPTOR_SET).expect("Rust descriptor");
    let normalize = |pool: &DescriptorPool| {
        let mut file = pool
            .get_file_by_name("workers/v1/workers.proto")
            .expect("file")
            .file_descriptor_proto()
            .clone();
        file.source_code_info = None;
        file
    };
    assert_eq!(normalize(&old), normalize(&current));
}

#[test]
fn semantic_ingress_preserves_presence_and_existing_admission() {
    let request = wire::SelectDeploymentRequest {
        alias: "A_1".into(),
        version_sha256: vec![1; 32].into(),
        expected_revision: None,
        idempotency_key: "select".into(),
    };
    let typed =
        domain::SelectDeploymentRequest::try_from(request.clone()).expect("absent revision");
    let restored: wire::SelectDeploymentRequest = typed.into();
    assert_eq!(restored.encode_to_vec(), request.encode_to_vec());
    let mut invalid = request.clone();
    invalid.expected_revision = Some(0);
    assert!(matches!(
        domain::SelectDeploymentRequest::try_from(invalid),
        Err(domain::DomainError::Contract(
            acyclic_workers::ContractError::InvalidArgument
        ))
    ));
    let mut maximum = request;
    maximum.expected_revision = Some(u64::MAX);
    assert!(domain::SelectDeploymentRequest::try_from(maximum).is_ok());
    let payload = wire::Payload {
        source: Some(wire::payload::Source::InlineBytes(Vec::new().into())),
    };
    assert!(domain::Payload::try_from(payload).is_ok());
    assert!(domain::Payload::try_from(wire::Payload { source: None }).is_err());
}

#[test]
fn unknown_enum_is_observable_and_raw_wire_remains_lossless() {
    let request = wire::JobObservation {
        state: 991,
        ..Default::default()
    };
    let restored = wire::JobObservation::decode(request.encode_to_vec().as_slice()).expect("wire");
    assert_eq!(restored.state, 991);
    assert!(matches!(
        domain::JobObservation::try_from(restored),
        Err(domain::DomainError::UnknownJobState(991))
    ));
}

#[test]
fn empty_inline_and_permitted_object_spelling_preserve_admission() {
    let mut request = wire::SubmitJobRequest {
        target: Some(wire::JobTarget {
            target: Some(wire::job_target::Target::DeploymentAlias("A_1".into())),
        }),
        input: Some(wire::Payload {
            source: Some(wire::payload::Source::InlineBytes(Vec::new().into())),
        }),
        limits: Some(wire::JobLimits {
            timeout_millis: 1,
            memory_bytes: 1,
            output_bytes: 1,
        }),
        retry: Some(wire::RetryPolicy {
            max_attempts: 1,
            backoff_millis: 0,
        }),
        idempotency_key: "submit".into(),
    };
    let typed =
        domain::SubmitJobRequest::try_from(request.clone()).expect("empty inline is present");
    let restored: wire::SubmitJobRequest = typed.into();
    assert_eq!(restored.encode_to_vec(), request.encode_to_vec());
    request.input = Some(wire::Payload {
        source: Some(wire::payload::Source::Object(wire::ObjectRef {
            bucket: " ".into(),
            key: "nested/\u{e9}".into(),
        })),
    });
    assert!(domain::SubmitJobRequest::try_from(request.clone()).is_ok());
    request.retry = None;
    assert!(domain::SubmitJobRequest::try_from(request).is_err());
}
