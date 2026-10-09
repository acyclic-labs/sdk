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
        // Protify sorts top-level declarations by name. Declaration order is
        // not wire identity; retain every declaration's complete contents.
        file.message_type.sort_by(|a, b| a.name.cmp(&b.name));
        file.enum_type.sort_by(|a, b| a.name.cmp(&b.name));
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

#[test]
fn every_request_ingress_uses_the_real_admission_hook() {
    use sha2::{Digest, Sha256};
    let module = b"export default {}";
    let mut publish = wire::PublishVersionRequest {
        javascript_module: module.to_vec().into(),
        expected_sha256: Sha256::digest(module).to_vec().into(),
        idempotency_key: "publish".into(),
    };
    assert!(domain::PublishVersionRequest::try_from(publish.clone()).is_ok());
    publish.expected_sha256 = vec![1; 32].into();
    assert!(matches!(
        domain::PublishVersionRequest::try_from(publish),
        Err(domain::DomainError::Contract(
            acyclic_workers::ContractError::DigestMismatch
        ))
    ));
    let submit = wire::SubmitJobRequest {
        target: Some(wire::JobTarget {
            target: Some(wire::job_target::Target::DeploymentAlias("current".into())),
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
            max_attempts: 0,
            backoff_millis: 0,
        }),
        idempotency_key: "submit".into(),
    };
    assert!(matches!(
        domain::SubmitJobRequest::try_from(submit),
        Err(domain::DomainError::Contract(
            acyclic_workers::ContractError::InvalidArgument
        ))
    ));
    let raw = wire::Error {
        code: 991,
        message: "original".into(),
    };
    let decoded = wire::Error::decode(raw.encode_to_vec().as_slice()).expect("raw error");
    assert_eq!(decoded.code, 991);
    assert!(matches!(
        domain::Error::try_from(decoded),
        Err(domain::DomainError::UnknownErrorCode(991))
    ));
}

#[test]
fn submit_requires_every_message_and_both_selectors() {
    let request = wire::SubmitJobRequest {
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
    for field in 0..4 {
        let mut absent = request.clone();
        match field {
            0 => absent.target = None,
            1 => absent.input = None,
            2 => absent.limits = None,
            _ => absent.retry = None,
        }
        assert_eq!(
            acyclic_workers::validate_submit(&absent),
            Err(acyclic_workers::ContractError::InvalidArgument)
        );
        assert!(domain::SubmitJobRequest::try_from(absent).is_err());
    }
    assert!(domain::JobTarget::try_from(wire::JobTarget { target: None }).is_err());
    assert!(domain::Payload::try_from(wire::Payload { source: None }).is_err());
    let mut invalid = request;
    invalid.limits.as_mut().expect("limits").output_bytes = 0;
    assert!(matches!(
        domain::SubmitJobRequest::try_from(invalid),
        Err(domain::DomainError::Contract(
            acyclic_workers::ContractError::InvalidArgument
        ))
    ));
}

#[test]
fn publish_preserves_ordered_error_classes() {
    let empty = wire::PublishVersionRequest {
        javascript_module: Vec::new().into(),
        expected_sha256: vec![1; 32].into(),
        idempotency_key: "publish".into(),
    };
    assert!(matches!(
        domain::PublishVersionRequest::try_from(empty),
        Err(domain::DomainError::Contract(
            acyclic_workers::ContractError::InvalidArgument
        ))
    ));
    let oversized = wire::PublishVersionRequest {
        javascript_module: vec![0; acyclic_workers::MAX_MODULE_BYTES + 1].into(),
        expected_sha256: vec![0; 32].into(),
        idempotency_key: "publish".into(),
    };
    assert!(matches!(
        domain::PublishVersionRequest::try_from(oversized),
        Err(domain::DomainError::Contract(
            acyclic_workers::ContractError::LimitExceeded
        ))
    ));
}
