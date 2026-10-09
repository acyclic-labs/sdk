#![allow(
    clippy::unwrap_used,
    reason = "Fixture construction and peer assertions must fail the test immediately"
)]
//! Focused production boundary tests; service/runtime qualification is separate.
use super::*;

fn options() -> WorkersClientOptions {
    serde_json::from_str(r#"{"endpoint":"https://localhost:443/prefix/","token":"test"}"#).unwrap()
}

#[test]
fn configuration_defaults_and_limits_are_rust_owned() {
    let value = options().validate().unwrap();
    assert_eq!(value.maximum, 16 * 1024 * 1024);
    assert_eq!(value.endpoint, "https://localhost:443/prefix");
    assert_eq!(value.deadline, None);
    for maximum in [1, 8 * 1024 * 1024, 16 * 1024 * 1024] {
        let mut value = options();
        value.maximum_message_bytes = Some(maximum);
        assert_eq!(value.validate().unwrap().maximum, maximum as usize);
    }
    for maximum in [0, 16 * 1024 * 1024 + 1, u32::MAX] {
        let mut value = options();
        value.maximum_message_bytes = Some(maximum);
        assert_eq!(value.validate().err().unwrap().code, "invalid_argument");
    }
    for deadline in [0, i32::MAX as u32 + 1, u32::MAX] {
        assert!(validate_deadline(Some(deadline)).is_err());
    }
    assert_eq!(
        validate_deadline(Some(i32::MAX as u32)).unwrap(),
        Some(i32::MAX as u32)
    );
    for literal in ["-1", "1.5", "4294967296", "\"1\"", "NaN", "Infinity"] {
        assert!(serde_json::from_str::<WorkersClientOptions>(&format!(
            "{{\"endpoint\":\"https://localhost\",\"token\":\"test\",\"maximumMessageBytes\":{literal}}}"
        )).is_err());
    }
}

#[test]
fn metadata_cannot_replace_authentication_or_protocol_fields() {
    for key in [
        "authorization",
        "grpc-timeout",
        "content-type",
        "te",
        "host",
        "x-grpc-web",
        "a-bin",
        "Uppercase",
    ] {
        assert!(
            append_metadata(
                tonic::metadata::MetadataMap::new(),
                Some(vec![(key.to_owned(), "value".to_owned())])
            )
            .is_err()
        );
    }
    let metadata = append_metadata(
        tonic::metadata::MetadataMap::new(),
        Some(vec![
            ("x-request-id".to_owned(), "first".to_owned()),
            ("x-request-id".to_owned(), "second".to_owned()),
        ]),
    )
    .unwrap();
    assert_eq!(
        metadata
            .get_all("x-request-id")
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
}

#[test]
fn submit_multifault_retains_canonical_admission_precedence() {
    let request = wire::SubmitJobRequest {
        target: Some(wire::JobTarget {
            target: Some(wire::job_target::Target::DeploymentAlias("a".to_owned())),
        }),
        input: Some(wire::Payload {
            source: Some(wire::payload::Source::InlineBytes(vec![
                1;
                crate::MAX_INLINE_BYTES
                    + 1
            ])),
        }),
        // Missing retry and limits must not replace the earlier inline-payload violation.
        idempotency_key: "i".to_owned(),
        ..Default::default()
    };
    let error = admit::<wire::SubmitJobRequest, domain::SubmitJobRequest>(
        &request.encode_to_vec(),
        crate::validate_submit,
    )
    .unwrap_err();
    assert_eq!(
        error.message,
        crate::ContractError::LimitExceeded.to_string()
    );
}

#[test]
fn production_ingress_preserves_wide_u64_and_required_presence() {
    for value in [0, (1_u64 << 53) + 1, u64::MAX] {
        let request = wire::SubmitJobRequest {
            target: Some(wire::JobTarget {
                target: Some(wire::job_target::Target::DeploymentAlias("a".to_owned())),
            }),
            input: Some(wire::Payload {
                source: Some(wire::payload::Source::InlineBytes(vec![])),
            }),
            limits: Some(wire::JobLimits {
                timeout_millis: u64::MAX,
                memory_bytes: (1_u64 << 53) + 1,
                output_bytes: 1,
            }),
            retry: Some(wire::RetryPolicy {
                max_attempts: 1,
                backoff_millis: value,
            }),
            idempotency_key: "i".to_owned(),
        };
        let admitted = admit::<wire::SubmitJobRequest, domain::SubmitJobRequest>(
            &request.encode_to_vec(),
            crate::validate_submit,
        )
        .unwrap();
        assert_eq!(admitted, request);
    }
    let empty = wire::SubmitJobRequest::default().encode_to_vec();
    assert!(
        admit::<wire::SubmitJobRequest, domain::SubmitJobRequest>(&empty, crate::validate_submit)
            .is_err()
    );
    for value in [1, (1_u64 << 53) + 1, u64::MAX] {
        let request = wire::SelectDeploymentRequest {
            alias: "a".to_owned(),
            version_sha256: vec![1; 32],
            idempotency_key: "i".to_owned(),
            expected_revision: Some(value),
        };
        assert_eq!(
            admit::<wire::SelectDeploymentRequest, domain::SelectDeploymentRequest>(
                &request.encode_to_vec(),
                crate::validate_select
            )
            .unwrap(),
            request
        );
    }
}

#[test]
fn response_optional_presence_and_u64_survive_production_conversion() {
    for version in [
        None,
        Some(wire::CodeVersion {
            sha256: vec![1; 32],
            size_bytes: u64::MAX,
        }),
    ] {
        let value = wire::PublishVersionResponse { version };
        let bytes = response::<wire::PublishVersionResponse, domain::PublishVersionResponse>(
            value.clone(),
            1024,
        )
        .unwrap();
        assert_eq!(
            wire::PublishVersionResponse::decode(bytes.as_slice()).unwrap(),
            value
        );
    }
    for revision in [None, Some(0), Some((1_u64 << 53) + 1), Some(u64::MAX)] {
        let value = wire::InvokeResponse {
            status: 201,
            headers: vec![],
            body: vec![0, 255],
            resolved_sha256: vec![1; 32],
            resolved_revision: revision,
        };
        let bytes =
            response::<wire::InvokeResponse, domain::InvokeResponse>(value.clone(), 1024).unwrap();
        assert_eq!(
            wire::InvokeResponse::decode(bytes.as_slice()).unwrap(),
            value
        );
    }
    let value = wire::InspectJobResponse { job: None };
    assert_eq!(
        response::<wire::InspectJobResponse, domain::InspectJobResponse>(value, 1024).unwrap(),
        Vec::<u8>::new()
    );
}

#[test]
fn status_projection_retains_unknown_codes_and_malformed_raw_details() {
    for (bytes, expected_code) in [
        (
            wire::Error {
                code: 99,
                message: "future 100%: café / %25".to_owned(),
            }
            .encode_to_vec(),
            Some(99),
        ),
        (
            wire::Error {
                code: -7,
                message: "negative future code".to_owned(),
            }
            .encode_to_vec(),
            Some(-7),
        ),
        (
            wire::Error {
                code: 0,
                message: "unspecified detail".to_owned(),
            }
            .encode_to_vec(),
            None,
        ),
        (vec![], None),
        (vec![255, 0], None),
    ] {
        let status = tonic::Status::with_details(
            tonic::Code::PermissionDenied,
            "original 100%: café / %25",
            bytes.clone().into(),
        );
        let projected = Failure::status(&status, 1024);
        assert_eq!(projected.grpc_code, Some(7));
        assert_eq!(projected.message, "original 100%: café / %25");
        assert_eq!(projected.raw_details, bytes);
        assert_eq!(projected.service_code, expected_code);
    }
}

#[tokio::test]
async fn cancellation_never_polls_precancelled_work() {
    let token = CancellationToken::new();
    token.cancel();
    let error = run(
        async {
            panic!("pre-cancelled work must not be polled");
            #[allow(
                unreachable_code,
                reason = "The pending cancellation fixture never produces a response"
            )]
            Ok::<(), Failure>(())
        },
        token,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "cancelled");
}

#[tokio::test]
async fn deadline_drops_the_actual_pending_operation_future() {
    struct Guard(std::sync::Arc<std::sync::atomic::AtomicBool>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let guard = Guard(dropped.clone());
    let error = run(
        async move {
            let _guard = guard;
            std::future::pending::<Result<(), Failure>>().await
        },
        CancellationToken::new(),
        Some(1),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "deadline_exceeded");
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
}

#[derive(Clone, Default)]
struct HeaderStatus(std::sync::Arc<std::sync::Mutex<Vec<String>>>);
impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>> for HeaderStatus {
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: tonic::codegen::http::Request<tonic::body::Body>) -> Self::Future {
        assert_eq!(request.headers().get("x-request-id").unwrap(), "receipt");
        assert!(request.headers().contains_key("grpc-timeout"));
        self.0.lock().unwrap().push(request.uri().path().to_owned());
        let details = wire::Error {
            code: 99,
            message: "future 100%: café / %25".to_owned(),
        }
        .encode_to_vec();
        std::future::ready(Ok(tonic::Status::with_details(
            tonic::Code::PermissionDenied,
            "original 100%: café / %25",
            details.into(),
        )
        .into_http()))
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "Exercise every generated method against one descriptor-bound peer"
)]
async fn every_descriptor_method_reaches_its_generated_rpc_and_keeps_status_details() {
    use sha2::{Digest, Sha256};
    let module = b"x";
    let submit = wire::SubmitJobRequest {
        target: Some(wire::JobTarget {
            target: Some(wire::job_target::Target::DeploymentAlias("a".to_owned())),
        }),
        input: Some(wire::Payload {
            source: Some(wire::payload::Source::InlineBytes(vec![])),
        }),
        limits: Some(wire::JobLimits {
            timeout_millis: u64::MAX,
            memory_bytes: (1_u64 << 53) + 1,
            output_bytes: 1,
        }),
        retry: Some(wire::RetryPolicy {
            max_attempts: 1,
            backoff_millis: u64::MAX,
        }),
        idempotency_key: "i".to_owned(),
    };
    let cases = [
        (
            "PublishVersion",
            wire::PublishVersionRequest {
                javascript_module: module.to_vec(),
                expected_sha256: Sha256::digest(module).to_vec(),
                idempotency_key: "i".to_owned(),
            }
            .encode_to_vec(),
        ),
        (
            "SelectDeployment",
            wire::SelectDeploymentRequest {
                alias: "a".to_owned(),
                version_sha256: vec![1; 32],
                idempotency_key: "i".to_owned(),
                expected_revision: Some(u64::MAX),
            }
            .encode_to_vec(),
        ),
        ("SubmitJob", submit.encode_to_vec()),
        (
            "InspectJob",
            wire::InspectJobRequest {
                job_id: "j".to_owned(),
            }
            .encode_to_vec(),
        ),
        (
            "CancelJob",
            wire::CancelJobRequest {
                job_id: "j".to_owned(),
                idempotency_key: "i".to_owned(),
            }
            .encode_to_vec(),
        ),
        (
            "InvokeVersion",
            wire::InvokeVersionRequest {
                version_sha256: vec![1; 32],
                ..Default::default()
            }
            .encode_to_vec(),
        ),
        (
            "InvokeDeployment",
            wire::InvokeDeploymentRequest {
                alias: "a".to_owned(),
                ..Default::default()
            }
            .encode_to_vec(),
        ),
    ];
    let pool = prost_reflect::DescriptorPool::decode(crate::FILE_DESCRIPTOR_SET).unwrap();
    let service = pool
        .get_service_by_name("acyclic.workers.v1.WorkersService")
        .unwrap();
    let declared = service
        .methods()
        .map(|method| method.name().to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        declared,
        cases.iter().map(|(name, _)| (*name).to_owned()).collect()
    );
    let transport = HeaderStatus::default();
    let metadata = append_metadata(
        tonic::metadata::MetadataMap::new(),
        Some(vec![("x-request-id".to_owned(), "receipt".to_owned())]),
    )
    .unwrap();
    for (name, bytes) in cases {
        let client = wire::workers_service_client::WorkersServiceClient::new(transport.clone());
        let error = dispatch(
            client,
            name,
            &bytes,
            16 * 1024 * 1024,
            Some(1000),
            &metadata,
        )
        .await
        .unwrap_err();
        assert_eq!(error.grpc_code, Some(7));
        assert_eq!(error.service_code, Some(99));
        assert_eq!(error.message, "original 100%: café / %25");
        assert_eq!(
            error.raw_details,
            wire::Error {
                code: 99,
                message: "future 100%: café / %25".to_owned()
            }
            .encode_to_vec()
        );
        assert_eq!(
            transport.0.lock().unwrap().last().unwrap(),
            &format!("/acyclic.workers.v1.WorkersService/{name}")
        );
    }
    let dispatched = transport.0.lock().unwrap().len();
    for (name, bytes) in [
        (
            "PublishVersion",
            wire::PublishVersionRequest::default().encode_to_vec(),
        ),
        (
            "SelectDeployment",
            wire::SelectDeploymentRequest::default().encode_to_vec(),
        ),
        (
            "SubmitJob",
            wire::SubmitJobRequest::default().encode_to_vec(),
        ),
        (
            "InvokeVersion",
            wire::InvokeVersionRequest::default().encode_to_vec(),
        ),
        (
            "InvokeDeployment",
            wire::InvokeDeploymentRequest {
                alias: ".".to_owned(),
                ..Default::default()
            }
            .encode_to_vec(),
        ),
        ("UnknownMethod", vec![]),
    ] {
        let client = wire::workers_service_client::WorkersServiceClient::new(transport.clone());
        assert_eq!(
            dispatch(client, name, &bytes, 16 * 1024 * 1024, None, &metadata)
                .await
                .unwrap_err()
                .code,
            "invalid_argument"
        );
        assert_eq!(transport.0.lock().unwrap().len(), dispatched);
    }
}

#[test]
fn response_conversion_rejects_unknown_enum_and_preserves_optional_result_bytes() {
    for state in [99, -7] {
        let value = wire::InspectJobResponse {
            job: Some(wire::JobObservation {
                state,
                ..Default::default()
            }),
        };
        let failure = response::<wire::InspectJobResponse, domain::InspectJobResponse>(value, 1024)
            .unwrap_err();
        assert_eq!(failure.code, "malformed_response");
        assert!(failure.message.contains(&state.to_string()));
    }
    for result in [
        None,
        Some(wire::JobResult {
            body: vec![0, 255, 128],
        }),
    ] {
        let value = wire::InspectJobResponse {
            job: Some(wire::JobObservation {
                result,
                ..Default::default()
            }),
        };
        let expected = value.clone();
        let encoded =
            response::<wire::InspectJobResponse, domain::InspectJobResponse>(value, 1024).unwrap();
        assert_eq!(
            wire::InspectJobResponse::decode(encoded.as_slice()).unwrap(),
            expected
        );
    }
}
