use super::*;
use crate::DESCRIPTOR;
use prost::Message;

// Every connect fixture takes this lock before subscriber setup or connecting.
// Concurrent first registration can overwrite a subscriber's rebuilt interest.
static CONNECT_TRACE_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct WarmFixture(std::sync::Mutex<wire::WarmView>);

#[tonic::async_trait]
impl wire::warm_contexts_service_server::WarmContextsService for Arc<WarmFixture> {
    async fn retain(
        &self,
        _: Request<wire::RetainWarmRequest>,
    ) -> Result<tonic::Response<wire::WarmView>, tonic::Status> {
        self.inspect(Request::new(wire::InspectWarmRequest::default()))
            .await
    }
    async fn renew(
        &self,
        _: Request<wire::RenewWarmRequest>,
    ) -> Result<tonic::Response<wire::WarmView>, tonic::Status> {
        self.inspect(Request::new(wire::InspectWarmRequest::default()))
            .await
    }
    async fn inspect(
        &self,
        _: Request<wire::InspectWarmRequest>,
    ) -> Result<tonic::Response<wire::WarmView>, tonic::Status> {
        self.0
            .lock()
            .map(|view| tonic::Response::new(view.clone()))
            .map_err(|_| tonic::Status::internal("fixture lock poisoned"))
    }
    async fn release(
        &self,
        _: Request<wire::ReleaseWarmRequest>,
    ) -> Result<tonic::Response<wire::WarmView>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture release"))
    }
}

#[tokio::test]
async fn native_warm_operations_reject_substituted_modes_authority_and_policy()
-> Result<(), Box<dyn std::error::Error>> {
    let idle = crate::contract::tests::idle_view();
    let mut latency = idle.clone();
    latency.idle_kv = None;
    latency.latency_profile = vec![6; 32];
    let fixture = Arc::new(WarmFixture(std::sync::Mutex::new(latency.clone())));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(
                wire::warm_contexts_service_server::WarmContextsServiceServer::new(fixture.clone()),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let client = Inference(Arc::new(Connection {
        channel: Endpoint::from_shared(format!("http://{address}"))?
            .connect()
            .await?,
        authorization: authorization("fixture")?,
        client_instance: [7; 16],
    }));
    let revision = [2; 32];
    let commitment = [1; 32];
    for baseline in [latency.clone(), idle.clone()] {
        let is_idle = baseline.idle_kv.is_some();
        let retain = client.retention_request(
            revision,
            if is_idle {
                Retention::idle_kv([6; 32], 10)
            } else {
                Retention::warm_until([6; 32], 110)
            },
        );
        let renew = wire::RenewWarmRequest {
            identity: Some(client.identity()),
            commitment: commitment.to_vec(),
            expires_at_ms: if is_idle { 0 } else { 110 },
            idle_timeout_ms: is_idle.then_some(10),
        };
        for fault in 0..6 {
            let mut response = baseline.clone();
            match fault {
                1 => response.context = vec![9; 32],
                2 => response.commitment = vec![9; 32],
                3 => response.expires_at_ms += 1,
                5 => response.sequence = 0,
                4 => {
                    response = if is_idle {
                        latency.clone()
                    } else {
                        idle.clone()
                    }
                }
                _ => {}
            }
            *fixture.0.lock().map_err(|_| "fixture lock poisoned")? = response;
            assert_eq!(
                client.retain_warm(&retain).await.is_ok(),
                fault == 0 || fault == 2
            );
            assert_eq!(
                client.renew_warm(&renew).await.is_ok(),
                fault == 0 || fault == 1
            );
        }
        let mut response = baseline;
        if let Some(policy) = response
            .idle_kv
            .as_mut()
            .and_then(|idle| idle.policy.as_mut())
        {
            policy.profile = vec![9; 32];
        } else {
            response.latency_profile = vec![9; 32];
        }
        *fixture.0.lock().map_err(|_| "fixture lock poisoned")? = response;
        assert!(client.retain_warm(&retain).await.is_err());
    }
    server.abort();
    let _cancelled = server.await;
    Ok(())
}

fn evaluation_spec() -> wire::EvaluationSpec {
    wire::EvaluationSpec {
        candidates: vec![wire::EvaluationArtifact {
            digest: vec![1; 32],
            media_type: "application/vnd.acyclic.model".to_owned(),
            logical_size: 1024,
        }],
        suite: Some(wire::EvaluationSuite {
            identity: "native-output-binding-v1".to_owned(),
            digest: vec![2; 32],
            cases: vec![wire::EvaluationCase {
                case_id: vec![3; 16],
                input: b"deterministic device input".to_vec(),
                input_artifact_digest: None,
            }],
        }),
        grader: Some(wire::EvaluationGrader {
            handle: b"grader://exact-v1".to_vec(),
            artifact_digest: vec![4; 32],
        }),
        metrics: vec![wire::EvaluationMetric {
            identity: "exact-match".to_owned(),
            aggregation: wire::EvaluationAggregation::Mean.into(),
        }],
        maximum_case_results: 1,
        spec_digest: vec![5; 32],
    }
}

fn completed_evaluation() -> wire::EvaluationView {
    let native_output_digest = [7; 32];
    let grader_observation_digest = [8; 32];
    wire::EvaluationView {
        evaluation_id: vec![6; 16],
        spec: Some(evaluation_spec()),
        state: wire::EvaluationState::Completed.into(),
        result: Some(wire::EvaluationResult {
            spec_digest: vec![5; 32],
            case_results: vec![wire::EvaluationCaseResult {
                candidate_digest: vec![1; 32],
                case_id: vec![3; 16],
                observation: Some(wire::EvaluationGraderObservation {
                    native_output_digest: native_output_digest.to_vec(),
                    observation_digest: grader_observation_digest.to_vec(),
                    binding_digest: contract::evaluation_observation_binding(
                        &native_output_digest,
                        &grader_observation_digest,
                    )
                    .to_vec(),
                }),
                metrics: vec![wire::EvaluationMetricValue {
                    metric_identity: "exact-match".to_owned(),
                    value: Some(wire::ExactRational {
                        numerator: 1,
                        denominator: 1,
                    }),
                }],
                outcome: wire::EvaluationCaseOutcome::Scored.into(),
            }],
            aggregates: vec![wire::EvaluationAggregate {
                candidate_digest: vec![1; 32],
                metric_identity: "exact-match".to_owned(),
                aggregation: wire::EvaluationAggregation::Mean.into(),
                value: Some(wire::ExactRational {
                    numerator: 1,
                    denominator: 1,
                }),
            }],
            result_digest: vec![9; 32],
        }),
        sequence: 2,
    }
}

#[test]
fn descriptor_contains_customer_and_validation_files() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor = prost_types::FileDescriptorSet::decode(DESCRIPTOR)?;
    assert_eq!(descriptor.file.len(), 3);
    let descriptor_names: Vec<_> = descriptor
        .file
        .iter()
        .map(|file| file.name.as_deref())
        .collect();
    assert_eq!(
        descriptor_names,
        [
            Some("google/protobuf/descriptor.proto"),
            Some("validation/v1/options.proto"),
            Some("inference/v1/inference.proto"),
        ]
    );
    let file = descriptor
        .file
        .iter()
        .find(|file| file.name.as_deref() == Some("inference/v1/inference.proto"))
        .ok_or("inference descriptor is missing")?;
    assert_eq!(file.package.as_deref(), Some("inference.customer.v1"));
    assert_eq!(
        file.dependency,
        vec!["validation/v1/options.proto".to_owned()]
    );
    let manifest = include_str!("../../Cargo.toml");
    for dependency in ["inference-protocol", "inference-client", "git ="] {
        assert!(
            !manifest.contains(dependency),
            "private/source dependency in customer manifest"
        );
    }
    Ok(())
}

#[test]
fn descriptor_declares_unique_http_routes_for_every_rpc() -> Result<(), Box<dyn std::error::Error>>
{
    let pool = prost_reflect::DescriptorPool::decode(DESCRIPTOR)?;
    let file = pool
        .get_file_by_name("inference/v1/inference.proto")
        .ok_or("inference descriptor is missing")?;
    let extension = pool
        .get_extension_by_name("acyclic.validation.v1.http_path")
        .ok_or("http_path descriptor extension is missing")?;
    let expected = [
        "models/list",
        "contexts/create",
        "contexts/inspect",
        "contexts/mutate",
        "warm/retain",
        "warm/inspect",
        "warm/renew",
        "warm/release",
        "runs/generate",
        "runs/inspect",
        "runs/watch",
        "runs/cancel",
        "evaluations/create",
        "evaluations/inspect",
    ];
    let mut routes = Vec::new();
    for service in file.services() {
        for method in service.methods() {
            let options = method.options();
            assert!(
                options.has_extension(&extension),
                "{}.{} has no http_path",
                service.name(),
                method.name()
            );
            let path = options.get_extension(&extension).into_owned();
            let prost_reflect::Value::String(path) = path else {
                panic!("http_path is not a string");
            };
            assert!(!path.is_empty());
            routes.push(path);
        }
    }
    assert_eq!(routes.len(), expected.len());
    assert_eq!(routes, expected);
    let unique: std::collections::HashSet<_> = routes.iter().collect();
    assert_eq!(unique.len(), routes.len());
    Ok(())
}

#[test]
fn rejects_incomplete_receipts_and_sensitive_credentials() -> Result<(), Error> {
    assert!(validate_receipt(&wire::MutationReceipt::default()).is_err());
    assert!(validate_warm_view(&wire::WarmView::default(), None, None).is_err());
    let valid = wire::WarmView {
        commitment: vec![1; 32],
        context: vec![2; 32],
        model_profile: vec![3; 32],
        latency_profile: vec![4; 32],
        expires_at_ms: 1,
        state: wire::WarmState::Active.into(),
        evidence_digest: vec![5; 32],
        admission_receipt_id: vec![6; 32],
        sequence: 1,
        idle_kv: None,
    };
    validate_warm_view(&valid, Some([2; 32]), Some([1; 32]))?;
    assert!(authorization("").is_err());
    assert!(authorization("line\nbreak").is_err());
    let secret = authorization("secret")?;
    assert_eq!(secret.as_str(), "Bearer secret");
    Ok(())
}

#[test]
fn evaluation_contract_binds_native_output_to_exact_grader_observation() -> Result<(), Error> {
    let view = completed_evaluation();
    validate_evaluation_view(&view, [6; 16])?;
    for fault in 0..4 {
        let mut invalid = view.clone();
        let result = invalid.result.as_mut().unwrap();
        match fault {
            0 | 1 => {
                let observation = result.case_results[0].observation.as_mut().unwrap();
                if fault == 0 {
                    observation.native_output_digest = vec![0; 32];
                } else {
                    observation.observation_digest = vec![9; 32];
                }
            }
            2 => result.case_results[0].metrics.clear(),
            _ => result.aggregates.clear(),
        }
        assert!(validate_evaluation_view(&invalid, [6; 16]).is_err());
    }
    let mut changed_spec = evaluation_spec();
    changed_spec.spec_digest = vec![10; 32];
    assert!(validate_evaluation_admission(&view, [6; 16], &changed_spec).is_err());
    let mut unbounded = evaluation_spec();
    unbounded.maximum_case_results = 2;
    assert!(validate_evaluation_spec(&unbounded).is_err());
    Ok(())
}

#[tokio::test]
async fn request_metadata_marks_the_ephemeral_bearer_copy_sensitive() -> Result<(), Error> {
    let client = Inference(Arc::new(Connection {
        channel: Endpoint::from_static("https://localhost").connect_lazy(),
        authorization: authorization("secret")?,
        client_instance: [1; 16],
    }));
    let request = client.request(())?;
    let bearer = request
        .metadata()
        .get("authorization")
        .ok_or(Error::Invalid("missing authorization"))?;
    assert!(bearer.is_sensitive());
    assert_eq!(client.0.authorization.as_str(), "Bearer secret");
    Ok(())
}

#[tokio::test]
async fn calls_emit_spans_with_status_codes_and_no_content() -> Result<(), Error> {
    let _connect_trace = CONNECT_TRACE_TEST.lock().await;
    use acyclic_grpc_observability::tests::Capture;
    use tracing_subscriber::layer::SubscriberExt as _;

    let seen = Capture::default();
    let _default =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(seen.clone()));
    assert!(
        Inference::connect("http://localhost", "secret", b"pem")
            .await
            .is_err()
    );
    // A peer that closes every connection fails the RPC.
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").map_err(|_| Error::Invalid("bind"))?;
    let address = listener
        .local_addr()
        .map_err(|_| Error::Invalid("address"))?;
    std::thread::spawn(move || listener.incoming().for_each(drop));
    let client = Inference(Arc::new(Connection {
        channel: Endpoint::from_shared(format!("http://{address}"))?.connect_lazy(),
        authorization: authorization("secret")?,
        client_instance: [1; 16],
    }));
    let mut creation = client.context("model");
    creation
        .items
        .push(text_item(wire::ItemKind::Instruction, "prompt text".into()));
    let Err(Error::Observation(status)) = client.create_context(&creation).await else {
        return Err(Error::Invalid("the RPC did not fail"));
    };

    let evidence = seen.0.lock().unwrap();
    let seen = &evidence.records;
    let has = |span: &str, field: &str, value: &str| {
        seen.iter()
            .any(|(s, f, v)| s == span && f == field && v.trim_matches('"') == value)
    };
    assert!(
        has("acyclic.inference.connect", "error.kind", "invalid"),
        "{seen:?}"
    );
    let code = i32::from(status.code()).to_string();
    assert!(has("acyclic.inference.context.create", "rpc.code", &code));
    assert!(has(
        "acyclic.inference.context.create",
        "error.kind",
        "observation"
    ));
    assert!(has("acyclic.inference.context.create", "outcome", "err"));
    for (span, field, value) in seen.iter() {
        assert!(
            !["path", "token", "content", "body", "authorization"].contains(&field.as_str()),
            "{span} records {field}"
        );
        assert!(
            !value.contains("secret") && !value.contains("prompt"),
            "{value}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn connection_rejects_insecure_or_unbounded_trust_configuration() {
    let _connect_trace = CONNECT_TRACE_TEST.lock().await;
    assert!(matches!(
        Inference::connect("http://localhost", "secret", b"certificate").await,
        Err(Error::Invalid("HTTPS is required"))
    ));
    assert!(matches!(
        Inference::connect("https://localhost", "secret", b"").await,
        Err(Error::Invalid(INVALID_CA_CERTIFICATE_LENGTH))
    ));
    let oversized = vec![b'x'; MAXIMUM_CA_CERTIFICATE_BYTES + 1];
    assert!(matches!(
        Inference::connect("https://localhost", "secret", &oversized).await,
        Err(Error::Invalid(INVALID_CA_CERTIFICATE_LENGTH))
    ));
}

#[test]
fn customer_output_destructors_scrub_owned_buffers() -> Result<(), Error> {
    let mut event = wire::RunEvent {
        sequence: 0,
        event: Some(wire::run_event::Event::Output(vec![7; 32])),
    };
    scrub_run_event(&mut event);
    let Some(wire::run_event::Event::Output(bytes)) = event.event.as_ref() else {
        return Err(Error::Invalid("output event is absent"));
    };
    assert!(bytes.iter().all(|byte| *byte == 0));

    let mut result = wire::RunResult {
        output: vec![9; 32],
        context: None,
        terminal: wire::RunTerminal::Completed.into(),
        receipt: None,
    };
    scrub_run_result(&mut result);
    assert!(result.output.iter().all(|byte| *byte == 0));
    Ok(())
}
