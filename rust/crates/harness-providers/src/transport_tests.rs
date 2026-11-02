//! Real-socket transport failures: each class is typed, redelivered only when retryable and
//! before response acceptance, and counted exactly.
use super::*;
use acyclic_harness_core::ModelTransportFailure;
use std::sync::atomic::{AtomicUsize, Ordering};
#[path = "../tests/support/transport.rs"]
mod transport;
use transport::{Reply, serve};

/// Replay actual provider bodies through public selection and execution. The
/// local server supplies framing only; the original headers were not captured.
#[tokio::test]
async fn captured_gemini_terminal_and_refusal_keep_original_evidence() -> Result<()> {
    use acyclic_harness_core::{
        AgentId, OperationId,
        context::{CompactionPolicy, ContextPipeline},
        executor::{Executor as _, StockExecutor, TurnInput, usage_page},
        filesystem::MemoryHarnessStorage,
        tool::ToolRegistry,
        wire::ErrorCode,
    };

    const SUCCESS: &str = include_str!("fixtures/gemini-captured-session.sse");
    const REFUSAL: &str = include_str!("fixtures/gemini-captured-refusal.json");
    let (port, accepted) =
        serve(vec![Reply::Full(200, SUCCESS), Reply::Full(400, REFUSAL)]).await?;
    let mut registry = Registry::new(Credentials::explicit(BTreeMap::from([(
        "gemini".into(),
        "local-replay-only".into(),
    )])))?;
    let mut endpoint = registry.catalog()["gemini"].as_ref().clone();
    endpoint.base_url = format!("http://127.0.0.1:{port}");
    registry.register("gemini".into(), endpoint)?;
    let provider = Arc::new(registry.resolve("gemini/gemini-3.8-flash")?);
    let executor = || {
        StockExecutor::new(
            provider.model().clone(),
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_compaction_policy(CompactionPolicy::Disabled)
    };
    let agent = AgentId::new();
    let storage = MemoryHarnessStorage::new(agent, 1_048_576).await?;
    let journal = storage.journal();
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("Replay captured response through the selected provider".into()),
        selected_context: None,
        max_steps: Some(1),
    };
    let output = executor().execute(input.clone(), journal.as_ref()).await?;
    let original: Value = serde_json::from_str(
        SUCCESS
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .last()
            .unwrap(),
    )
    .map_err(|error| invalid(error.to_string()))?;
    assert_eq!(output.metadata["usage"], original["usageMetadata"]);
    assert_eq!(output.metadata["id"], original["responseId"]);
    assert_eq!(output.metadata["finish_reason"], "STOP");
    assert_eq!(output.text, "LIVE_d06f8c94b23148fd9ea588524212ab76");
    assert!(output.metadata.get("provider_request_id").is_none());
    let page = usage_page(journal.as_ref(), input.operation_id, &agent.to_string(), 0).await?;
    assert!(page.next.is_none());
    let completed: Vec<_> = page.items.iter().filter(|item| item.completed).collect();
    assert_eq!(completed.len(), 1);
    let usage = completed[0];
    assert_eq!(usage.tokens.input, Some(7467));
    assert_eq!(usage.tokens.output, Some(31));
    assert_eq!(usage.tokens.cached, None);
    assert_eq!(usage.provider_request_id, None);
    let projected = serde_json::to_value(
        usage
            .estimate(None)
            .map_err(|error| invalid(error.to_string()))?,
    )
    .map_err(|error| invalid(error.to_string()))?;
    assert_eq!(projected["usage"]["tokens"]["input"], 7467);
    assert_eq!(projected["usage"]["tokens"]["output"], 31);
    // A fresh executor consumes the original committed terminal, without a send.
    assert_eq!(executor().execute(input, journal.as_ref()).await?, output);
    assert_eq!(accepted.load(Ordering::SeqCst), 1);

    let failed = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("Replay the captured continuation refusal".into()),
        selected_context: None,
        max_steps: Some(1),
    };
    let error = executor()
        .execute(failed.clone(), journal.as_ref())
        .await
        .unwrap_err();
    match error {
        Error::ModelDiagnostic { code, message, .. } => {
            assert_eq!(code, ErrorCode::Invalid);
            assert!(message.contains("400"), "{message}");
            assert!(
                message.contains("Function call is missing a thought_signature"),
                "{message}"
            );
        }
        error => panic!("original HTTP refusal lost its semantic class: {error}"),
    }
    assert_eq!(accepted.load(Ordering::SeqCst), 2);
    let failed_usage =
        usage_page(journal.as_ref(), failed.operation_id, &agent.to_string(), 0).await?;
    assert!(failed_usage.items.iter().all(|item| !item.completed));
    Ok(())
}

/// Counts every send while delegating to the platform transport, without backoff waits.
struct Counted {
    inner: ReqwestTransport,
    sent: AtomicUsize,
    body: Mutex<Option<([u8; 32], u64)>>,
}

impl Counted {
    fn observe(&self, request: &HttpRequest) {
        self.sent.fetch_add(1, Ordering::SeqCst);
        *self.body.lock().unwrap() = Some((
            *blake3::hash(&request.body).as_bytes(),
            request.body.len() as u64,
        ));
    }

    fn assert_body(&self, metadata: &Value, canonical: [u8; 32]) {
        let observed = acyclic_harness_core::executor::ModelRequestBody::reported(metadata);
        if acyclic_harness_core::model::RESEARCH_CAPTURE_ENABLED {
            let body = observed.expect("original serialized body observation");
            assert_eq!(Some((body.digest, body.bytes)), *self.body.lock().unwrap());
            assert_ne!(
                body.digest, canonical,
                "wire body is distinct from the canonical request"
            );
        } else {
            assert!(observed.is_none());
            assert!(metadata.get("request_body").is_none());
        }
    }
}

impl HttpTransport for Counted {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        self.observe(&request);
        self.inner.send(request)
    }
    // Each failed exchange is the explicit model-redelivery step.
    fn delay(&self, wait: std::time::Duration) -> BoxProviderFuture<'static, Result<()>> {
        crate::fixtures::delay(wait)
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }
    fn supports_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        self.inner.supports_native(work)
    }
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness_core::NativeFutureBundle<'a, HttpResponse>> {
        self.observe(&request);
        self.inner.send_with_native_work(request, work)
    }
    fn delay_with_native_work(
        &self,
        wait: std::time::Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness_core::NativeFutureBundle<'static, ()>> {
        self.inner.delay_with_native_work(wait, work)
    }
}

fn model(base_url: &str) -> Result<(ResolvedModel, Arc<Counted>)> {
    let transport = Arc::new(Counted {
        inner: ReqwestTransport::new()?,
        sent: AtomicUsize::new(0),
        body: Mutex::new(None),
    });
    Ok((
        super::provider_with(Adapter::Chat, base_url, transport.clone()),
        transport,
    ))
}

async fn generate(provider: &ResolvedModel) -> Result<Vec<ModelEvent>> {
    let (prepared, dispatch) = super::request(provider)?;
    provider.generate(&prepared, dispatch).try_collect().await
}

fn failure(error: &Error) -> Option<ModelTransportFailure> {
    match error {
        Error::ModelUnavailable { failure, .. } => Some(*failure),
        _ => None,
    }
}

const ATTEMPTS: usize = 1 + crate::RetrySpec::DEFAULT.attempts as usize;

#[tokio::test]
async fn unreachable_host_is_a_typed_connect_failure_after_bounded_redelivery() -> Result<()> {
    // Port zero cannot name a listener. Releasing an ephemeral port races a
    // different binder and triggers Windows' delayed connection refusal.
    // The live-server cases below are the control for a reachable destination.
    let (provider, transport) = model("http://127.0.0.1:0/v1")?;
    let error = generate(&provider)
        .await
        .expect_err("impossible destination port");
    assert_eq!(
        failure(&error),
        Some(ModelTransportFailure::Connect),
        "{error}"
    );
    assert_eq!(
        error.code(),
        acyclic_harness_core::wire::ErrorCode::Unavailable
    );
    assert!(!error.to_string().contains("127.0.0.1"), "{error}");
    assert!(!error.to_string().contains("scripted-key"));
    assert_eq!(transport.sent.load(Ordering::SeqCst), ATTEMPTS);
    // The token count shares the class and the budget.
    let (prepared, _) = super::request(&provider)?;
    let error = provider
        .count_tokens(&prepared)
        .await
        .expect_err("impossible destination port");
    assert_eq!(
        failure(&error),
        Some(ModelTransportFailure::Connect),
        "{error}"
    );
    assert_eq!(transport.sent.load(Ordering::SeqCst), 2 * ATTEMPTS);
    Ok(())
}

#[tokio::test]
async fn refusal_then_success_redelivers_exactly_once() -> Result<()> {
    for native in [false, true] {
        for first in [
            Reply::Full(503, "busy"),
            Reply::Cut(503, CHAT, 0),
            Reply::Cut(503, CHAT, 12),
        ] {
            let (port, accepted) = serve(vec![first, Reply::Full(200, CHAT)]).await?;
            let (provider, transport) = model(&format!("http://127.0.0.1:{port}/v1"))?;
            let (prepared, dispatch) = super::request(&provider)?;
            if native {
                let work = super::native_tests::work(&acyclic_fs::CancellationToken::new())?;
                let baseline = work.live_allocation_bytes();
                let mut stream = provider
                    .generate_with_native_work(&prepared, dispatch, &work)?
                    .await?;
                let mut completed = false;
                while let Some(event) = stream.next().await {
                    let (event, custody) = event?;
                    if let ModelEvent::Completed { metadata } = &event {
                        completed = true;
                        transport.assert_body(metadata, dispatch.request_digest);
                    }
                    drop((event, custody));
                }
                assert!(completed);
                drop(stream);
                assert_eq!(work.live_allocation_bytes(), baseline);
                let bytes = transport.body.lock().unwrap().unwrap().1 as usize;
                let expected = if acyclic_harness_core::model::RESEARCH_CAPTURE_ENABLED {
                    crate::projection::encoding::hashed_bytes(&provider, bytes)?
                } else {
                    0
                };
                assert_eq!(
                    work.receipt().bytes_hashed,
                    expected,
                    "recovery reuses the original body and hash"
                );
            } else {
                let events: Vec<_> = provider.generate(&prepared, dispatch).try_collect().await?;
                let Some(ModelEvent::Completed { metadata }) = events.last() else {
                    panic!("original Completed event")
                };
                transport.assert_body(metadata, dispatch.request_digest);
            }
            assert_eq!(transport.sent.load(Ordering::SeqCst), 2);
            assert_eq!(accepted.load(Ordering::SeqCst), 2);
        }
    }
    // Negative control: a 400 is final on the first attempt.
    let (port, accepted) = serve(vec![Reply::Full(400, "bad"), Reply::Full(200, CHAT)]).await?;
    let (provider, transport) = model(&format!("http://127.0.0.1:{port}/v1"))?;
    assert!(matches!(
        generate(&provider).await,
        Err(Error::ModelDiagnostic {
            code: acyclic_harness_core::wire::ErrorCode::Invalid,
            custody: None,
            ..
        })
    ));
    assert_eq!(transport.sent.load(Ordering::SeqCst), 1);
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn response_status_keeps_its_original_interruption_before_and_after_output() -> Result<()> {
    // Reuse the existing shared Chat response, cutting before any bytes,
    // inside its first event and after one event. Also lose all response headers
    // after the server reads the request. A second response would let the former
    // before-output redelivery hide the original run's failure.
    let first = CHAT.find("\n\n").map_or(0, |end| end + 2);
    for native in [false, true] {
        // A diagnostic body can lose its producer too. Mutate the status of
        // the original cut response; a final refusal must retain that boundary
        // even when its body ends with the same transport interruption.
        for (status, cut) in [
            (200, None),
            (200, Some(0)),
            (200, Some(12)),
            (200, Some(first + 8)),
            (201, Some(0)),
            (202, Some(first + 8)),
            (400, Some(0)),
            (400, Some(12)),
            (413, Some(0)),
            (413, Some(12)),
        ] {
            let (port, accepted) = serve(vec![
                cut.map_or(Reply::Lost, |cut| Reply::Cut(status, CHAT, cut)),
                Reply::Full(200, CHAT),
            ])
            .await?;
            let (provider, transport) = model(&format!("http://127.0.0.1:{port}/v1"))?;
            let (prepared, dispatch) = super::request(&provider)?;
            let work = super::native_tests::work(&acyclic_fs::CancellationToken::new())?;
            let baseline = work.live_allocation_bytes();
            let mut output = 0;
            let error = if native {
                let mut stream = provider
                    .generate_with_native_work(&prepared, dispatch, &work)?
                    .await?;
                let error = loop {
                    match stream.next().await {
                        Some(Ok(event)) => {
                            output += 1;
                            drop(event);
                        }
                        Some(Err(error)) => break error,
                        None => return Err(invalid("cut stream completed")),
                    }
                };
                assert!(stream.next().await.is_none());
                error
            } else {
                let mut stream = provider.generate(&prepared, dispatch);
                let error = loop {
                    match stream.next().await {
                        Some(Ok(_)) => output += 1,
                        Some(Err(error)) => break error,
                        None => return Err(invalid("cut stream completed")),
                    }
                };
                assert!(stream.next().await.is_none());
                error
            };
            assert_eq!(
                output > 0,
                cut.is_some_and(|cut| cut > first),
                "native={native}, status={status}, cut={cut:?}"
            );
            assert_eq!(
                failure(&error),
                Some(ModelTransportFailure::Interrupted),
                "{error}"
            );
            assert_eq!(transport.sent.load(Ordering::SeqCst), 1);
            assert_eq!(accepted.load(Ordering::SeqCst), 1);
            drop(error);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
        // Read-only counting may repeat an interrupted successful response, but
        // a final refusal cannot become retryable when its diagnostic is cut.
        // The queued valid count detects a lost status on either transport path.
        for status in [400, 413] {
            for cut in [0, 12] {
                let body = "{\"input_tokens\":7}";
                let (port, accepted) =
                    serve(vec![Reply::Cut(status, body, cut), Reply::Full(200, body)]).await?;
                let (provider, transport) = model(&format!("http://127.0.0.1:{port}/v1"))?;
                let (prepared, _) = super::request(&provider)?;
                let work = super::native_tests::work(&acyclic_fs::CancellationToken::new())?;
                let baseline = work.live_allocation_bytes();
                let result = if native {
                    provider
                        .count_tokens_with_native_work(&prepared, &work)?
                        .await
                        .map(drop)
                } else {
                    provider.count_tokens(&prepared).await.map(drop)
                };
                let error = result.expect_err("final count refusal was redelivered");
                assert_eq!(
                    failure(&error),
                    Some(ModelTransportFailure::Interrupted),
                    "native={native}, status={status}, cut={cut}: {error}"
                );
                assert_eq!(transport.sent.load(Ordering::SeqCst), 1);
                assert_eq!(accepted.load(Ordering::SeqCst), 1);
                drop(error);
                assert_eq!(work.live_allocation_bytes(), baseline);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn tls_failure_is_typed_and_never_redelivered() -> Result<()> {
    let (port, accepted) = serve(vec![Reply::Garbage, Reply::Garbage]).await?;
    let (provider, transport) = model(&format!("https://127.0.0.1:{port}/v1"))?;
    let error = generate(&provider).await.expect_err("not TLS");
    assert_eq!(failure(&error), Some(ModelTransportFailure::Tls), "{error}");
    assert_eq!(error.code(), acyclic_harness_core::wire::ErrorCode::Invalid);
    assert_eq!(transport.sent.load(Ordering::SeqCst), 1);
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn refused_socket_is_a_typed_connect_failure() -> Result<()> {
    // Retain ownership of the address without listening. Releasing a listener before
    // connecting lets another process acquire it and changes the failure under test.
    let socket = tokio::net::TcpSocket::new_v4().map_err(|_| invalid("socket"))?;
    socket
        .bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
        .map_err(|_| invalid("bind"))?;
    let port = socket.local_addr().map_err(|_| invalid("addr"))?.port();
    assert_ne!(port, 0);
    let response = ReqwestTransport::new()?
        .send(HttpRequest {
            url: format!("http://127.0.0.1:{port}/v1"),
            headers: [("authorization".into(), "Bearer scripted-key".into())].into(),
            body: bytes::Bytes::from_static(b"{}"),
        })
        .await;
    let Err(error) = response else {
        return Err(invalid("non-listening socket accepted a request"));
    };
    assert_eq!(
        failure(&error),
        Some(ModelTransportFailure::Connect),
        "{error}"
    );
    assert!(!error.to_string().contains("127.0.0.1"), "{error}");
    assert!(!error.to_string().contains("scripted-key"));
    drop(socket);
    Ok(())
}
