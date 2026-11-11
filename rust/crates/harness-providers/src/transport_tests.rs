//! Real-socket transport failures: each class is typed, redelivered only when retryable and
//! before response acceptance, and counted exactly.
use super::*;
use acyclic_harness_core::ModelTransportFailure;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// One scripted connection.
enum Reply {
    /// A complete response with this status and body.
    Full(u16, &'static str),
    /// A complete response with provider-authored recovery headers.
    Headers(u16, &'static str, &'static str),
    /// A response declaring the whole body but sending only `usize` bytes before closing.
    Cut(u16, &'static str, usize),
    /// The server reads the request, then closes without returning headers.
    Lost,
    /// Bytes that are not a TLS record.
    Garbage,
}

/// Counts real exchanges and records recovery timers, optionally held on an event.
struct Counted {
    inner: ReqwestTransport,
    sent: AtomicUsize,
    waits: Mutex<Vec<std::time::Duration>>,
    gate: Option<Arc<WaitGate>>,
}

struct WaitGate {
    entered: Mutex<Option<tokio::sync::oneshot::Sender<std::time::Duration>>>,
    release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl Counted {
    fn wait(
        &self,
        wait: std::time::Duration,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static + use<> {
        self.waits.lock().unwrap().push(wait);
        let gate = self.gate.clone();
        async move {
            if let Some(gate) = gate {
                let release = gate
                    .release
                    .lock()
                    .unwrap()
                    .take()
                    .expect("one recovery wait");
                gate.entered
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(wait)
                    .unwrap();
                release
                    .await
                    .map_err(|_| invalid("recovery timer producer stopped"))?;
            }
            Ok(())
        }
    }
}

impl HttpTransport for Counted {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        self.sent.fetch_add(1, Ordering::SeqCst);
        self.inner.send(request)
    }
    // Each failed exchange is the explicit model-redelivery step.
    fn delay(&self, wait: std::time::Duration) -> BoxProviderFuture<'static, Result<()>> {
        Box::pin(self.wait(wait))
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }
    fn unix_ms(&self) -> Result<u64> {
        Ok(1_700_000_000_000)
    }
    fn supports_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        self.inner.supports_native(work)
    }
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeFutureBundle<'a, HttpResponse, acyclic_harness_core::Error>> {
        self.sent.fetch_add(1, Ordering::SeqCst);
        self.inner.send_with_native_work(request, work)
    }
    fn delay_with_native_work(
        &self,
        wait: std::time::Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeFutureBundle<'static, (), acyclic_harness_core::Error>> {
        acyclic_fs::NativeFutureBundle::new(work, || self.wait(wait))
    }
}

/// Serves `replies` in order, one per connection, and returns the port and the accept count.
async fn serve(replies: Vec<Reply>) -> Result<(u16, Arc<AtomicUsize>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|_| invalid("bind"))?;
    let port = listener.local_addr().map_err(|_| invalid("addr"))?.port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = accepted.clone();
    let mut replies: VecDeque<_> = replies.into();
    tokio::spawn(async move {
        while let Some(reply) = replies.pop_front() {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            if matches!(reply, Reply::Garbage) {
                // Take the ClientHello, so the answer is read rather than reset away.
                let mut hello = [0; 4096];
                let _ = socket.read(&mut hello).await;
            } else {
                read_request(&mut socket).await;
            }
            let bytes = match reply {
                Reply::Full(status, body) => format!(
                    "HTTP/1.1 {status} S\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                ),
                Reply::Headers(status, headers, body) => format!(
                    "HTTP/1.1 {status} S\r\n{headers}content-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                ),
                Reply::Cut(status, body, sent) => format!(
                    "HTTP/1.1 {status} S\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    &body[..sent]
                ),
                Reply::Garbage => "this is not a TLS record\r\n\r\n".repeat(4),
                Reply::Lost => String::new(),
            };
            let _ = socket.write_all(bytes.as_bytes()).await;
            let _ = socket.flush().await;
            // Wait for the client to hang up, so no unread bytes turn the close into a reset;
            // closing short of the declared length cuts the body mid-stream.
            let _ = socket.shutdown().await;
            let mut rest = [0; 4096];
            while matches!(socket.read(&mut rest).await, Ok(read) if read > 0) {}
            drop(socket);
        }
    });
    Ok((port, accepted))
}

/// Reads one request's headers and its declared body.
async fn read_request(socket: &mut tokio::net::TcpStream) {
    let mut buffer = Vec::new();
    let mut chunk = [0; 4096];
    loop {
        let Ok(read) = socket.read(&mut chunk).await else {
            return;
        };
        if read == 0 {
            return;
        }
        buffer.extend_from_slice(&chunk[..read]);
        let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&buffer[..end]).to_ascii_lowercase();
        let length = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(0);
        if buffer.len() >= end + 4 + length {
            return;
        }
    }
}

fn model(base_url: &str) -> Result<(ResolvedModel, Arc<Counted>)> {
    let transport = Arc::new(Counted {
        inner: ReqwestTransport::new()?,
        sent: AtomicUsize::new(0),
        waits: Mutex::new(Vec::new()),
        gate: None,
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

#[tokio::test]
async fn retry_after_429_is_visible_until_the_exact_wait_finishes_then_resumes_the_turn()
-> Result<()> {
    use std::time::Duration;
    use tracing::instrument::WithSubscriber as _;
    use tracing_subscriber::prelude::*;

    #[derive(Clone)]
    struct Log(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Log {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    for native in [false, true] {
        for (status, headers, reason, seconds) in [
            (
                429,
                "Retry-After: 60\r\nRateLimit-Reset: 900\r\n",
                "provider rate limit",
                60,
            ),
            (429, "RateLimit-Reset: 60\r\n", "provider rate limit", 60),
            (
                429,
                "x-ratelimit-remaining-requests: 0\r\nx-ratelimit-reset-requests: 1s\r\nx-ratelimit-remaining-tokens: 10000\r\nx-ratelimit-reset-tokens: 65s\r\n",
                "provider rate limit",
                65,
            ),
            (
                429,
                "x-ratelimit-remaining-requests: 1000\r\nx-ratelimit-reset-requests: 1s\r\nx-ratelimit-remaining-tokens: 100\r\nx-ratelimit-reset-tokens: 65s\r\n",
                "provider rate limit",
                65,
            ),
            (503, "Retry-After: 60\r\n", "provider transport", 60),
        ] {
            let expected = Duration::from_secs(seconds);
            let until = format!("until {}.000000000 (Unix seconds)", 1_700_000_000 + seconds);
            let (port, accepted) = serve(vec![
                Reply::Headers(status, headers, r#"{"detail":"Rate limit exceeded. Please slow down your requests. Limit: 24290 uncached input tokens/minute."}"#),
                Reply::Full(200, CHAT),
            ]).await?;
            let (entered_tx, mut entered) = tokio::sync::oneshot::channel();
            let (release, release_rx) = tokio::sync::oneshot::channel();
            let transport = Arc::new(Counted {
                inner: ReqwestTransport::new()?,
                sent: AtomicUsize::new(0),
                waits: Mutex::new(Vec::new()),
                gate: Some(Arc::new(WaitGate {
                    entered: Mutex::new(Some(entered_tx)),
                    release: Mutex::new(Some(release_rx)),
                })),
            });
            let provider = super::provider_with(
                Adapter::Chat,
                &format!("http://127.0.0.1:{port}/v1"),
                transport.clone(),
            );
            let (request, dispatch) = super::request(&provider)?;
            let work = super::native_tests::work(&acyclic_fs::CancellationToken::new())?;
            let baseline = work.live_allocation_bytes();
            let report = acyclic_native_runtime::obs::WaitReport::default();
            let log = Log(Arc::new(Mutex::new(Vec::new())));
            let writer = log.clone();
            let subscriber = tracing_subscriber::registry().with(report.clone()).with(
                tracing_subscriber::fmt::layer()
                    .without_time()
                    .with_ansi(false)
                    .with_writer(move || writer.clone()),
            );
            let run = async {
                let (mut completed, mut calls, mut content) = (0, 0, false);
                let mut observe = |event: &ModelEvent| match event {
                    ModelEvent::Content { delta } => content |= delta == "hé",
                    ModelEvent::ToolCall(call) => {
                        assert_eq!(call.call_id, dispatch.tool_call_id(0));
                        calls += 1;
                    }
                    ModelEvent::Completed { .. } => completed += 1,
                    _ => {}
                };
                if native {
                    let mut stream = provider
                        .generate_with_native_work(&request, dispatch, &work)?
                        .await?;
                    while let Some(event) = stream.next().await {
                        let (event, custody) = event?;
                        observe(&event);
                        drop((event, custody));
                    }
                } else {
                    let mut stream = provider.generate(&request, dispatch);
                    while let Some(event) = stream.next().await {
                        observe(&event?);
                    }
                }
                Ok::<_, Error>((completed, calls, content))
            }
            .with_subscriber(subscriber);
            tokio::pin!(run);
            tokio::select! {
                result = &mut run => panic!("turn ended before its provider wait: {result:?}"),
                wait = &mut entered => assert_eq!(wait.unwrap(), expected),
            }
            assert_eq!(transport.sent.load(Ordering::SeqCst), 1);
            assert_eq!(accepted.load(Ordering::SeqCst), 1);
            assert_eq!(*transport.waits.lock().unwrap(), [expected]);
            let waits = report.snapshot();
            assert!(
                waits
                    .iter()
                    .any(|wait| wait.primitive == reason && wait.key == until)
            );
            let visible = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
            assert!(
                visible.contains(&format!("waiting for {reason} {until}")),
                "{visible}"
            );
            release.send(()).unwrap();
            assert_eq!(run.await?, (1, 1, true));
            assert_eq!(transport.sent.load(Ordering::SeqCst), 2);
            assert_eq!(accepted.load(Ordering::SeqCst), 2);
            assert!(
                report
                    .snapshot()
                    .iter()
                    .all(|wait| wait.primitive != reason)
            );
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}

#[tokio::test]
async fn refusal_then_success_redelivers_exactly_once() -> Result<()> {
    for native in [false, true] {
        for first in [
            Reply::Lost,
            Reply::Full(429, "rate limited"),
            Reply::Full(503, "busy"),
            Reply::Cut(503, CHAT, 0),
            Reply::Cut(503, CHAT, 12),
        ] {
            let (port, accepted) = serve(vec![first, Reply::Full(200, CHAT)]).await?;
            let (provider, transport) = model(&format!("http://127.0.0.1:{port}/v1"))?;
            if native {
                let (prepared, dispatch) = super::request(&provider)?;
                let work = super::native_tests::work(&acyclic_fs::CancellationToken::new())?;
                let baseline = work.live_allocation_bytes();
                let mut stream = provider
                    .generate_with_native_work(&prepared, dispatch, &work)?
                    .await?;
                let mut completed = false;
                while let Some(event) = stream.next().await {
                    let (event, custody) = event?;
                    completed |= matches!(event, ModelEvent::Completed { .. });
                    drop((event, custody));
                }
                assert!(completed);
                drop(stream);
                assert_eq!(work.live_allocation_bytes(), baseline);
            } else {
                let events = generate(&provider).await?;
                assert!(matches!(events.last(), Some(ModelEvent::Completed { .. })));
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
async fn interrupted_accepted_responses_restart_without_duplicate_output() -> Result<()> {
    let first = CHAT.find("\n\n").unwrap() + 2;
    for native in [false, true] {
        for cut in [0, 12, first + 8] {
            let (port, accepted) =
                serve(vec![Reply::Cut(200, CHAT, cut), Reply::Full(200, CHAT)]).await?;
            let (provider, transport) = model(&format!("http://127.0.0.1:{port}/v1"))?;
            let (request, dispatch) = super::request(&provider)?;
            let work = super::native_tests::work(&acyclic_fs::CancellationToken::new())?;
            let baseline = work.live_allocation_bytes();
            let mut events = Vec::new();
            let mut restarts = 0;
            if native {
                let mut stream = provider
                    .generate_with_native_work(&request, dispatch, &work)?
                    .await?;
                while let Some(event) = stream.next().await {
                    let (event, custody) = event?;
                    assert_eq!(custody.bytes(), event.logical_owned_bytes()?);
                    if matches!(event, ModelEvent::Restarted) {
                        restarts += 1;
                        events.clear();
                    } else {
                        events.push(event);
                    }
                    drop(custody);
                }
            } else {
                let mut stream = provider.generate(&request, dispatch);
                while let Some(event) = stream.next().await {
                    let event = event?;
                    if matches!(event, ModelEvent::Restarted) {
                        restarts += 1;
                        events.clear();
                    } else {
                        events.push(event);
                    }
                }
            }
            assert_eq!(restarts, 1);
            let text: String = events
                .iter()
                .filter_map(|event| match event {
                    ModelEvent::Content { delta } => Some(delta.as_str()),
                    _ => None,
                })
                .collect();
            assert_eq!(text, "hé");
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(event, ModelEvent::ToolCall(_)))
                    .count(),
                1
            );
            assert!(matches!(events.last(), Some(ModelEvent::Completed { .. })));
            assert_eq!(transport.sent.load(Ordering::SeqCst), 2);
            assert_eq!(accepted.load(Ordering::SeqCst), 2);
            drop(events);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
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
                        .count_tokens_with_native_work(&prepared, 0, &work)?
                        .await
                        .map(drop)
                } else {
                    provider.count_tokens(&prepared, 0).await.map(drop)
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
async fn tls_failure_is_typed_for_shared_recovery() -> Result<()> {
    let (port, accepted) = serve(vec![Reply::Garbage]).await?;
    let (provider, transport) = model(&format!("https://127.0.0.1:{port}/v1"))?;
    let error = transport
        .send(HttpRequest {
            method: ::http::Method::POST,
            url: provider.config.base_url.clone(),
            headers: std::collections::BTreeMap::new(),
            body: Bytes::new(),
        })
        .await
        .err()
        .expect("not TLS");
    assert_eq!(failure(&error), Some(ModelTransportFailure::Tls), "{error}");
    assert_eq!(
        error.code(),
        acyclic_harness_core::wire::ErrorCode::Unavailable
    );
    assert_eq!(transport.sent.load(Ordering::SeqCst), 1);
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    Ok(())
}
