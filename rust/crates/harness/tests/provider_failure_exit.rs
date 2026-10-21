//! A typed provider failure reaches the headless JSONL and exits 1 over real loopback sockets:
//! refused connections, refusal statuses, a body cut after output escaped and a body that falls
//! silent after output. A recovered refusal is the negative control, and exits 0 with the
//! provider's text. Harness state lives in fixture directories only.
//!
//! Deadlines are measured on a fixture clock, not the wall clock: time passes only when the
//! scripted server has fallen silent after output, so scheduling delay on a loaded host never
//! reads as provider silence, and the one test that measures silence measures only that.
#![cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]

use acyclic_harness::{
    Error, LocalHarness, OperationId, Result, client,
    context::{CompactionPolicy, ModelContextCapacity},
    model::ModelProvider as _,
    test_support::local_harness_config,
};
use acyclic_harness_providers::{
    Adapter, Authentication, Credentials, HttpRequest, HttpResponse, HttpTransport, ModelFeatures,
    ModelSpec, ProviderSpec, Registry, ReqwestTransport, RetrySpec, TimeoutSpec, TokenCountField,
    TokenCounterSpec,
};
use acyclic_stream::BoxProviderFuture;
use futures::StreamExt as _;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

const TEXT: &str = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"loopback ok\"}\n\n";
const DONE: &str = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"run\",\"status\":\"completed\",\"usage\":{\"input_tokens\":4,\"output_tokens\":2,\"total_tokens\":6}}}\n\n";
/// Redeliveries after the first attempt.
const REDELIVERIES: u8 = 2;

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

/// One scripted connection.
enum Reply {
    /// A complete response with this status and body.
    Full(u16, String),
    /// A 200 response declaring `body` but closing after its first `usize` bytes.
    Cut(String, usize),
    /// A 200 response declaring `body`, sending its first `usize` bytes, then falling silent.
    Hold(String, usize),
}

/// The idle deadline of these exchanges, on the fixture [`Clock`].
const IDLE_MS: u32 = 300;

/// Whether a run measures the server's silence after output.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Silence {
    /// No deadline elapses: the scripted reply alone decides the outcome.
    Unmeasured,
    /// Once output has arrived, deadline wake-ups elapse, so a silent body times out.
    Measured,
}

/// The fixture clock deadlines are measured on. It is frozen, so neither scheduling nor a slow
/// consumer between polls counts as silence. Redelivery backoffs, shorter than the idle deadline,
/// elapse at once. A deadline wake-up elapses only after output arrived in a [`Silence::Measured`]
/// run, and then advances the clock by its wait, so it expires exactly the deadline it was armed
/// for.
struct Clock {
    silence: Silence,
    /// Nanoseconds since the origin.
    now: AtomicU64,
    /// Set once output arrived in a measured run.
    output: tokio::sync::watch::Sender<bool>,
}

impl Clock {
    fn now(&self) -> Duration {
        Duration::from_nanos(self.now.load(Ordering::SeqCst))
    }
}

/// Counts the requests sent through the platform transport, and keeps the fixture clock.
struct Counted {
    inner: ReqwestTransport,
    clock: Arc<Clock>,
    /// Model requests.
    sent: AtomicUsize,
    /// Every request, token counts included.
    all: AtomicUsize,
}

impl HttpTransport for Counted {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        self.all.fetch_add(1, Ordering::SeqCst);
        if !request.url.ends_with(COUNT_PATH) {
            self.sent.fetch_add(1, Ordering::SeqCst);
        }
        let clock = self.clock.clone();
        let send = self.inner.send(request);
        Box::pin(async move {
            let mut response = send.await?;
            if clock.silence == Silence::Measured {
                response.body = Box::pin(response.body.inspect(move |item| {
                    if matches!(item, Ok(bytes) if !bytes.is_empty()) {
                        clock.output.send_replace(true);
                    }
                }));
            }
            Ok(response)
        })
    }
    fn delay(&self, wait: Duration) -> BoxProviderFuture<'static, ()> {
        if wait < Duration::from_millis(IDLE_MS.into()) {
            return Box::pin(async {});
        }
        let clock = self.clock.clone();
        let due = clock.now().saturating_add(wait);
        let mut output = clock.output.subscribe();
        Box::pin(async move {
            // The clock owns the sender, so the channel stays open while this waits.
            let _ = output.wait_for(|arrived| *arrived).await;
            let due = u64::try_from(due.as_nanos()).unwrap_or(u64::MAX);
            clock.now.fetch_max(due, Ordering::SeqCst);
        })
    }
    fn now(&self) -> Duration {
        self.clock.now()
    }
}

const COUNT_PATH: &str = "count_tokens";

/// Serves `replies` in order, one per model request, answering every token count, and returns
/// the API root.
async fn serve(replies: Vec<Reply>) -> Result<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(storage)?;
    let port = listener.local_addr().map_err(storage)?.port();
    let mut replies: VecDeque<_> = replies.into();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let count = Reply::Full(200, "{\"input_tokens\":4}".into());
            let reply = if read_request(&mut socket).await.contains(COUNT_PATH) {
                count
            } else if let Some(reply) = replies.pop_front() {
                reply
            } else {
                return;
            };
            let (status, declared, sent) = match &reply {
                Reply::Full(status, body) => (*status, body.len(), body.as_str()),
                Reply::Cut(body, sent) | Reply::Hold(body, sent) => {
                    (200, body.len(), &body[..*sent])
                }
            };
            let bytes = format!(
                "HTTP/1.1 {status} S\r\ncontent-type: text/event-stream\r\ncontent-length: {declared}\r\nconnection: close\r\n\r\n{sent}"
            );
            let _ = socket.write_all(bytes.as_bytes()).await;
            let _ = socket.flush().await;
            if matches!(reply, Reply::Hold(..)) {
                // Silent but open until the client gives up and hangs up.
                let mut rest = [0; 4096];
                while matches!(socket.read(&mut rest).await, Ok(read) if read > 0) {}
                continue;
            }
            // Let the client hang up first, so the short body is a cut and not a reset.
            let _ = socket.shutdown().await;
            let mut rest = [0; 4096];
            while matches!(socket.read(&mut rest).await, Ok(read) if read > 0) {}
        }
    });
    Ok(format!("http://127.0.0.1:{port}/v1"))
}

/// Reads one request's headers and its declared body, returning the request line.
async fn read_request(socket: &mut tokio::net::TcpStream) -> String {
    let mut buffer = Vec::new();
    let mut chunk = [0; 4096];
    while let Ok(read @ 1..) = socket.read(&mut chunk).await {
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
            return head.lines().next().unwrap_or_default().to_owned();
        }
    }
    String::new()
}

/// One headless run's exit code, JSONL, model requests and all requests.
struct Run {
    code: u8,
    jsonl: String,
    sent: usize,
    all: usize,
}

/// Runs one headless prompt against `base_url` with fixture state only.
async fn headless(base_url: String, silence: Silence) -> Result<Run> {
    let workspace = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let transport = Arc::new(Counted {
        inner: ReqwestTransport::new()?,
        clock: Arc::new(Clock {
            silence,
            now: AtomicU64::new(0),
            output: tokio::sync::watch::channel(false).0,
        }),
        sent: AtomicUsize::new(0),
        all: AtomicUsize::new(0),
    });
    let mut registry =
        Registry::with_transport(Credentials::explicit(BTreeMap::new()), transport.clone());
    registry.register(
        "loopback".into(),
        ProviderSpec {
            adapter: Adapter::Responses,
            authentication: Authentication::Optional,
            base_url,
            headers: BTreeMap::new(),
            models: BTreeMap::from([(
                "model".into(),
                ModelSpec {
                    max_context_tokens: None,
                    capacity: ModelContextCapacity {
                        context_tokens: 32_768,
                        output_tokens: Some(1024),
                    },
                    features: ModelFeatures {
                        tools: true,
                        images: false,
                    },
                },
            )]),
            token_counter: Some(TokenCounterSpec {
                path: COUNT_PATH.into(),
                adapter: Adapter::Responses,
                response_field: TokenCountField::InputTokens,
                framing_tokens: 0,
            }),
            max_event_bytes: 4096,
            retry: RetrySpec {
                attempts: REDELIVERIES,
                base_delay_ms: 1,
                max_delay_ms: 1,
            },
            timeouts: TimeoutSpec {
                idle_ms: IDLE_MS,
                total_ms: 60_000,
            },
        },
    )?;
    let provider = Arc::new(registry.resolve("loopback/model")?);
    let config = local_harness_config(workspace.path(), (provider.model().clone(), provider))?
        .state_directory(state.path().join("state"))
        .compaction(CompactionPolicy::Disabled)
        .goal_continuations(None);
    let local = Arc::new(LocalHarness::open(config).await?);
    let operation = OperationId::new();
    let command = client::prepare_local_prompt(operation, "Say ok.")?;
    let (mut connection, target) = client::connect_local(local, operation).await?;
    let mut output = Vec::new();
    let outcome = client::run_headless(&mut connection, command, target, &mut output).await;
    let code = connection.finish(outcome).await?.exit_code();
    let jsonl = String::from_utf8(output).map_err(storage)?;
    Ok(Run {
        code,
        jsonl,
        sent: transport.sent.load(Ordering::SeqCst),
        all: transport.all.load(Ordering::SeqCst),
    })
}

fn attempts() -> usize {
    1 + usize::from(REDELIVERIES)
}

#[tokio::test]
async fn refused_connections_fail_the_turn_with_a_typed_cause_and_exit_1() -> Result<()> {
    // Bound and released: nothing listens, so every connect is refused.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(storage)?;
    let port = listener.local_addr().map_err(storage)?.port();
    drop(listener);
    let Run {
        code, jsonl, all, ..
    } = headless(format!("http://127.0.0.1:{port}/v1"), Silence::Unmeasured).await?;
    assert_eq!(code, 1, "{jsonl}");
    // The first exchange to meet the refusal, a token count or the request, ends the turn
    // after exactly its bounded attempts.
    assert_eq!(all, attempts());
    assert!(
        jsonl.contains("model provider unavailable (connection failed)"),
        "{jsonl}"
    );
    assert!(!jsonl.contains("storage failure"), "{jsonl}");
    Ok(())
}

#[tokio::test]
async fn refusal_statuses_fail_with_their_status_and_body_and_exit_1() -> Result<()> {
    // Retryable: redelivered until the budget runs out, then settled with the body excerpt.
    let replies = (0..attempts())
        .map(|_| Reply::Full(503, "upstream overloaded".into()))
        .collect();
    let Run {
        code, jsonl, sent, ..
    } = headless(serve(replies).await?, Silence::Unmeasured).await?;
    assert_eq!(code, 1, "{jsonl}");
    assert_eq!(sent, attempts());
    assert!(
        jsonl.contains("model provider unavailable (HTTP 503): upstream overloaded"),
        "{jsonl}"
    );
    // Final: one attempt only.
    let replies = vec![Reply::Full(400, "bad request".into())];
    let Run {
        code, jsonl, sent, ..
    } = headless(serve(replies).await?, Silence::Unmeasured).await?;
    assert_eq!(code, 1, "{jsonl}");
    assert_eq!(sent, 1);
    assert!(jsonl.contains("status 400"), "{jsonl}");
    Ok(())
}

#[tokio::test]
async fn a_body_cut_after_output_is_final_and_exits_1() -> Result<()> {
    let body = format!("{TEXT}{DONE}");
    let replies = vec![Reply::Cut(body, TEXT.len())];
    let Run {
        code, jsonl, sent, ..
    } = headless(serve(replies).await?, Silence::Unmeasured).await?;
    assert_eq!(code, 1, "{jsonl}");
    assert_eq!(sent, 1);
    assert!(
        jsonl.contains("model provider unavailable (connection interrupted)"),
        "{jsonl}"
    );
    Ok(())
}

#[tokio::test]
async fn a_body_silent_after_output_times_out_and_exits_1() -> Result<()> {
    // Output escaped, so the idle timeout is final: one request, a typed timeout, exit 1.
    let body = format!("{TEXT}{DONE}");
    let replies = vec![Reply::Hold(body, TEXT.len())];
    let Run {
        code, jsonl, sent, ..
    } = headless(serve(replies).await?, Silence::Measured).await?;
    assert_eq!(code, 1, "{jsonl}");
    assert_eq!(sent, 1);
    assert!(
        jsonl.contains(&format!(
            "model provider unavailable (timed out): no response bytes for {IDLE_MS} ms"
        )),
        "{jsonl}"
    );
    Ok(())
}

#[tokio::test]
async fn a_recovered_refusal_completes_and_exits_0() -> Result<()> {
    let replies = vec![
        Reply::Full(503, "upstream overloaded".into()),
        Reply::Full(200, format!("{TEXT}{DONE}")),
    ];
    let Run {
        code, jsonl, sent, ..
    } = headless(serve(replies).await?, Silence::Unmeasured).await?;
    assert_eq!(code, 0, "{jsonl}");
    assert_eq!(sent, 2);
    assert!(jsonl.contains("loopback ok"), "{jsonl}");
    assert!(!jsonl.contains("model provider unavailable"), "{jsonl}");
    Ok(())
}
