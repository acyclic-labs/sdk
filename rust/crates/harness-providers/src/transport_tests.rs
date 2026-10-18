//! Real-socket transport failures: each class is typed, redelivered only when retryable and
//! before output, and counted exactly.
use super::*;
use acyclic_harness::ModelTransportFailure;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// One scripted connection.
enum Reply {
    /// A complete response with this status and body.
    Full(u16, &'static str),
    /// A 200 response declaring the whole body but sending only `usize` bytes before closing.
    Cut(&'static str, usize),
    /// Bytes that are not a TLS record.
    Garbage,
}

/// Counts every send while delegating to the platform transport, without backoff waits.
struct Counted {
    inner: ReqwestTransport,
    sent: AtomicUsize,
}

impl HttpTransport for Counted {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        self.sent.fetch_add(1, Ordering::SeqCst);
        self.inner.send(request)
    }
    // Backoff waits elapse at once; the exchange deadlines, minutes long, never expire here.
    fn delay(&self, wait: std::time::Duration) -> BoxProviderFuture<'static, ()> {
        if wait >= std::time::Duration::from_secs(60) {
            Box::pin(futures::future::pending())
        } else {
            Box::pin(async {})
        }
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
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
                Reply::Cut(body, sent) => format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    &body[..sent]
                ),
                Reply::Garbage => "this is not a TLS record\r\n\r\n".repeat(4),
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
    });
    Ok((
        super::provider_with(Adapter::Chat, base_url, transport.clone()),
        transport,
    ))
}

async fn generate(provider: &ResolvedModel) -> Result<Vec<ModelEvent>> {
    let (prepared, dispatch) = super::request(provider)?;
    provider.generate(prepared, dispatch).try_collect().await
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
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|_| invalid("bind"))?;
        listener.local_addr().map_err(|_| invalid("addr"))?.port()
    };
    let (provider, transport) = model(&format!("http://127.0.0.1:{port}/v1"))?;
    let error = generate(&provider).await.expect_err("closed port");
    assert_eq!(
        failure(&error),
        Some(ModelTransportFailure::Connect),
        "{error}"
    );
    assert_eq!(error.code(), acyclic_harness::wire::ErrorCode::Unavailable);
    assert!(!error.to_string().contains("127.0.0.1"), "{error}");
    assert!(!error.to_string().contains("scripted-key"));
    assert_eq!(transport.sent.load(Ordering::SeqCst), ATTEMPTS);
    // The token count shares the class and the budget.
    let (prepared, _) = super::request(&provider)?;
    let error = provider
        .count_tokens(&prepared)
        .await
        .expect_err("closed port");
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
    let (port, accepted) = serve(vec![Reply::Full(503, "busy"), Reply::Full(200, CHAT)]).await?;
    let (provider, transport) = model(&format!("http://127.0.0.1:{port}/v1"))?;
    let events = generate(&provider).await?;
    assert!(matches!(events.last(), Some(ModelEvent::Completed { .. })));
    assert_eq!(transport.sent.load(Ordering::SeqCst), 2);
    assert_eq!(accepted.load(Ordering::SeqCst), 2);
    // Negative control: a 400 is final on the first attempt.
    let (port, accepted) = serve(vec![Reply::Full(400, "bad"), Reply::Full(200, CHAT)]).await?;
    let (provider, transport) = model(&format!("http://127.0.0.1:{port}/v1"))?;
    assert!(matches!(generate(&provider).await, Err(Error::Invalid(_))));
    assert_eq!(transport.sent.load(Ordering::SeqCst), 1);
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn reset_before_output_is_redelivered_and_after_output_is_final() -> Result<()> {
    // Cut inside the first event: nothing reached the caller, so the request is resent.
    let (port, accepted) = serve(vec![Reply::Cut(CHAT, 12), Reply::Full(200, CHAT)]).await?;
    let (provider, transport) = model(&format!("http://127.0.0.1:{port}/v1"))?;
    let events = generate(&provider).await?;
    assert!(matches!(events.last(), Some(ModelEvent::Completed { .. })));
    assert_eq!(transport.sent.load(Ordering::SeqCst), 2);
    assert_eq!(accepted.load(Ordering::SeqCst), 2);
    // Cut after the first event: output escaped, so the interruption is final.
    let first = CHAT.find("\n\n").map_or(0, |end| end + 2);
    let (port, accepted) = serve(vec![Reply::Cut(CHAT, first + 8), Reply::Full(200, CHAT)]).await?;
    let (provider, transport) = model(&format!("http://127.0.0.1:{port}/v1"))?;
    let (prepared, dispatch) = super::request(&provider)?;
    let mut stream = provider.generate(prepared, dispatch);
    let mut output = 0;
    let error = loop {
        match stream.next().await {
            Some(Ok(_)) => output += 1,
            Some(Err(error)) => break error,
            None => return Err(invalid("cut stream completed")),
        }
    };
    assert!(output > 0);
    assert_eq!(
        failure(&error),
        Some(ModelTransportFailure::Interrupted),
        "{error}"
    );
    assert_eq!(transport.sent.load(Ordering::SeqCst), 1);
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn tls_failure_is_typed_and_never_redelivered() -> Result<()> {
    let (port, accepted) = serve(vec![Reply::Garbage, Reply::Garbage]).await?;
    let (provider, transport) = model(&format!("https://127.0.0.1:{port}/v1"))?;
    let error = generate(&provider).await.expect_err("not TLS");
    assert_eq!(failure(&error), Some(ModelTransportFailure::Tls), "{error}");
    assert_eq!(error.code(), acyclic_harness::wire::ErrorCode::Invalid);
    assert_eq!(transport.sent.load(Ordering::SeqCst), 1);
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    Ok(())
}
