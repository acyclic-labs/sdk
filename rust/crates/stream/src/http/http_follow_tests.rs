//! JSON unary framing, cancellation and observation regressions.
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "Test fixtures and assertions must fail immediately on malformed inputs or violated expectations"
)]
use super::*;
use acyclic_grpc_observability::tests::Capture;
use serde_json::json;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};

struct Reply(u16, Value);
enum BodyMode {
    Truncated,
    Held {
        prefix_written: oneshot::Sender<()>,
        peer_closed: oneshot::Sender<()>,
    },
}
impl Reply {
    fn ok(body: Value) -> Self {
        Self(200, body)
    }
}
struct Server {
    client: HttpStream,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    task: JoinHandle<()>,
}
impl Server {
    async fn new(replies: Vec<Reply>) -> Self {
        Self::with_body_mode(replies, None).await
    }
    async fn with_body_mode(replies: Vec<Reply>, mode: Option<BodyMode>) -> Self {
        Self::with_body_mode_at(replies, mode, "/v1/stream/tail").await
    }
    async fn with_body_mode_at(
        replies: Vec<Reply>,
        mut mode: Option<BodyMode>,
        held_route: &'static str,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = HttpStream::new(
            &format!("http://{}", listener.local_addr().unwrap()),
            "fixture-token",
            1024 * 1024,
        )
        .unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let mut replies: VecDeque<_> = replies.into();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (route, body) = request(&mut socket).await;
                let hold = route == held_route;
                observed.lock().unwrap().push((route, body));
                let Reply(status, value) = replies
                    .pop_front()
                    .unwrap_or(Reply(500, json!({"code":"unavailable"})));
                let body = serde_json::to_vec(&value).unwrap();
                socket.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
                match if hold { mode.take() } else { None } {
                    Some(BodyMode::Truncated) => {
                        socket.write_all(&body[..1]).await.unwrap();
                    }
                    Some(BodyMode::Held {
                        prefix_written,
                        peer_closed,
                    }) => {
                        socket.write_all(&body[..1]).await.unwrap();
                        prefix_written.send(()).unwrap();
                        let mut byte = [0; 1];
                        let closed = match socket.read(&mut byte).await {
                            Ok(count) => count == 0,
                            Err(error) => matches!(
                                error.kind(),
                                std::io::ErrorKind::ConnectionReset
                                    | std::io::ErrorKind::ConnectionAborted
                            ),
                        };
                        assert!(closed, "expected peer closure while response body held");
                        peer_closed.send(()).unwrap();
                        continue;
                    }
                    None => socket.write_all(&body).await.unwrap(),
                }
                socket.shutdown().await.unwrap();
            }
        });
        Self {
            client,
            requests,
            task,
        }
    }
    fn requests(&self) -> Vec<(String, Value)> {
        self.requests.lock().unwrap().clone()
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn request(socket: &mut TcpStream) -> (String, Value) {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0; 1024];
        let count = socket.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0, "HTTP request ended before headers");
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(index) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break index + 4;
        }
        assert!(bytes.len() <= 65536, "oversized fixture request headers");
    };
    let headers = std::str::from_utf8(&bytes[..header_end]).unwrap();
    let route = headers
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .to_owned();
    let length: usize = headers
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().unwrap())
        })
        .unwrap();
    assert!(length <= 65536);
    while bytes.len() < header_end + length {
        let mut chunk = [0; 1024];
        let count = socket.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0, "HTTP request ended before body");
        bytes.extend_from_slice(&chunk[..count]);
    }
    let body = serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
    (route, body)
}

fn path() -> StreamPath {
    StreamPath::new("follow/test").unwrap()
}
fn record(sequence: u64) -> Value {
    json!({"sequence":sequence.to_string(),"value":"eA==", "commitId":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "committedAtMicros":"1", "keys":[], "attributes":[]})
}

#[tokio::test]
async fn head_refuses_incoherent_cuts_without_another_provider_request()
-> Result<(), Box<dyn std::error::Error>> {
    for (tail, durable, sequence, inherited, valid) in [
        (0, 0, None, None, true),
        (0, 0, None, Some(0), true),
        (1, 1, Some(0), Some(1), true),
        (1, 0, None, None, false),
        (0, 0, Some(0), None, false),
        (2, 0, Some(0), None, false),
        (1, 2, Some(0), None, false),
        (1, 0, Some(0), Some(2), false),
        (u64::MAX, 0, Some(u64::MAX), None, false),
    ] {
        let origin = inherited.map(|cut| json!({
            "source":"head/source", "forkedAt":cut.to_string(),
            "commitId":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        }));
        let server = Server::new(vec![Reply::ok(json!({
            "tail":tail.to_string(), "durable":durable.to_string(),
            "last":sequence.map(record), "origin":origin,
        }))]).await;
        let actual = server.client.head(StreamPath::new("head/child")?).await;
        let wire = crate::wire::PathHead {
            tail, durable,
            last: sequence.map(|n| parse_record(&record(n))
                .map(crate::wire_codec::record_wire)).transpose()?,
            origin: inherited.map(|forked_at| crate::wire::ForkOrigin {
                source: "head/source".into(), forked_at,
                commit_id: Bytes::from_static(&[0; 32]),
            }),
        };
        assert_eq!(actual, crate::wire_codec::head_from_wire(wire));
        if valid {
            assert_eq!(actual?.tail, tail);
        } else {
            assert_eq!(actual, Err(StreamError::InvalidResponse));
        }
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, "/v1/stream/head");
    }
    Ok(())
}

#[tokio::test]
async fn request_path_typed_append_preserves_json_responses_and_validation_trace() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;

    let captured = Capture::default();
    let subscriber = tracing_subscriber::registry().with(captured.clone());
    async {
        let server = Server::new(vec![
            Reply::ok(json!({"ok":true,"start":"0","end":"1","tail":"1","commitId":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="})),
            Reply::ok(json!({"ok":false,"code":"tail_conflict","actualTail":"1"})),
        ]).await;
        let mut request = AppendRequest {
            path: path(),
            records: Vec::new(),
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::new(bytes::Bytes::from_static(b"key")).unwrap(),
        };
        assert_eq!(server.client.append(request.clone()).await, Err(StreamError::LimitExceeded));
        assert!(server.requests().is_empty());
        request.records.push((OwnedBytes::from_static(b"\x00\xff")).into());
        assert!(matches!(server.client.append(request.clone()).await.unwrap(), AppendOutcome::Committed(AppendReceipt { start: 0, end: 1, tail: 1, .. })));
        assert_eq!(server.client.append(request).await.unwrap(), AppendOutcome::TailConflict { actual_tail: 1 });
        let expected = ("/v1/stream/append".into(), json!({
            "path":"follow/test", "records":[{"value":"AP8=","keys":[],"attributes":[]}],
            "options":{"ifTail":"0","idempotencyKey":"a2V5"},
        }));
        assert_eq!(server.requests(), vec![expected.clone(), expected]);
    }.with_subscriber(subscriber).await;
    let evidence = captured.0.lock().unwrap();
    let calls: Vec<_> = evidence
        .spans
        .iter()
        .filter(|span| span.name == "acyclic.stream.http.call")
        .collect();
    assert_eq!(calls.len(), 3);
    assert!(calls.iter().all(|span| span.closed));
    assert_eq!(calls[0].fields["outcome"], "err");
    assert_eq!(calls[0].fields["error.kind"], "limit_exceeded");
    assert!(calls[1..].iter().all(|span| span.fields["outcome"] == "ok"));
}

#[tokio::test]
async fn truncated_unary_body_fails_without_retry() {
    let server =
        Server::with_body_mode(vec![Reply::ok(json!("1"))], Some(BodyMode::Truncated)).await;
    assert!(matches!(
        server.client.tail(path()).await,
        Err(StreamError::Unavailable)
    ));
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn cancelling_a_pending_unary_body_closes_the_connection() {
    let (prefix_written, prefix) = oneshot::channel();
    let (peer_closed, closed) = oneshot::channel();
    let server = Server::with_body_mode(
        vec![Reply::ok(json!("1"))],
        Some(BodyMode::Held {
            prefix_written,
            peer_closed,
        }),
    )
    .await;
    {
        let reading = server.client.tail(path());
        tokio::pin!(reading);
        tokio::select! {
            result = &mut reading => panic!("body completed while held: {}", result.is_ok()),
            ready = prefix => ready.unwrap(),
        }
    }
    tokio::time::timeout(Duration::from_secs(1), closed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(server.requests().len(), 1);
}
