//! Qualification of the real HTTP cursor with an explicitly released idle clock.
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "Test fixtures and assertions must fail immediately on malformed inputs or violated expectations"
)]
use super::*;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::Instrument;

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
        Self::with_body_mode_at(replies, mode, "/v1/stream/read").await
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
    assert_eq!(headers.lines().next().unwrap().split_whitespace().next(), Some("POST"));
    assert!(headers.lines().any(|line| {
        line.split_once(':').is_some_and(|(key, value)| {
            key.eq_ignore_ascii_case("authorization") && value.trim() == "Bearer fixture-token"
        })
    }));
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

#[tokio::test]
async fn explicit_delivery_ack_uses_the_canonical_loopback_projection() {
    let response = crate::http_response::encode(
        "delivery/acknowledge",
        &wire::AcknowledgeDeliveryResponse {}.encode_to_vec(),
        1024,
    )
    .unwrap();
    let server = Server::new(vec![Reply::ok(serde_json::from_slice(&response).unwrap())]).await;
    assert_eq!(
        server.client.acknowledge_delivery(Bytes::from_static(&[0; 32])).await,
        Ok(())
    );
    assert_eq!(
        server.requests(),
        vec![(
            "/v1/stream/delivery/acknowledge".to_owned(),
            json!({"deliveryToken": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}),
        )]
    );
}

#[tokio::test]
async fn explicit_delivery_ack_rejects_bad_tokens_without_a_request() {
    let server = Server::new(vec![]).await;
    for length in [0, 1, 31, 33, 64] {
        assert_eq!(
            server.client.acknowledge_delivery(Bytes::from(vec![0; length])).await,
            Err(StreamError::InvalidArgument)
        );
    }
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn explicit_delivery_ack_validates_success_and_never_retries_failures() {
    for reply in [
        Reply::ok(Value::Null),
        Reply::ok(json!([])),
        Reply::ok(json!(true)),
        Reply(503, json!({"code": "unavailable"})),
    ] {
        let server = Server::new(vec![reply]).await;
        assert_eq!(
            server.client.acknowledge_delivery(Bytes::from_static(&[0; 32])).await,
            Err(StreamError::Unavailable)
        );
        assert_eq!(server.requests().len(), 1);
    }
}

#[tokio::test]
async fn http_read_never_projects_or_automatically_acknowledges_metadata() {
    let server = Server::new(vec![Reply::ok(json!("0")), Reply::ok(json!([]))]).await;
    let mut records = server.client.read(ReadRequest {
        path: StreamPath::new("events").unwrap(),
        from: 0,
        limit: 1,
    }).await.unwrap();
    assert!(records.next().await.is_none());
    assert_eq!(
        server.requests(),
        vec![
            ("/v1/stream/tail".to_owned(), json!({"path": "events"})),
            ("/v1/stream/read".to_owned(), json!({"path": "events", "from": "0", "limit": 1})),
        ]
    );
}

type Gate = (Duration, oneshot::Sender<()>);
type DelayFuture = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;
fn clock() -> (
    impl Fn(Duration) -> DelayFuture + Send,
    mpsc::UnboundedReceiver<Gate>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    (
        move |duration| -> DelayFuture {
            let (release, wait) = oneshot::channel();
            tx.send((duration, release)).unwrap();
            Box::pin(async move {
                wait.await.unwrap();
            })
        },
        rx,
    )
}
async fn idle(
    stream: &mut RecordStream,
    clock: &mut mpsc::UnboundedReceiver<Gate>,
) -> oneshot::Sender<()> {
    tokio::select! {
        item = stream.next() => panic!("expected idle wait, got {item:?}"),
        gate = clock.recv() => {
            let (duration, release) = gate.unwrap();
            assert_eq!(duration, Duration::from_millis(250));
            release
        }
    }
}
fn path() -> StreamPath {
    StreamPath::new("follow/test").unwrap()
}
fn record(sequence: u64) -> Value {
    json!({"sequence":sequence.to_string(),"value":"eA==", "commitId":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "committedAtMicros":"1"})
}
fn page(start: u64, length: u64) -> Value {
    Value::Array((start..start + length).map(record).collect())
}

#[tokio::test]
async fn startup_rejects_invalid_tail_and_cursor_before_any_read() {
    for (reply, from, expected) in [
        (Reply::ok(json!("3")), 4, StreamError::OutOfRange),
        (
            Reply(404, json!({"code":"stream_not_found"})),
            0,
            StreamError::NotFound,
        ),
        (Reply::ok(json!("01")), 0, StreamError::Unavailable),
    ] {
        let server = Server::new(vec![reply]).await;
        let (delay, mut gates) = clock();
        let result = server.client.follow_with_delay(path(), from, delay).await;
        assert!(matches!(result, Err(error) if error == expected));
        assert_eq!(
            server.requests(),
            vec![("/v1/stream/tail".into(), json!({"path":"follow/test"}))]
        );
        assert!(gates.try_recv().is_err());
    }
}

#[tokio::test]
async fn pages_preserve_backpressure_cursor_handoff_and_live_delivery() {
    let server = Server::new(vec![
        Reply::ok(json!("266")),
        Reply::ok(page(10, 256)),
        Reply::ok(json!([])),
        Reply::ok(page(266, 1)),
        Reply::ok(json!([])),
    ])
    .await;
    let (delay, mut gates) = clock();
    let mut stream = server
        .client
        .follow_with_delay(path(), 10, delay)
        .await
        .unwrap();
    assert_eq!(
        server.requests().len(),
        1,
        "follow construction is lazy after tail validation"
    );
    for sequence in 10..266 {
        let received = stream.next().await.unwrap().unwrap();
        assert_eq!(received.sequence, sequence);
        assert_eq!(received.value.as_ref(), b"x");
        assert_eq!(
            server.requests().len(),
            2,
            "queued delivery must not fetch another page"
        );
    }
    let release = idle(&mut stream, &mut gates).await;
    assert_eq!(server.requests().len(), 3);
    release.send(()).unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().sequence, 266);
    assert_eq!(server.requests().len(), 4);
    let mut release = idle(&mut stream, &mut gates).await;
    assert_eq!(server.requests().len(), 5);
    let requests = server.requests();
    assert_eq!(
        requests[1..]
            .iter()
            .map(|(route, value)| (
                route.as_str(),
                value["from"].clone(),
                value["limit"].clone()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("/v1/stream/read", json!("10"), json!(256)),
            ("/v1/stream/read", json!("266"), json!(256)),
            ("/v1/stream/read", json!("266"), json!(256)),
            ("/v1/stream/read", json!("267"), json!(256)),
        ]
    );
    drop(stream);
    release.closed().await;
    assert_eq!(server.requests().len(), 5);
}

#[tokio::test]
async fn idle_polling_budget_and_drop_are_controlled_by_each_wait() {
    let server = Server::new(vec![
        Reply::ok(json!("0")),
        Reply::ok(json!([])),
        Reply::ok(json!([])),
        Reply::ok(json!([])),
    ])
    .await;
    let (delay, mut gates) = clock();
    let mut stream = server
        .client
        .follow_with_delay(path(), 0, delay)
        .await
        .unwrap();
    let release = idle(&mut stream, &mut gates).await;
    assert_eq!(server.requests().len(), 2);
    release.send(()).unwrap();
    let release = idle(&mut stream, &mut gates).await;
    assert_eq!(server.requests().len(), 3, "one read per released wait");
    release.send(()).unwrap();
    let mut release = idle(&mut stream, &mut gates).await;
    assert_eq!(server.requests().len(), 4, "tail is never polled again");
    drop(stream);
    release.closed().await;
    assert!(
        gates.recv().await.is_none(),
        "dropping cursor cancels its clock closure"
    );
    assert_eq!(server.requests().len(), 4);
}

#[tokio::test]
async fn adversarial_pages_emit_one_error_then_eof_without_retry() {
    for (from, reply, expected) in [
        (
            0,
            Reply::ok(json!({"records":[]})),
            StreamError::Unavailable,
        ),
        (
            0,
            Reply::ok(json!([{"sequence":"0"}])),
            StreamError::Unavailable,
        ),
        (0, Reply::ok(page(1, 1)), StreamError::Unavailable),
        (
            0,
            Reply::ok(json!([record(0), record(0)])),
            StreamError::Unavailable,
        ),
        (0, Reply::ok(page(0, 257)), StreamError::Unavailable),
        (
            u64::MAX,
            Reply::ok(json!([record(u64::MAX)])),
            StreamError::Unavailable,
        ),
        (
            0,
            Reply(403, json!({"error":{"code":"access_denied"}})),
            StreamError::AccessDenied,
        ),
    ] {
        let server = Server::new(vec![Reply::ok(json!(u64::MAX.to_string())), reply]).await;
        let (delay, mut gates) = clock();
        let mut stream = server
            .client
            .follow_with_delay(path(), from, delay)
            .await
            .unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap_err(), expected);
        assert!(stream.next().await.is_none());
        assert_eq!(server.requests().len(), 2);
        assert!(gates.try_recv().is_err());
    }
}

#[tokio::test]
async fn public_follow_uses_the_same_immediate_page_and_terminal_error_contract() {
    let server = Server::new(vec![
        Reply::ok(json!("1")),
        Reply::ok(page(0, 1)),
        Reply(403, json!({"code":"access_denied"})),
    ])
    .await;
    let mut stream = server.client.follow(path(), 0).await.unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().sequence, 0);
    assert_eq!(
        stream.next().await.unwrap().unwrap_err(),
        StreamError::AccessDenied
    );
    assert!(stream.next().await.is_none());
    assert_eq!(server.requests().len(), 3);
}

use acyclic_grpc_observability::test_support::Capture;

fn assert_cancelled_call(captured: &Capture, route: &str) {
    let spans = captured.0.lock().unwrap();
    let call = spans
        .spans.iter().rev()
        .find(|span| {
            span.name == "acyclic.stream.http.call"
                && span
                    .fields
                    .get("route")
                    .is_some_and(|value| value.trim_matches('"') == route)
        })
        .unwrap();
    assert!(call.closed);
    assert_eq!(call.fields["outcome"], "err");
    assert_eq!(call.fields["error.kind"], "cancelled");
}
async fn until_status(captured: &Capture, route: &str) {
    loop {
        let notified = captured.1.notified();
        let headers_received = captured.0.lock().unwrap().spans.iter().rev()
            .find(|span| span.name == "acyclic.stream.http.call"
                && span.fields.get("route").is_some_and(|value| value == route))
            .is_some_and(|span| span.fields.get("http.status").is_some_and(|status| status == "200"));
        if headers_received {
            return;
        }
        notified.await;
    }
}
fn assert_parent_sentinels(captured: &Capture) {
    let spans = captured.0.lock().unwrap();
    let parent = spans
        .spans.iter()
        .find(|span| span.name == "test.parent")
        .unwrap();
    assert_eq!(parent.fields["outcome"], "sentinel");
    assert_eq!(parent.fields["error.kind"], "sentinel");
    assert_eq!(parent.fields["bytes"], "777");
    assert_eq!(parent.fields["http.status"], "777");
}
fn assert_safe_fields(captured: &Capture) {
    for span in captured.0.lock().unwrap().spans.iter() {
        for field in &span.declared_fields {
            assert!(
                !["path", "token", "content", "body", "authorization", "error"].contains(field),
                "forbidden trace field {field} on {}",
                span.name
            );
        }
        for value in span.fields.values() {
            assert!(!value.contains("fixture-token"));
            assert!(!value.contains("follow/test"));
            assert!(!value.contains("eA=="));
        }
    }
}


#[tokio::test]
async fn filtered_spans_never_record_child_results_on_the_parent() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::{filter::filter_fn, prelude::*};
    for (request_enabled, follow_enabled) in
        [(true, true), (false, true), (true, false), (false, false)]
    {
        for fails in [false, true] {
            let captured = Capture::default();
            let filter = filter_fn(move |metadata| match metadata.name() {
                "acyclic.stream.http.call" => request_enabled,
                "acyclic.stream.http.follow" => follow_enabled,
                _ => true,
            });
            let subscriber =
                tracing_subscriber::registry().with(captured.clone().with_filter(filter));
            async {
                let parent = tracing::info_span!(
                    "test.parent",
                    outcome = "sentinel",
                    error.kind = "sentinel",
                    bytes = 777_u64,
                    http.status = 777_u64
                );
                async {
                    let server = Server::new(vec![
                        Reply::ok(json!("1")),
                        Reply::ok(page(0, 1)),
                        Reply(403, json!({"code":"access_denied"})),
                    ])
                    .await;
                    let (delay, _) = clock();
                    let mut stream = server
                        .client
                        .follow_with_delay(path(), 0, delay)
                        .await
                        .unwrap();
                    assert_parent_sentinels(&captured);
                    assert_eq!(stream.next().await.unwrap().unwrap().sequence, 0);
                    assert_parent_sentinels(&captured);
                    if follow_enabled {
                        let spans = captured.0.lock().unwrap();
                        let follow = spans
                            .spans.iter()
                            .find(|span| span.name == "acyclic.stream.http.follow")
                            .unwrap();
                        assert!(
                            !follow.fields.contains_key("outcome"),
                            "successful child requests cannot claim the follow completed"
                        );
                        assert!(!follow.fields.contains_key("error.kind"));
                    }
                    if fails {
                        assert_eq!(
                            stream.next().await.unwrap().unwrap_err(),
                            StreamError::AccessDenied
                        );
                        assert!(stream.next().await.is_none());
                    }
                    drop(stream);
                    assert_parent_sentinels(&captured);
                    if follow_enabled {
                        let spans = captured.0.lock().unwrap();
                        let follow = spans
                            .spans.iter()
                            .find(|span| span.name == "acyclic.stream.http.follow")
                            .unwrap();
                        assert!(follow.closed);
                        assert_eq!(
                            follow.fields["terminal"],
                            if fails { "error" } else { "dropped" }
                        );
                        if fails {
                            assert_eq!(follow.fields["outcome"], "err");
                            assert_eq!(follow.fields["error.kind"], "access_denied");
                        } else {
                            assert!(!follow.fields.contains_key("outcome"));
                        }
                    }
                }
                .instrument(parent)
                .await;
                assert_parent_sentinels(&captured);
                assert_safe_fields(&captured);
            }
            .with_subscriber(subscriber)
            .await;
        }
    }
}

#[tokio::test]
async fn visible_follow_descendants_stay_with_the_originating_subscriber() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    let origin = Capture::default();
    let consumer = Capture::default();
    let (server, mut stream) = async {
        let server = Server::new(vec![Reply::ok(json!("1")), Reply::ok(page(0, 1))]).await;
        let (delay, _) = clock();
        let stream = server
            .client
            .follow_with_delay(path(), 0, delay)
            .await
            .unwrap();
        (server, stream)
    }
    .with_subscriber(tracing_subscriber::registry().with(origin.clone()))
    .await;
    async {
        assert_eq!(stream.next().await.unwrap().unwrap().sequence, 0);
        drop(stream);
    }
    .with_subscriber(tracing_subscriber::registry().with(consumer.clone()))
    .await;
    assert_eq!(server.requests().len(), 2);
    {
        let spans = origin.0.lock().unwrap();
        let (follow_id, follow) = spans
            .spans.iter().enumerate()
            .find(|(_, span)| span.name == "acyclic.stream.http.follow")
            .unwrap();
        assert!(follow.closed);
        assert_eq!(follow.fields["terminal"], "dropped");
        assert!(!follow.fields.contains_key("outcome"));
        let (poll_id, poll) = spans
            .spans.iter().enumerate()
            .find(|(_, span)| span.name == "acyclic.stream.http.follow.poll")
            .unwrap();
        assert_eq!(poll.parent, Some(follow_id));
        assert!(poll.closed);
        let read = spans
            .spans.iter()
            .find(|span| {
                span.name == "acyclic.stream.http.call"
                    && span
                        .fields
                        .get("route")
                        .is_some_and(|route| route == "read")
            })
            .unwrap();
        assert_eq!(read.parent, Some(poll_id));
        assert!(read.closed);
        assert_eq!(read.fields["outcome"], "ok");
    }
    assert!(
        consumer.0.lock().unwrap().spans.iter().all(|span| {
            span.name != "acyclic.stream.http.follow.poll"
                && !(span.name == "acyclic.stream.http.call"
                    && span
                        .fields
                        .get("route")
                        .is_some_and(|route| route == "read"))
        }),
        "consumption must not move follow descendants to a different subscriber"
    );
    assert_safe_fields(&origin);
    assert_safe_fields(&consumer);
}

#[tokio::test]
async fn truncated_http_body_yields_one_error_then_eof_without_retry() {
    let server = Server::with_body_mode(
        vec![Reply::ok(json!("1")), Reply::ok(page(0, 1))],
        Some(BodyMode::Truncated),
    )
    .await;
    let (delay, mut gates) = clock();
    let mut stream = server
        .client
        .follow_with_delay(path(), 0, delay)
        .await
        .unwrap();
    assert_eq!(
        stream.next().await.unwrap().unwrap_err(),
        StreamError::Unavailable
    );
    assert!(stream.next().await.is_none());
    assert_eq!(server.requests().len(), 2);
    assert!(gates.try_recv().is_err());
}

#[tokio::test]
async fn drop_during_startup_tail_body_closes_connection_and_follow_span() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    let captured = Capture::default();
    let subscriber = tracing_subscriber::registry().with(captured.clone());
    async {
        let (prefix_written, prefix) = oneshot::channel();
        let (peer_closed, closed) = oneshot::channel();
        let server = Server::with_body_mode_at(
            vec![Reply::ok(json!("0"))],
            Some(BodyMode::Held { prefix_written, peer_closed }),
            "/v1/stream/tail",
        ).await;
        let (delay, mut gates) = clock();
        {
            let follow = server.client.follow_with_delay(path(), 0, delay);
            tokio::pin!(follow);
            tokio::time::timeout(Duration::from_secs(10), async {
                tokio::select! {
                    _ = &mut follow => panic!("startup tail body must remain pending"),
                    result = prefix => result.unwrap(),
                }
                tokio::select! {
                    _ = &mut follow => panic!("startup tail body must remain pending after headers"),
                    () = until_status(&captured, "tail") => {},
                }
            }).await.unwrap();
        } // Destroy the pinned future, including the unfinished tail response.
        tokio::time::timeout(Duration::from_secs(10), closed).await.unwrap().unwrap();
        assert_eq!(server.requests(), vec![("/v1/stream/tail".into(), json!({"path":"follow/test"}))]);
        assert!(gates.recv().await.is_none(), "startup cancellation never creates an idle wait");
        {
            let spans = captured.0.lock().unwrap();
            let follow = spans.spans.iter().find(|span| span.name == "acyclic.stream.http.follow").unwrap();
            assert!(follow.closed);
            assert_eq!(follow.fields["phase"], "tail");
            assert_eq!(follow.fields["terminal"], "dropped");
            assert!(!follow.fields.contains_key("outcome"));
            assert!(spans.spans.iter().filter(|span| span.name == "acyclic.stream.http.call").all(|span| span.closed));
        }
        assert_cancelled_call(&captured, "tail");
        assert_safe_fields(&captured);
    }.with_subscriber(subscriber).await;
}

#[tokio::test]
async fn drop_while_http_body_is_pending_closes_connection_and_read_spans() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    let captured = Capture::default();
    let subscriber = tracing_subscriber::registry().with(captured.clone());
    async {
        let (prefix_written, prefix) = oneshot::channel();
        let (peer_closed, closed) = oneshot::channel();
        let server = Server::with_body_mode(
            vec![Reply::ok(json!("1")), Reply::ok(page(0, 1))],
            Some(BodyMode::Held {
                prefix_written,
                peer_closed,
            }),
        )
        .await;
        let (delay, mut gates) = clock();
        let mut stream = server
            .client
            .follow_with_delay(path(), 0, delay)
            .await
            .unwrap();
        pending_read(&mut stream, &captured, prefix).await;
        {
            let spans = captured.0.lock().unwrap();
            let follow = spans
                .spans.iter()
                .find(|span| span.name == "acyclic.stream.http.follow")
                .unwrap();
            assert!(!follow.closed);
            assert_eq!(follow.fields["phase"], "read");
        }
        drop(stream);
        // A real-time failure watchdog does not drive the follow clock or success path.
        tokio::time::timeout(Duration::from_secs(10), closed)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(server.requests().len(), 2);
        assert!(gates.recv().await.is_none());
        {
            let spans = captured.0.lock().unwrap();
            let (follow_id, follow) = spans
                .spans.iter().enumerate()
                .find(|(_, span)| span.name == "acyclic.stream.http.follow")
                .unwrap();
            assert!(follow.closed);
            assert_eq!(follow.fields["phase"], "read");
            assert_eq!(follow.fields["terminal"], "dropped");
            assert!(!follow.fields.contains_key("outcome"));
            assert!(
                spans
                    .spans.iter()
                    .any(|span| span.name == "acyclic.stream.http.follow.poll"
                        && span.parent == Some(follow_id)
                        && span.closed)
            );
            assert!(
                spans
                    .spans.iter()
                    .filter(|span| span.name == "acyclic.stream.http.call")
                    .all(|span| span.closed)
            );
        }
        assert_cancelled_call(&captured, "read");
        assert_safe_fields(&captured);
    }
    .with_subscriber(subscriber)
    .await;
}

async fn pending_read(
    stream: &mut RecordStream,
    captured: &Capture,
    prefix: oneshot::Receiver<()>,
) {
    // Failure watchdog only; success is driven by body and subscriber barriers.
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            item = stream.next() => panic!("body must remain pending, got {item:?}"),
            result = prefix => result.unwrap(),
        }
        // Status is recorded after response headers; the held remainder keeps chunk pending.
        tokio::select! {
            item = stream.next() => panic!("body must remain pending after headers, got {item:?}"),
            () = until_status(&captured, "read") => {},
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn foreign_dispatch_drop_and_unwind_close_real_parents_and_pending_body() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    for unwind in [false, true] {
        let origin = Capture::default();
        let consumer = Capture::default();
        let origin_dispatch =
            tracing::Dispatch::new(tracing_subscriber::registry().with(origin.clone()));
        let (prefix_written, prefix) = oneshot::channel();
        let (peer_closed, closed) = oneshot::channel();
        let server = Server::with_body_mode(
            vec![Reply::ok(json!("1")), Reply::ok(page(0, 1))],
            Some(BodyMode::Held {
                prefix_written,
                peer_closed,
            }),
        )
        .await;
        let (delay, mut gates) = clock();
        let mut stream = async {
            let parent = tracing::info_span!("test.origin_parent");
            server
                .client
                .follow_with_delay(path(), 0, delay)
                .instrument(parent)
                .await
                .unwrap()
        }
        .with_subscriber(origin_dispatch.clone())
        .await;
        // The origin parent handle is gone; only its children retain it.
        pending_read(&mut stream, &origin, prefix)
            .with_subscriber(origin_dispatch)
            .await;
        async {
            let parent = tracing::info_span!("test.consumer_parent");
            let _entered = parent.enter();
            if unwind {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let _owned = stream;
                    panic!("intentional pending cursor unwind");
                }));
                let payload = result.unwrap_err();
                assert_eq!(
                    payload.downcast_ref::<&str>(),
                    Some(&"intentional pending cursor unwind")
                );
            } else {
                drop(stream);
            }
        }
        .with_subscriber(tracing_subscriber::registry().with(consumer.clone()))
        .await;
        tokio::time::timeout(Duration::from_secs(10), closed)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(server.requests().len(), 2);
        assert!(gates.recv().await.is_none());
        assert_cancelled_call(&origin, "read");
        {
            let spans = origin.0.lock().unwrap();
            assert!(spans.spans.iter().any(|span| span.name == "test.origin_parent"));
            assert!(spans.spans.iter().all(|span| span.closed));
            let follow = spans
                .spans.iter()
                .find(|span| span.name == "acyclic.stream.http.follow")
                .unwrap();
            assert_eq!(follow.fields["phase"], "read");
            assert_eq!(follow.fields["terminal"], "dropped");
            assert!(!follow.fields.contains_key("outcome"));
        }
        {
            let spans = consumer.0.lock().unwrap();
            assert!(
                spans
                    .spans.iter()
                    .any(|span| span.name == "test.consumer_parent")
            );
            assert!(spans.spans.iter().all(|span| span.closed));
        }
        assert_safe_fields(&origin);
        assert_safe_fields(&consumer);
    }
}

#[tokio::test]
async fn traces_follow_cursor_lifetime_and_distinguish_drop_from_error() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    for fails in [false, true] {
        let captured = Capture::default();
        let subscriber = tracing_subscriber::registry().with(captured.clone());
        async {
            let reply = if fails {
                Reply(403, json!({"code":"access_denied"}))
            } else {
                Reply::ok(json!([]))
            };
            let server = Server::new(vec![Reply::ok(json!("0")), reply]).await;
            let (delay, mut gates) = clock();
            let mut stream = server
                .client
                .follow_with_delay(path(), 0, delay)
                .await
                .unwrap();
            let follow_id = {
                let spans = captured.0.lock().unwrap();
                let (id, span) = spans
                    .spans.iter().enumerate()
                    .find(|(_, span)| span.name == "acyclic.stream.http.follow")
                    .unwrap();
                assert!(!span.closed, "constructor must not close the cursor span");
                id
            };
            if fails {
                assert_eq!(
                    stream.next().await.unwrap().unwrap_err(),
                    StreamError::AccessDenied
                );
                assert!(stream.next().await.is_none());
            } else {
                let mut release = idle(&mut stream, &mut gates).await;
                {
                    let spans = captured.0.lock().unwrap();
                    let follow = &spans.spans[follow_id];
                    assert!(!follow.closed);
                    assert_eq!(follow.fields["phase"], "sleep");
                    assert!(
                        spans
                            .spans.iter()
                            .any(|span| span.name == "acyclic.stream.http.follow.sleep"
                                && span.parent == Some(follow_id)
                                && !span.closed)
                    );
                }
                drop(stream);
                release.closed().await;
                let spans = captured.0.lock().unwrap();
                let follow = &spans.spans[follow_id];
                assert!(follow.closed);
                assert_eq!(follow.fields["terminal"], "dropped");
                assert!(
                    !follow.fields.contains_key("outcome"),
                    "drop cannot claim success or cancellation"
                );
                assert!(
                    spans
                        .spans.iter()
                        .filter(|span| span.name == "acyclic.stream.http.follow.sleep")
                        .all(|span| span.closed)
                );
                return;
            }
            drop(stream);
            let spans = captured.0.lock().unwrap();
            let follow = &spans.spans[follow_id];
            assert!(follow.closed);
            assert_eq!(follow.fields["terminal"], "error");
            assert_eq!(follow.fields["outcome"], "err");
            assert_eq!(follow.fields["error.kind"], "access_denied");
            assert!(
                spans
                    .spans.iter()
                    .any(|span| span.name == "acyclic.stream.http.follow.poll"
                        && span.parent == Some(follow_id)
                        && span.closed)
            );
        }
        .with_subscriber(subscriber)
        .await;
        assert_safe_fields(&captured);
    }
}

#[tokio::test]
async fn traces_buffered_drop_and_startup_error_without_claiming_success() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    for startup_error in [false, true] {
        let captured = Capture::default();
        let subscriber = tracing_subscriber::registry().with(captured.clone());
        async {
            let replies = if startup_error {
                vec![Reply(404, json!({"code":"stream_not_found"}))]
            } else {
                vec![Reply::ok(json!("2")), Reply::ok(page(0, 2))]
            };
            let server = Server::new(replies).await;
            let (delay, _) = clock();
            let result = server.client.follow_with_delay(path(), 0, delay).await;
            if startup_error {
                assert!(matches!(result, Err(StreamError::NotFound)));
            } else {
                let mut stream = result.unwrap();
                assert_eq!(stream.next().await.unwrap().unwrap().sequence, 0);
                {
                    let spans = captured.0.lock().unwrap();
                    let follow = spans
                        .spans.iter()
                        .find(|span| span.name == "acyclic.stream.http.follow")
                        .unwrap();
                    assert!(!follow.closed);
                    assert_eq!(follow.fields["phase"], "consumer");
                    assert_eq!(follow.fields["queued"], "1");
                    assert_eq!(follow.fields["rev"], "1");
                }
                drop(stream);
                assert_eq!(server.requests().len(), 2);
            }
            let spans = captured.0.lock().unwrap();
            let follow = spans
                .spans.iter()
                .find(|span| span.name == "acyclic.stream.http.follow")
                .unwrap();
            assert!(follow.closed);
            if startup_error {
                assert_eq!(follow.fields["terminal"], "error");
                assert_eq!(follow.fields["error.kind"], "not_found");
            } else {
                assert_eq!(follow.fields["terminal"], "dropped");
                assert!(!follow.fields.contains_key("outcome"));
            }
        }
        .with_subscriber(subscriber)
        .await;
        assert_safe_fields(&captured);
    }
}
