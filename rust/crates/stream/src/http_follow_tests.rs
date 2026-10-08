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
    async fn with_body_mode(replies: Vec<Reply>, mut mode: Option<BodyMode>) -> Self {
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
                let read = route == "/v1/stream/read";
                observed.lock().unwrap().push((route, body));
                let Reply(status, value) = replies
                    .pop_front()
                    .unwrap_or(Reply(500, json!({"code":"unavailable"})));
                let body = serde_json::to_vec(&value).unwrap();
                socket.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
                match if read { mode.take() } else { None } {
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

#[derive(Default, Clone)]
struct CapturedSpans(Arc<Mutex<std::collections::HashMap<u64, CapturedSpan>>>);
#[derive(Default)]
struct CapturedSpan {
    name: &'static str,
    parent: Option<u64>,
    declared_fields: Vec<&'static str>,
    fields: std::collections::HashMap<String, String>,
    closed: bool,
}
struct Fields<'a>(&'a mut std::collections::HashMap<String, String>);
impl tracing::field::Visit for Fields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}
impl<S> tracing_subscriber::Layer<S> for CapturedSpans
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attributes: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut captured = CapturedSpan {
            name: attributes.metadata().name(),
            declared_fields: attributes
                .metadata()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect(),
            parent: context
                .span(id)
                .and_then(|span| span.parent().map(|parent| parent.id().into_u64())),
            ..CapturedSpan::default()
        };
        // Check at creation as well: registry IDs may be reused after close.
        for field in &captured.declared_fields {
            assert!(
                !["path", "token", "content", "body", "authorization", "error"].contains(field),
                "forbidden trace field {field} on {}",
                captured.name
            );
        }
        attributes.record(&mut Fields(&mut captured.fields));
        self.0.lock().unwrap().insert(id.into_u64(), captured);
    }
    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        _: tracing_subscriber::layer::Context<'_, S>,
    ) {
        values.record(&mut Fields(
            &mut self
                .0
                .lock()
                .unwrap()
                .get_mut(&id.into_u64())
                .unwrap()
                .fields,
        ));
    }
    fn on_close(&self, id: tracing::span::Id, _: tracing_subscriber::layer::Context<'_, S>) {
        self.0
            .lock()
            .unwrap()
            .get_mut(&id.into_u64())
            .unwrap()
            .closed = true;
    }
}
impl CapturedSpans {
    fn assert_safe_fields(&self) {
        for span in self.0.lock().unwrap().values() {
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
async fn drop_while_http_body_is_pending_closes_connection_and_read_spans() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    let captured = CapturedSpans::default();
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
        tokio::select! {
            item = stream.next() => panic!("body must remain pending, got {item:?}"),
            result = prefix => result.unwrap(),
        }
        // Poll once with the prefix written; no complete body can be delivered.
        assert!(futures::poll!(stream.next()).is_pending());
        {
            let spans = captured.0.lock().unwrap();
            let follow = spans
                .values()
                .find(|span| span.name == "acyclic.stream.http.follow")
                .unwrap();
            assert!(!follow.closed);
            assert_eq!(follow.fields["phase"], "\"read\"");
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
                .iter()
                .find(|(_, span)| span.name == "acyclic.stream.http.follow")
                .unwrap();
            assert!(follow.closed);
            assert_eq!(follow.fields["phase"], "\"read\"");
            assert_eq!(follow.fields["terminal"], "\"dropped\"");
            assert!(!follow.fields.contains_key("outcome"));
            assert!(
                spans
                    .values()
                    .any(|span| span.name == "acyclic.stream.http.follow.poll"
                        && span.parent == Some(*follow_id)
                        && span.closed)
            );
            assert!(
                spans
                    .values()
                    .filter(|span| span.name == "acyclic.stream.http.call")
                    .all(|span| span.closed)
            );
        }
        captured.assert_safe_fields();
    }
    .with_subscriber(subscriber)
    .await;
}

#[tokio::test]
async fn traces_follow_cursor_lifetime_and_distinguish_drop_from_error() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    for fails in [false, true] {
        let captured = CapturedSpans::default();
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
                    .iter()
                    .find(|(_, span)| span.name == "acyclic.stream.http.follow")
                    .unwrap();
                assert!(!span.closed, "constructor must not close the cursor span");
                *id
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
                    let follow = &spans[&follow_id];
                    assert!(!follow.closed);
                    assert_eq!(follow.fields["phase"], "\"sleep\"");
                    assert!(
                        spans
                            .values()
                            .any(|span| span.name == "acyclic.stream.http.follow.sleep"
                                && span.parent == Some(follow_id)
                                && !span.closed)
                    );
                }
                drop(stream);
                release.closed().await;
                let spans = captured.0.lock().unwrap();
                let follow = &spans[&follow_id];
                assert!(follow.closed);
                assert_eq!(follow.fields["terminal"], "\"dropped\"");
                assert!(
                    !follow.fields.contains_key("outcome"),
                    "drop cannot claim success or cancellation"
                );
                assert!(
                    spans
                        .values()
                        .filter(|span| span.name == "acyclic.stream.http.follow.sleep")
                        .all(|span| span.closed)
                );
                return;
            }
            drop(stream);
            let spans = captured.0.lock().unwrap();
            let follow = &spans[&follow_id];
            assert!(follow.closed);
            assert_eq!(follow.fields["terminal"], "\"error\"");
            assert_eq!(follow.fields["outcome"], "\"err\"");
            assert_eq!(follow.fields["error.kind"], "\"access_denied\"");
            assert!(
                spans
                    .values()
                    .any(|span| span.name == "acyclic.stream.http.follow.poll"
                        && span.parent == Some(follow_id)
                        && span.closed)
            );
        }
        .with_subscriber(subscriber)
        .await;
        captured.assert_safe_fields();
    }
}

#[tokio::test]
async fn traces_buffered_drop_and_startup_error_without_claiming_success() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    for startup_error in [false, true] {
        let captured = CapturedSpans::default();
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
                        .values()
                        .find(|span| span.name == "acyclic.stream.http.follow")
                        .unwrap();
                    assert!(!follow.closed);
                    assert_eq!(follow.fields["phase"], "\"consumer\"");
                    assert_eq!(follow.fields["queued"], "1");
                    assert_eq!(follow.fields["rev"], "1");
                }
                drop(stream);
                assert_eq!(server.requests().len(), 2);
            }
            let spans = captured.0.lock().unwrap();
            let follow = spans
                .values()
                .find(|span| span.name == "acyclic.stream.http.follow")
                .unwrap();
            assert!(follow.closed);
            if startup_error {
                assert_eq!(follow.fields["terminal"], "\"error\"");
                assert_eq!(follow.fields["error.kind"], "\"not_found\"");
            } else {
                assert_eq!(follow.fields["terminal"], "\"dropped\"");
                assert!(!follow.fields.contains_key("outcome"));
            }
        }
        .with_subscriber(subscriber)
        .await;
        captured.assert_safe_fields();
    }
}
