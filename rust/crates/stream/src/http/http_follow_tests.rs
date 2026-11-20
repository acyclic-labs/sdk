//! JSON unary framing, cancellation and observation regressions.
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "Test fixtures and assertions must fail immediately on malformed inputs or violated expectations"
)]
use super::*;
use crate::StreamCategory;

use prost::Message;
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
        Self::with_body_mode_at(replies, mode, "/v1/stream/idempotency/inspect").await
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
                observed
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .push((route, body));
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
        self.requests
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
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
async fn missing_idempotency() -> (IdempotencyKey, Reply) {
    let key = IdempotencyKey::unique();
    // Capture the real provider's absent observation with the shipping codec.
    let observation = MemoryStream::default()
        .inspect_idempotency(key.clone())
        .await
        .unwrap();
    let response = wire::InspectIdempotencyResponse {
        observation: observation.map(crate::wire_codec::observation_wire),
    };
    let response = crate::http_response::value("idempotency/inspect", &response.encode_to_vec())
        .unwrap()
        .unwrap_or(Value::Null);
    (key, Reply::ok(response))
}

#[tokio::test]
async fn request_path_typed_append_preserves_json_responses_and_validation() {
    let server = Server::new(vec![
            Reply::ok(json!({"ok":true,"start":"0","end":"1","tail":"1","commitId":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","revision":{"scope":"AQ==","incarnation":"AAAAAAAAAAAAAAAAAAAAAQ==","index":"1"}})),
            Reply::ok(json!({"ok":false,"code":"tail_conflict","actualTail":"1"})),
        ]).await;
    let mut request = AppendRequest {
        path: path(),
        records: Vec::new(),
        if_tail: Some(0),
        idempotency_key: IdempotencyKey::new(bytes::Bytes::from_static(b"key")).unwrap(),
    };
    assert_eq!(
        server
            .client
            .append(request.clone())
            .await
            .map_err(|error| error.category()),
        Err(StreamCategory::LimitExceeded)
    );
    assert!(server.requests().is_empty());
    request
        .records
        .push((OwnedBytes::from_static(b"\x00\xff")).into());
    assert!(matches!(
        server.client.append(request.clone()).await.unwrap(),
        AppendOutcome::Committed(AppendReceipt {
            start: 0,
            end: 1,
            tail: 1,
            ..
        })
    ));
    assert_eq!(
        server.client.append(request).await.unwrap(),
        AppendOutcome::TailConflict { actual_tail: 1 }
    );
    let expected = (
        "/v1/stream/append".into(),
        json!({
            "path":"follow/test", "records":[{"value":"AP8=","keys":[],"attributes":[]}],
            "options":{"ifTail":"0","idempotencyKey":"a2V5"},
        }),
    );
    assert_eq!(server.requests(), vec![expected.clone(), expected]);
}

#[tokio::test]
async fn truncated_unary_body_fails_without_retry() {
    let (key, reply) = missing_idempotency().await;
    let server = Server::with_body_mode(vec![reply], Some(BodyMode::Truncated)).await;
    let error = server
        .client
        .inspect_idempotency(key)
        .await
        .expect_err("truncated original body cannot settle");
    assert_eq!(error.category(), StreamCategory::Unavailable);
    assert!(error.uncertain());
    assert_eq!(server.requests().len(), 1);
    drop(server);
    assert!(
        std::iter::successors(std::error::Error::source(&error), |source| source.source())
            .any(|source| source.is::<reqwest::Error>())
    );
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_pending_unary_body_closes_the_connection() {
    let (prefix_written, prefix) = oneshot::channel();
    let (peer_closed, closed) = oneshot::channel();
    let (key, reply) = missing_idempotency().await;
    let server = Server::with_body_mode(
        vec![reply],
        Some(BodyMode::Held {
            prefix_written,
            peer_closed,
        }),
    )
    .await;
    #[cfg(feature = "serde")]
    let trace = Stream::new(
        Arc::new(MemoryStream::default()),
        "agents/http-cancel/trace",
    )
    .unwrap();
    #[cfg(feature = "serde")]
    let recorder = Arc::new(crate::trace::Recorder::new(trace.clone(), [95; 16]));
    #[cfg(feature = "serde")]
    let root = recorder
        .start(
            "http-owner",
            crate::trace::Mechanism::ToolCall,
            None,
            Some("original_request"),
        )
        .await
        .unwrap()
        .into_owned();
    #[cfg(feature = "serde")]
    let parent = root.id();
    {
        let reading = server.client.inspect_idempotency(key);
        #[cfg(feature = "serde")]
        let reading = root.within(reading);
        tokio::pin!(reading);
        tokio::select! {
            result = &mut reading => panic!("body completed while held: {}", result.is_ok()),
            ready = prefix => ready.unwrap(),
        }
        tokio::time::advance(std::time::Duration::from_secs(31)).await;
        assert!(
            futures::poll!(reading.as_mut()).is_pending(),
            "the original held response acquired an implicit request deadline"
        );
    }
    closed.await.unwrap();
    assert_eq!(server.requests().len(), 1);
    #[cfg(feature = "serde")]
    {
        use crate::trace::{Mechanism, Observation, Outcome, TraceQuery};
        let _completion = root.finish_submitted(None, Outcome::Succeeded).unwrap();
        let mut query = TraceQuery::new(
            trace.follow(0, Default::default()).await.unwrap(),
            Default::default(),
        );
        let mut completed = 0;
        let mut cancelled = false;
        while completed < 2 {
            if let Observation::Completed {
                id,
                start,
                bytes,
                result,
                ..
            } = query
                .next()
                .await
                .unwrap()
                .expect("original trace must remain readable")
            {
                completed += 1;
                if id == parent {
                    assert_eq!(result, Outcome::Succeeded);
                } else {
                    assert!(
                        !cancelled,
                        "one physical request emitted duplicate terminals"
                    );
                    cancelled = true;
                    assert_eq!(start.parent, Some(parent));
                    assert_eq!(start.mechanism, Mechanism::RoundTrip);
                    assert_eq!(result, Outcome::Unsettled);
                    assert_eq!(bytes, None, "a partial response has no settled body size");
                }
            }
        }
        assert!(cancelled, "dropped HTTP work was reported as settled");
    }
}

#[cfg(feature = "serde")]
#[tokio::test]
async fn physical_http_exchange_has_owned_round_trip_and_original_body_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::trace::{Mechanism, Observation, Outcome, Recorder, TraceQuery};
    let response = json!({"ok":false,"code":"tail_conflict","actualTail":"1"});
    let response_bytes = serde_json::to_vec(&response)?.len() as u64;
    let mut server = Server::new(vec![Reply::ok(response.clone()), Reply::ok(response.clone()),
        Reply::ok(json!({"ok":true,"start":"0","end":"1","tail":"1","commitId":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}))]).await;
    let resolutions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let calls = resolutions.clone();
    server.client.credential = Some(CredentialProvider::new(move || {
        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        futures::future::ready(Ok("original-secret-credential".into()))
    }));
    let trace = Stream::new(Arc::new(MemoryStream::default()), "agents/http/trace")?;
    assert_eq!(
        server
            .client
            .request_json("append", || Ok("{}".into()), Ok)
            .await?,
        response
    );
    assert!(
        matches!(trace.tail().await, Err(error) if error.category() == StreamCategory::NotFound)
    );
    let recorder = Arc::new(Recorder::new(trace.clone(), [94; 16]));
    let root = recorder
        .start(
            "http-owner",
            Mechanism::ToolCall,
            None,
            Some("original_request"),
        )
        .await?
        .into_owned();
    let parent = root.id();
    assert!(
        root.within(
            server
                .client
                .request_json("append", || Err("limit_exceeded"), Ok)
        )
        .await
        .is_err()
    );
    assert_eq!(server.requests().len(), 1);
    assert_eq!(
        root.within(server.client.request_json("append", || Ok("{}".into()), Ok))
            .await?,
        response
    );
    let original_credential = server
        .client
        .credential
        .replace(CredentialProvider::new(|| {
            futures::future::ready(Err(acyclic_auth::credential::CredentialError::Invalid))
        }));
    let refused = root
        .within(server.client.request_json("append", || Ok("{}".into()), Ok))
        .await
        .expect_err("credential refusal must stop dispatch");
    server.client.credential = original_credential;
    assert_eq!(refused.category(), StreamCategory::AccessDenied);
    assert_eq!(
        server.requests().len(),
        2,
        "credential refusal made a physical request"
    );
    let rejected = root
        .within(server.client.append(AppendRequest {
            path: path(),
            records: vec![OwnedBytes::from_static(b"typed-reply").into()],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::unique(),
        }))
        .await
        .expect_err("a committed append without revision must be rejected");
    assert_eq!(rejected.category(), StreamCategory::Unavailable);
    assert_eq!(server.requests().len(), 3);
    let _completion = root.finish_submitted(None, Outcome::Succeeded)?;
    let mut query = TraceQuery::new(
        trace.follow(0, Default::default()).await?,
        Default::default(),
    );
    let mut completed = Vec::new();
    while let Some(observation) = query.next().await? {
        if let Observation::Completed {
            id,
            start,
            bytes,
            result,
            ..
        } = observation
        {
            completed.push((id, start, bytes, result));
            if completed.len() == 13 {
                break;
            }
        }
    }
    let append_ids: Vec<_> = completed
        .iter()
        .filter(|(_, start, ..)| start.name == "stream.append")
        .map(|(id, ..)| *id)
        .collect();
    let exchange_ids: Vec<_> = completed
        .iter()
        .filter(|(_, start, ..)| start.name == "stream.http.request")
        .map(|(id, ..)| *id)
        .collect();
    assert_eq!(append_ids.len(), 1);
    assert_eq!(exchange_ids.len(), 2);
    for (_, start, bytes, result) in completed
        .iter()
        .filter(|(_, start, ..)| start.name == "stream.append")
    {
        assert_eq!(start.parent, Some(parent));
        assert_eq!(start.mechanism, Mechanism::Commit);
        assert_eq!(
            start.guarantee.as_deref(),
            Some("atomic_contiguous_append_decision")
        );
        assert_eq!(*bytes, None);
        assert_eq!(*result, Outcome::Unsettled);
    }
    let mut exchanges = 0;
    let mut rejected_replies = 0;
    for (_, start, bytes, result) in completed
        .iter()
        .filter(|(_, start, ..)| start.name == "stream.http.request")
    {
        exchanges += 1;
        let owner = start.parent.expect("physical exchange has an owner");
        assert!(owner == parent || append_ids.contains(&owner));
        assert_eq!(start.mechanism, Mechanism::RoundTrip);
        assert_eq!(
            start.guarantee.as_deref(),
            Some("authenticated_bounded_json_reply_validated_for_original_call")
        );
        if *result == Outcome::Succeeded {
            assert_eq!(owner, parent);
            assert_eq!(*bytes, Some(response_bytes));
        } else {
            assert_eq!(*result, Outcome::Unsettled);
            assert!(append_ids.contains(&owner));
            assert_eq!(*bytes, None);
            rejected_replies += 1;
        }
    }
    let mut auth_checks = 0;
    let mut refused_credentials = 0;
    let mut auth_root = 0;
    let mut auth_exchange = 0;
    let mut auth_append = 0;
    for (_, start, bytes, result) in completed
        .iter()
        .filter(|(_, start, ..)| start.mechanism == Mechanism::AuthCheck)
    {
        auth_checks += 1;
        assert_eq!(*bytes, None);
        assert_eq!(
            start.guarantee.as_deref(),
            Some(if start.name == "credential.resolve" {
                "original_credential_source_admission"
            } else {
                "original_credential_lease_valid_at_boundary"
            })
        );
        let owner = start.parent.expect("auth check has an owner");
        if owner == parent {
            auth_root += 1;
        } else if exchange_ids.contains(&owner) {
            auth_exchange += 1;
        } else if append_ids.contains(&owner) {
            auth_append += 1;
        } else {
            return Err("auth check has an unknown owner".into());
        }
        if *result == Outcome::Unsettled {
            assert_eq!(start.name, "credential.resolve");
            assert_eq!(owner, parent);
            refused_credentials += 1;
        } else {
            assert_eq!(*result, Outcome::Succeeded);
        }
    }
    assert_eq!(
        (
            exchanges,
            auth_checks,
            refused_credentials,
            rejected_replies
        ),
        (2, 9, 1, 1)
    );
    assert_eq!((auth_root, auth_exchange, auth_append), (3, 4, 2));
    assert_eq!(completed.iter().filter(|(id, ..)| *id == parent).count(), 1);
    assert_eq!(resolutions.load(std::sync::atomic::Ordering::SeqCst), 3);
    Ok(())
}
