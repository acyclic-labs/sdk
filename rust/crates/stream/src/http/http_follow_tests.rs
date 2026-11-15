//! JSON unary framing, cancellation and observation regressions.
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "Test fixtures and assertions must fail immediately on malformed inputs or violated expectations"
)]
use super::*;

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
#[tokio::test]
async fn json_heads_refuses_incoherent_cuts_without_another_provider_request()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = MemoryStream::default();
    let source = StreamPath::new("head/source")?;
    provider
        .append(AppendRequest {
            path: source.clone(),
            records: vec![OwnedBytes::from_static(b"x").into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    provider
        .fork(ForkRequest {
            source,
            destination: StreamPath::new("head/child")?,
            at_tail: None,
            exclude: Vec::new(),
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let request = HeadsRequest {
        at: None,
        retained: Default::default(),
        durable_through: None,
        projection: Default::default(),
        paths: vec![StreamPath::new("head/child")?].into(),
        children: None,
        limit: 1,
        at_least: RevisionCut::default(),
    };
    // Capture the real fork head through the shipping wire and JSON encoders;
    // each negative control changes only its named coherence fields.
    let view = crate::wire_codec::heads_to_wire(provider.heads(request.clone()).await?)?;
    let original = view.heads[0].head.clone().unwrap();
    let captured = crate::http_response::value("heads", &view.encode_to_vec())?.unwrap();
    let request = crate::wire_codec::heads_request_to_wire(request).encode_to_vec();
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
        let mut response = captured.clone();
        let head = &mut response["heads"][0];
        head["tail"] = json!(tail.to_string());
        head["durable"] = json!(durable.to_string());
        head["last"] = sequence.map_or(Value::Null, |sequence| {
            let mut record = head["last"].clone();
            record["sequence"] = json!(sequence.to_string());
            record
        });
        head["origin"] = inherited.map_or(Value::Null, |cut| {
            let mut origin = head["origin"].clone();
            origin["forkedAt"] = json!(cut.to_string());
            origin
        });
        let server = Server::new(vec![Reply::ok(response)]).await;
        let actual = server
            .client
            .request("heads", request.clone())
            .await
            .map(|response| response["heads"][0].clone());
        let mut wire = original.clone();
        wire.tail = tail;
        wire.durable = durable;
        wire.last = sequence.map(|sequence| {
            let mut record = original.last.clone().unwrap();
            record.sequence = sequence;
            record
        });
        wire.origin = inherited.map(|cut| {
            let mut origin = original.origin.clone().unwrap();
            origin.forked_at = cut;
            origin
        });
        let expected = crate::wire_codec::head_from_wire(wire).map(|head| {
            crate::http_response::value(
                "head",
                &crate::wire_codec::head_to_wire(head).encode_to_vec(),
            )
            .unwrap()
            .unwrap()
        });
        assert_eq!(
            actual.as_ref().map_err(|error| error.category()),
            expected.as_ref().map_err(|error| error.category())
        );
        if valid {
            assert_eq!(parse_u64(&actual?["tail"])?, tail);
        } else {
            assert_eq!(
                actual.as_ref().map_err(|error| error.category()),
                Err(StreamError::InvalidResponse)
            );
            assert!(std::error::Error::source(actual.as_ref().unwrap_err()).is_some());
        }
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, "/v1/stream/heads");
    }
    Ok(())
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
        server.client.append(request.clone()).await,
        Err(StreamError::LimitExceeded)
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
    assert_eq!(error.category(), StreamError::Unavailable);
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
    let mut server = Server::new(vec![
        Reply::ok(response.clone()),
        Reply::ok(response.clone()),
    ])
    .await;
    let trace = Stream::new(Arc::new(MemoryStream::default()), "agents/http/trace")?;
    // No active owner: no diagnostic publication, even for an actual exchange.
    assert_eq!(
        server
            .client
            .request_json("append", || Ok("{}".into()))
            .await?,
        response
    );
    assert!(matches!(trace.tail().await, Err(StreamError::NotFound)));
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
                .request_json("append", || Err("limit_exceeded"))
        )
        .await
        .is_err()
    );
    assert_eq!(
        server.requests().len(),
        1,
        "encoding refusal must not make a round trip"
    );
    assert_eq!(
        root.within(server.client.request_json("append", || Ok("{}".into())))
            .await?,
        response
    );
    assert_eq!(server.requests().len(), 2);
    let original_credential = server
        .client
        .credential
        .replace(CredentialProvider::new(|| {
            futures::future::ready(Err(acyclic_auth::credential::CredentialError::Invalid))
        }));
    let refused = root
        .within(server.client.request_json("append", || Ok("{}".into())))
        .await
        .expect_err("the original credential refusal must stop dispatch");
    server.client.credential = original_credential;
    assert_eq!(refused.category(), StreamError::AccessDenied);
    assert_eq!(
        server.requests().len(),
        2,
        "credential refusal made a physical request"
    );
    let _completion = root.finish_submitted(None, Outcome::Succeeded)?;
    let mut query = TraceQuery::new(
        trace.follow(0, Default::default()).await?,
        Default::default(),
    );
    let mut completed = 0;
    let mut exchanges = 0;
    while completed < 2 {
        if let Some(Observation::Completed {
            id,
            start,
            bytes,
            result,
            ..
        }) = query.next().await?
        {
            completed += 1;
            assert_eq!(result, Outcome::Succeeded);
            if id != parent {
                exchanges += 1;
                assert_eq!(start.parent, Some(parent));
                assert_eq!(start.name, "stream.http.request");
                assert_eq!(start.mechanism, Mechanism::RoundTrip);
                assert_eq!(
                    start.guarantee.as_deref(),
                    Some("authenticated_bounded_json_response_at_original_route")
                );
                assert_eq!(bytes, Some(response_bytes));
            }
        }
    }
    assert_eq!(exchanges, 1);
    Ok(())
}
