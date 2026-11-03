use super::*;

/// Observes the first backoff at the production scheduler boundary. A refused
/// socket must finish the request before scheduling a retry, without a timeout.
struct BackoffEvent(std::sync::Mutex<Option<futures::channel::oneshot::Sender<u32>>>);

impl crate::runtime::Executor for BackoffEvent {
    fn spawn(&self, future: crate::BoxProviderFuture<'static, ()>) {
        crate::runtime::Executor::spawn(&crate::runtime::PlatformExecutor, future);
    }

    fn sleep(&self, milliseconds: u32) -> crate::BoxProviderFuture<'static, ()> {
        if let Some(event) = self.0.lock().unwrap().take() {
            let _ = event.send(milliseconds);
        }
        Box::pin(futures::future::pending())
    }
}

#[tokio::test]
async fn refused_endpoint_finishes_without_scheduling_a_retry() -> TestResult {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    let channel =
        tonic::transport::Endpoint::from_shared(format!("http://{address}"))?.connect_lazy();
    let (send, backoff) = futures::channel::oneshot::channel();
    let client = Client::with_transport(
        native::endpoint(channel, Some("Bearer fixture".parse()?), None),
        Arc::new(|_| Box::pin(future::ready(Err(StreamError::Unavailable)))),
        Arc::new(BackoffEvent(std::sync::Mutex::new(Some(send)))),
    );
    client.register_credential(0, crate::session::testing::credential())?;
    let request = client.heads(
        crate::HeadsRequest {
            retained: Default::default(),
            durable_through: None,
            projection: crate::HeadProjection::Tail,
            paths: [StreamPath::new("refused/test")?].into_iter().collect(),
            children: None,
            limit: 1,
            at_least: Default::default(),
        },
        0,
    );
    futures::pin_mut!(request, backoff);
    match future::select(request, backoff).await {
        future::Either::Left((result, _)) => {
            assert_eq!(result.err(), Some(StreamError::Unavailable))
        }
        future::Either::Right((event, _)) => panic!(
            "a definite refused endpoint must finish without backoff: {:?}",
            event?
        ),
    }
    Ok(())
}

struct Outage {
    endpoint: super::super::transport::Endpoint,
    calls: std::sync::Arc<AtomicUsize>,
    terminal: bool,
}
impl super::super::transport::Transport for Outage {
    fn session(
        &self,
        input: super::super::transport::Requests,
    ) -> crate::BoxProviderFuture<'_, Result<super::super::Response, StreamError>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if (1..=4).contains(&call) {
                return Err(if self.terminal {
                    StreamError::AccessDenied
                } else {
                    StreamError::Unavailable
                });
            }
            self.endpoint.session(input).await
        })
    }
}

async fn check_recovery(terminal: bool) -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", 1).await?;
    let fixture = Fixture::open(provider.clone(), "node").await?;
    let calls = std::sync::Arc::new(AtomicUsize::new(0));
    let client = Client::with_transport(
        std::sync::Arc::new(Outage {
            endpoint: native::endpoint(
                fixture._channel.clone(),
                Some("Bearer fixture".parse()?),
                None,
            ),
            calls: calls.clone(),
            terminal,
        }),
        std::sync::Arc::new(|_| Box::pin(future::ready(Err(StreamError::Unavailable)))),
        std::sync::Arc::new(crate::runtime::PlatformExecutor),
    );
    client.register_credential(0, crate::session::testing::credential())?;
    let mut records = client
        .follow(crate::FollowRequest::new(StreamPath::new("events")?, 1))
        .map_ok(crate::records_only)
        .await?;
    let connections = client.connections();
    let first = connections.changed(0).await?;
    fixture.reset()?;
    let recovered = (async {
        let connected = connections.changed(first);
        let closed = records.next();
        let connected = core::pin::pin!(connected);
        let closed = core::pin::pin!(closed);
        match future::select(connected, closed).await {
            future::Either::Left((Ok(next), _)) => next > first,
            _ => false,
        }
    })
    .await;
    assert!(
        recovered,
        "recovery must retain live follows across exhausted handshake attempts"
    );
    assert!(calls.load(Ordering::SeqCst) >= 6);
    append(&provider, "events", 1).await?;
    assert_eq!(next(&mut records).await?.sequence, 1);
    Ok(())
}

#[tokio::test]
async fn established_interests_survive_more_than_one_handshake_attempt_group() -> TestResult {
    check_recovery(false).await
}
#[tokio::test]
#[should_panic(expected = "recovery must retain live follows across exhausted handshake attempts")]
async fn negative_control_terminal_transport_failure_retires_the_live_follow() {
    check_recovery(true).await.unwrap();
}
