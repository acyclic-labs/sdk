use super::*;

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
        std::sync::Arc::new(super::super::super::runtime::PlatformExecutor),
    );
    client.register_credential(0, crate::session::testing::credential())?;
    let mut records = client
        .follow(
            StreamPath::new("events")?,
            1,
            crate::FollowFilter::default(),
        )
        .await?;
    let connections = client.connections();
    let first = connections.changed(0).await?;
    fixture.reset()?;
    let recovered = tokio::time::timeout(Duration::from_secs(2), async {
        let connected = connections.changed(first);
        let closed = records.next();
        futures::pin_mut!(connected, closed);
        match future::select(connected, closed).await {
            future::Either::Left((Ok(next), _)) => next > first,
            _ => false,
        }
    })
    .await?;
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
