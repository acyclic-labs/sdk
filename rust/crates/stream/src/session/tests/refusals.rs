use super::*;

#[derive(Clone, Copy)]
enum Phase {
    Prepare,
    Open,
    Output,
    Input,
}

struct Rejected {
    phase: Phase,
    error: StreamError,
}

impl SocketProvider for Rejected {
    fn prepare(self: Arc<Self>, _: Bytes) -> Result<Box<dyn PreparedSocketOpen>, StreamError> {
        if matches!(self.phase, Phase::Prepare) {
            return Err(self.error.clone());
        }
        Ok(Box::new(self))
    }
}

impl PreparedSocketOpen for Arc<Rejected> {
    fn resource(&self) -> &str {
        "actors:/refusal"
    }

    fn open(
        self: Box<Self>,
        authorization: &dyn SocketAuthorization,
    ) -> Result<SocketChannel, StreamError> {
        authorization.authorize_socket(b"account", self.resource())?;
        if matches!(self.phase, Phase::Open) {
            return Err(self.error.clone());
        }
        let terminal = if matches!(self.phase, Phase::Output) {
            stream::once(future::ready(Err(self.error.clone()))).boxed()
        } else {
            stream::pending().boxed()
        };
        Ok(SocketChannel {
            outbound: stream::once(future::ready(Ok(Bytes::from_static(b"accepted"))))
                .chain(terminal)
                .boxed(),
            inbound: Box::new((*self).clone()),
        })
    }
}

impl SocketInbound for Arc<Rejected> {
    fn receive(&self, _: Bytes) -> Result<(), StreamError> {
        Err(self.error.clone())
    }
}

#[tokio::test]
async fn local_socket_refusals_retain_the_original_error_after_prior_data() -> TestResult {
    // Capture a real OS refusal at runtime, then retain the shared status
    // producer's original error across every local socket failure boundary.
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    let refused = tokio::net::TcpStream::connect(address).await.unwrap_err();
    let status = tonic::Status::from_error(Box::new(refused));
    let original = crate::grpc::status(&status);
    assert!(original.member_refusal().is_some());
    assert!(
        std::iter::successors(std::error::Error::source(&original), |source| source
            .source())
        .any(|source| source.is::<std::io::Error>())
    );
    // A member refusal crosses the close boundary with its original status
    // message, even while it retains a local source; only a local cause with
    // no member refusal is rendered there.
    let member = status.message().to_owned();
    let local = StreamError::Unavailable.with_source(status);
    let rendered = local.to_string();
    for (original, expected) in [(original, member), (local, rendered)] {
        for phase in [Phase::Prepare, Phase::Open, Phase::Output, Phase::Input] {
            let client = Client::in_process_with_sockets(
                Arc::new(MemoryStream::default()),
                testing::authority(),
                None,
                Arc::new(Rejected {
                    phase,
                    error: original.clone(),
                }),
                Arc::new(crate::runtime::PlatformExecutor),
                None,
            );
            client.register_credential(
                0,
                testing::credential_with_grants(&[acyclic_auth::contract::ResourceGrant {
                    resource: acyclic_auth::contract::ResourcePattern {
                        name: "actors:/refusal".into(),
                        prefix: false,
                    },
                    actions: vec!["actors/invoke".into()],
                }]),
            )?;
            let actual = match client.actor_socket(Bytes::from_static(b"open"), 0).await {
                Err(error) => error,
                Ok(mut socket) => {
                    assert_eq!(socket.next().await?, Some(Bytes::from_static(b"accepted")));
                    if matches!(phase, Phase::Input) {
                        socket.send(Bytes::from_static(b"input")).await?;
                    }
                    socket.next().await.unwrap_err()
                }
            };
            assert_eq!(
                actual.category(),
                original.category(),
                "local socket lost its original refusal"
            );
            assert_eq!(
                actual.member_refusal(),
                original.member_refusal(),
                "local socket lost the original refusal details"
            );
        }
        // Hold both credits at zero so the terminal must retain the same error
        // while earlier accepted data owns the queue. Drive the actual queue and
        // credit transitions, rather than substituting another close producer.
        let mut session = super::properties::empty_session(false)?;
        let identity = wire::SessionChannel {
            channel_id: 1,
            generation: 1,
        };
        session.record(
            identity,
            Ok(Event::Socket {
                next: 1,
                bytes: Bytes::from_static(b"queued"),
            }),
        )?;
        session.record(identity, Err(original.clone()))?;
        assert!(session.ready()?.is_none());
        assert!(session.controls.is_empty());
        session.receive(window(0))?;
        session.receive(window(1))?;
        let data = session.ready()?.ok_or("accepted data was dropped")?;
        assert!(matches!(&data.wire().body, Some(Body::Data(data))
        if matches!(&data.item, Some(Item::ActorSocket(bytes)) if bytes.as_ref() == b"queued")));
        let (closed, retained) = session
            .controls
            .pop_front()
            .ok_or("terminal was dropped")?
            .into_parts();
        let Some(Body::Closed(closed)) = closed.body else {
            return Err("failure was not encoded as a session close".into());
        };
        assert_eq!(closed.code, original.code());
        assert_eq!(closed.message, expected);
        assert_eq!(
            retained.map(|error| error.category()),
            Some(original.category()),
            "queued terminal lost the original refusal"
        );
        assert_eq!(
            retained.as_ref().and_then(|error| error.member_refusal()),
            original.member_refusal(),
            "queued terminal lost the original refusal details"
        );
        assert!(session.channels.is_empty());
        assert!(session.controls.is_empty());
    }
    Ok(())
}
