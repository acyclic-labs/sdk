use super::*;
use futures::channel::oneshot;

/// Echoes every peer frame back, and reports when the session drops its
/// inbound half.
struct Echo {
    gone: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    /// End the socket after its greeting instead of echoing.
    ends: bool,
}

struct EchoInbound {
    frames: mpsc::UnboundedSender<Bytes>,
    gone: Option<oneshot::Sender<()>>,
}

impl SocketInbound for EchoInbound {
    fn receive(&self, frame: Bytes) -> Result<(), StreamError> {
        self.frames
            .unbounded_send(frame)
            .map_err(|_| StreamError::Unavailable)
    }
}

impl Drop for EchoInbound {
    fn drop(&mut self) {
        if let Some(gone) = self.gone.take() {
            let _observer_left = gone.send(());
        }
    }
}

impl SocketProvider for Echo {
    fn resource(&self, open: &[u8]) -> Result<String, StreamError> {
        if open != b"open" {
            return Err(StreamError::InvalidArgument);
        }
        Ok("actors:/fixture".into())
    }

    fn open(
        &self,
        _: Bytes,
        authorization: CallAuthorization,
    ) -> Result<SocketChannel, StreamError> {
        let attributes = acyclic_auth::contract::RecordAttributes::try_new(vec![])
            .map_err(|_| StreamError::InvalidArgument)?;
        authorization.authorize(
            "actors:/fixture",
            &[],
            &attributes,
            acyclic_auth::authority::PolicyDecision::Allow,
        )?;
        let (frames, echoed) = mpsc::unbounded();
        let hello = stream::once(future::ready(Ok(Bytes::from_static(b"hello"))));
        Ok(SocketChannel {
            outbound: if self.ends {
                Box::pin(hello)
            } else {
                Box::pin(hello.chain(echoed.map(Ok)))
            },
            inbound: Box::new(EchoInbound {
                frames,
                gone: self
                    .gone
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take(),
            }),
        })
    }
}

fn client(sockets: Arc<dyn SocketProvider>) -> Client {
    Client::in_process_with_sockets(
        Arc::new(MemoryStream::default()),
        testing::authority(),
        None,
        sockets,
        Arc::new(crate::runtime::PlatformExecutor),
        None,
    )
}

#[test]
fn receive_refusal_releases_owner_before_uncredited_frames_drain() -> TestResult {
    let mut session = super::properties::empty_session(false);
    let (gone, left) = oneshot::channel();
    let (frames, receiver) = mpsc::unbounded();
    drop(receiver);
    session
        .channels
        .get_mut(&1)
        .ok_or("missing channel")?
        .socket = Some(Box::new(EchoInbound {
        frames,
        gone: Some(gone),
    }));
    session.record(
        wire::SessionChannel {
            channel_id: 1,
            generation: 1,
        },
        Ok(Event::Socket {
            next: 1,
            bytes: Bytes::from_static(b"queued"),
        }),
    )?;
    assert!(session.ready()?.is_none());
    session.receive(frame(
        1,
        1,
        Body::Data(wire::SessionData {
            item: Some(Item::ActorSocket(Bytes::from_static(b"refused"))),
            ..Default::default()
        }),
    ))?;
    // The original owner's terminal event is independent of peer credit.
    assert!(matches!(
        futures::FutureExt::now_or_never(left),
        Some(Ok(()))
    ));
    let channel = session
        .channels
        .get(&1)
        .ok_or("queued channel retired early")?;
    assert!(channel.socket.is_none());
    assert_eq!(channel.pending.len(), 1);
    assert!(session.controls.is_empty());
    assert!(session.ready()?.is_none());
    // Frames already in flight remain confined to the ended channel after
    // its inbound handle has been released.
    session.receive(frame(
        1,
        1,
        Body::Data(wire::SessionData {
            item: Some(Item::ActorSocket(Bytes::from_static(b"late"))),
            ..Default::default()
        }),
    ))?;
    // Later credit still delivers the accepted frame before the failure.
    session.receive(window(0))?;
    session.receive(window(1))?;
    let sent = session.ready()?.ok_or("queued frame lost")?.into_wire();
    assert!(matches!(sent.body, Some(Body::Data(wire::SessionData {
        item: Some(Item::ActorSocket(bytes)), ..
    })) if bytes == b"queued"[..]));
    assert!(
        matches!(session.controls.pop_front().and_then(|frame| frame.body),
        Some(Body::Closed(wire::SessionClosed { code, .. })) if code == StreamError::Unavailable.code())
    );
    assert!(session.channels.is_empty());
    assert_eq!(session.pending_frames, 0);
    assert_eq!(session.pending_bytes, 0);
    Ok(())
}

#[tokio::test]
async fn socket_frames_flow_both_ways_and_dropping_the_peer_ends_the_owner() -> TestResult {
    let (gone, left) = oneshot::channel();
    let client = client(Arc::new(Echo {
        gone: std::sync::Mutex::new(Some(gone)),
        ends: false,
    }));
    client.register_credential(
        0,
        testing::credential_with_grants(vec![acyclic_auth::contract::ResourceGrant {
            resource: acyclic_auth::contract::ResourcePattern {
                name: "actors:/fixture".into(),
                prefix: false,
            },
            actions: vec!["actors/invoke".into()],
        }]),
    )?;
    let mut socket = client.actor_socket(Bytes::from_static(b"open"), 0).await?;
    assert_eq!(socket.next().await?.as_deref(), Some(&b"hello"[..]));
    for frame in [&b"one"[..], b"two"] {
        socket.send(Bytes::copy_from_slice(frame)).await?;
        assert_eq!(socket.next().await?.as_deref(), Some(frame));
    }
    // Negative control: an empty frame never reaches the owner.
    assert_eq!(
        socket.send(Bytes::new()).await,
        Err(StreamError::InvalidArgument)
    );
    drop(socket);
    left.await?;
    Ok(())
}

#[tokio::test]
async fn socket_admission_requires_actor_invoke_on_the_resource() -> TestResult {
    let (gone, _left) = oneshot::channel();
    let client = client(Arc::new(Echo {
        gone: std::sync::Mutex::new(Some(gone)),
        ends: false,
    }));
    // The default credential reads streams but cannot invoke actors.
    client.register_credential(0, testing::credential())?;
    let refused = match client.actor_socket(Bytes::from_static(b"open"), 0).await {
        Err(error) => Some(error),
        Ok(mut socket) => socket.next().await.err(),
    };
    assert_eq!(refused, Some(StreamError::AccessDenied));
    Ok(())
}

#[tokio::test]
async fn a_socket_the_host_ends_after_its_final_frame_ends_cleanly_for_the_peer() -> TestResult {
    let (gone, left) = oneshot::channel();
    let client = client(Arc::new(Echo {
        gone: std::sync::Mutex::new(Some(gone)),
        ends: true,
    }));
    client.register_credential(
        0,
        testing::credential_with_grants(vec![acyclic_auth::contract::ResourceGrant {
            resource: acyclic_auth::contract::ResourcePattern {
                name: "actors:/fixture".into(),
                prefix: false,
            },
            actions: vec!["actors/invoke".into()],
        }]),
    )?;
    let mut socket = client.actor_socket(Bytes::from_static(b"open"), 0).await?;
    assert_eq!(socket.next().await?.as_deref(), Some(&b"hello"[..]));
    // The end follows the final frame as a clean end, not a lost link.
    assert_eq!(socket.next().await?, None);
    left.await?;
    Ok(())
}

#[tokio::test]
async fn host_shutdown_closes_all_channels_without_data_credit() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", Bytes::from_static(b"seed")).await?;
    let (sender, receiver) = mpsc::unbounded();
    let mut greeting = hello(1_000_000, 100);
    let Some(Body::Hello(hello)) = &mut greeting.body else {
        return Err("missing greeting".into());
    };
    hello.credentials.push(testing::credential_with_grants(vec![
        acyclic_auth::contract::ResourceGrant {
            resource: acyclic_auth::contract::ResourcePattern {
                name: "actors:/fixture".into(),
                prefix: false,
            },
            actions: vec!["actors/invoke".into()],
        },
    ]));
    sender.unbounded_send(Ok(greeting))?;
    let mut socket = subscribe(9, "events", 0, false);
    socket.credential = 1;
    socket.kind = Some(Kind::ActorSocket(Bytes::from_static(b"open")));
    sender.unbounded_send(Ok(interest(
        1,
        vec![
            subscribe(1, "events", 0, false),
            subscribe(3, "events", 1, false),
            socket,
        ],
    )))?;
    let (owner, stopped) = oneshot::channel::<()>();
    let (gone, left) = oneshot::channel();
    let mut output = serve_on_with_products(
        provider,
        testing::authority(),
        Products {
            sockets: Some(Arc::new(Echo {
                gone: std::sync::Mutex::new(Some(gone)),
                ends: false,
            })),
            ..Products::default()
        },
        "node".into(),
        Limits::default(),
        receiver.boxed(),
        Some(Box::pin(async {
            let _ = stopped.await;
        })),
    )?;
    assert!(matches!(
        next(&mut output).await?.body,
        Some(Body::Welcome(_))
    ));
    let mut opened = 0;
    while opened != 3 {
        if matches!(next(&mut output).await?.body, Some(Body::Opened(_))) {
            opened += 1;
        }
    }
    let denied = [5, 7].map(|id| {
        let mut subscription = subscribe(id, "events", 0, false);
        subscription.credential = 9;
        subscription
    });
    sender.unbounded_send(Ok(interest(2, denied.into())))?;
    loop {
        let frame = next(&mut output).await?;
        if let Some(Body::Closed(close)) = frame.body {
            assert_eq!(frame.channel_id, 5);
            assert_eq!(close.code, "forbidden");
            break;
        }
    }
    drop(owner);
    // One retained terminal response is emitted before any live channel's
    // response. Keep the response stream alive but stop polling it here.
    let first = next(&mut output).await?;
    assert_eq!(first.channel_id, 7);
    assert!(matches!(first.body, Some(Body::Closed(ref close)) if close.code == "forbidden"));
    assert!(matches!(
        futures::FutureExt::now_or_never(left),
        Some(Ok(()))
    ));
    let mut closed = BTreeSet::new();
    closed.insert((first.channel_id, first.generation));
    while let Some(frame) = output.next().await.transpose()? {
        let frame = frame.into_wire();
        match frame.body {
            Some(Body::Closed(close)) => {
                assert_eq!(
                    close.code,
                    if frame.channel_id == 7 {
                        "forbidden"
                    } else {
                        StreamError::Unavailable.code()
                    }
                );
                assert!(closed.insert((frame.channel_id, frame.generation)));
            }
            Some(Body::InterestApplied(_)) => {}
            other => panic!("unexpected frame after host shutdown: {other:?}"),
        }
    }
    assert_eq!(closed, BTreeSet::from([(1, 1), (3, 1), (7, 1), (9, 1)]));
    Ok(())
}
