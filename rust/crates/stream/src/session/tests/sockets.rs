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
    fn receive(&self, frame: Bytes) -> crate::BoxProviderFuture<'static, Result<(), StreamError>> {
        let sent = self.frames.unbounded_send(frame);
        Box::pin(async move { sent.map_err(|_| StreamError::Unavailable) })
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
