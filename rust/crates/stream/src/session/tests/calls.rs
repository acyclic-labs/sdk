use super::*;
use futures::TryFutureExt as _;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Calls {
    calls: Arc<AtomicUsize>,
    body: Bytes,
}

struct InputCalls {
    admitted: tokio::sync::mpsc::UnboundedSender<(CallInput, oneshot::Sender<Bytes>)>,
}

// Hold the actual request stream at EOF while the original server and client
// continue running. No alternate dispatch, credit or terminal is supplied.
struct HeldInputTerminal {
    calls: Arc<dyn CallProvider>,
    lifetime: tokio::sync::watch::Receiver<()>,
    held: tokio::sync::mpsc::UnboundedSender<oneshot::Sender<()>>,
}
impl transport::Transport for HeldInputTerminal {
    fn session(
        &self,
        source: transport::Requests,
    ) -> crate::BoxProviderFuture<'_, Result<transport::Responses, StreamError>> {
        let held = self.held.clone();
        let calls = self.calls.clone();
        let mut lifetime = self.lifetime.clone();
        Box::pin(async move {
            let source = stream::unfold((source, held), |(mut source, held)| async move {
                let frame = source.next().await?;
                if matches!(frame.body, Some(Body::InputEnd(_))) {
                    let (release, released) = oneshot::channel();
                    held.send(release).ok()?;
                    released.await.ok()?;
                }
                Some((Ok(frame), (source, held)))
            });
            serve_on_with_products(
                Arc::new(MemoryStream::default()),
                testing::authority(),
                Products {
                    calls: Some(calls),
                    ..Products::default()
                },
                "local".into(),
                Limits::default(),
                Box::pin(source),
                Some(Box::pin(async move {
                    let _ = lifetime.changed().await;
                })),
            )
        })
    }
}
impl CallProvider for InputCalls {
    fn resource(&self, _: &[u8]) -> Result<String, StreamError> {
        Ok("actors:/fixture".into())
    }
    fn call(
        &self,
        _: Bytes,
        _: CallAuthorization,
    ) -> crate::BoxProviderFuture<'static, Result<Bytes, StreamError>> {
        Box::pin(future::ready(Err(StreamError::Unsupported)))
    }
    fn call_input(
        &self,
        _: Bytes,
        authorization: CallAuthorization,
        input: CallInput,
    ) -> crate::BoxProviderFuture<'static, Result<Bytes, StreamError>> {
        let admitted = self.admitted.clone();
        Box::pin(async move {
            let attributes = acyclic_auth::contract::RecordAttributes::try_new(vec![])
                .map_err(|_| StreamError::InvalidArgument)?;
            authorization.authorize(
                "actors:/fixture",
                &[],
                &attributes,
                acyclic_auth::authority::PolicyDecision::Allow,
            )?;
            let (answer, reply) = oneshot::channel();
            admitted
                .send((input, answer))
                .map_err(|_| StreamError::Unavailable)?;
            reply.await.map_err(|_| StreamError::Unavailable)
        })
    }
}

#[tokio::test]
async fn streamed_input_uses_original_credit_and_terminal_before_eof() -> TestResult {
    use acyclic_auth::contract::*;
    let credential = acyclic_auth::testing::root(|block| {
        block
            .certificates
            .as_mut()
            .unwrap()
            .keys
            .first_mut()
            .unwrap()
            .key
            .grants = GrantSet {
            grants: vec![Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: "actors:/fixture".into(),
                        prefix: false,
                    },
                    actions: vec!["actors/invoke".into()],
                }),
            }],
        };
    });
    let (admitted, mut dispatches) = tokio::sync::mpsc::unbounded_channel();
    let (host, lifetime) = tokio::sync::watch::channel(());
    let (held, mut terminals) = tokio::sync::mpsc::unbounded_channel();
    let endpoint: transport::Endpoint = Arc::new(HeldInputTerminal {
        calls: Arc::new(InputCalls { admitted }),
        lifetime,
        held,
    });
    let client = Client::with_transport(
        endpoint,
        Arc::new(|_| Box::pin(future::ready(Err(StreamError::Unavailable)))),
        Arc::new(crate::runtime::PlatformExecutor),
    );
    client.register_credential(0, credential.into())?;
    client.register_credential(1, testing::credential())?;
    let invocation = Bytes::from_static(b"canonical product header");
    let (mut upload, replies) = client.actor_call_input(&invocation, 0, 128).await?;
    futures::pin_mut!(replies);
    // Dispatch happens with NO input or EOF. The old whole-request collector
    // cannot produce this original handler event.
    let (mut input, answer) = dispatches.recv().await.ok_or("handler was not admitted")?;
    let original = Bytes::from(vec![7; crate::MAX_RECORD_BYTES]);
    let second_original = Bytes::from(vec![9; 1024]);
    upload.send(original.clone()).await?;
    upload.send(second_original.clone()).await?;
    let first = input.next_chunk().await?.ok_or("missing first input")?;
    let second = input.next_chunk().await?.ok_or("missing second input")?;
    assert_eq!(first.as_ref().as_ptr(), original.as_ptr());
    assert_eq!(second.as_ref().as_ptr(), second_original.as_ptr());
    let copied = Bytes::copy_from_slice(first.as_ref());
    assert_ne!(
        copied.as_ptr(),
        original.as_ptr(),
        "full-copy negative control"
    );
    let third_original = Bytes::from_static(b"third");
    let mut blocked = Box::pin(upload.send(third_original.clone()));
    assert!(
        futures::poll!(blocked.as_mut()).is_pending(),
        "both original frame credits are held"
    );
    drop(first);
    blocked.await?;
    let third = input
        .next_chunk()
        .await?
        .ok_or("release did not fund the next input")?;
    assert_eq!(third.as_ref().as_ptr(), third_original.as_ptr());
    // Reply credit is independent of the exhausted input window. An original
    // result is available while EOF is still withheld and two inputs are held.
    answer
        .send(Bytes::from_static(b"before EOF"))
        .map_err(|_| "handler result was lost")?;
    assert_eq!(
        replies.next().await.ok_or("no early reply")??.1.as_ref(),
        b"before EOF"
    );
    assert!(
        replies.next().await.is_none(),
        "complete reply must retire only the reply half"
    );
    // The old cursor-only lifetime cancelled the original input here. Keep
    // sending after the complete reply and its cursor have both retired.
    let fourth_original = Bytes::from_static(b"after reply EOF");
    let mut blocked = Box::pin(upload.send(fourth_original.clone()));
    assert!(futures::poll!(blocked.as_mut()).is_pending());
    drop(second);
    blocked.await?;
    let fourth = input
        .next_chunk()
        .await?
        .ok_or("complete reply cancelled the input half")?;
    assert_eq!(fourth.as_ref().as_ptr(), fourth_original.as_ptr());
    let trailers = vec![wire::SessionHeader {
        name: Bytes::from_static(b"digest"),
        value: Bytes::from_static(b"original terminal"),
    }];
    let mut finish = Box::pin(upload.finish(trailers.clone()));
    assert!(futures::poll!(finish.as_mut()).is_pending());
    let release = terminals
        .recv()
        .await
        .ok_or("transport did not reach EOF")?;
    assert!(
        futures::poll!(finish.as_mut()).is_pending(),
        "queue acceptance must not retire the final route before transport EOF"
    );
    release
        .send(())
        .map_err(|_| "EOF transport was discarded")?;
    finish.await?;
    assert!(
        input.next_chunk().await?.is_none(),
        "EOF must not need a returned data credit"
    );
    assert_eq!(input.trailers(), Some(trailers.as_slice()));
    assert_eq!(third.as_ref(), third_original.as_ref());
    assert_eq!(fourth.as_ref(), fourth_original.as_ref());

    // Incomplete reply retirement is cancellation, even when the original
    // upload producer and two escaped data owners remain alive.
    let (mut cancelled, cancelled_reply) = client.actor_call_input(&invocation, 0, 128).await?;
    let (mut input, _answer) = dispatches.recv().await.ok_or("no cancellable handler")?;
    cancelled.send(original.clone()).await?;
    cancelled.send(second_original.clone()).await?;
    let cancelled_first = input
        .next_chunk()
        .await?
        .ok_or("no first cancelled input")?;
    let cancelled_second = input
        .next_chunk()
        .await?
        .ok_or("no second cancelled input")?;
    let mut pending = Box::pin(cancelled.send(Bytes::from_static(b"blocked")));
    assert!(futures::poll!(pending.as_mut()).is_pending());
    drop(cancelled_reply);
    assert_eq!(pending.await, Err(StreamError::Unavailable));
    assert!(matches!(
        input.next_chunk().await,
        Err(StreamError::Unavailable)
    ));
    assert_eq!(cancelled_first.as_ref().as_ptr(), original.as_ptr());
    assert_eq!(cancelled_second.as_ref().as_ptr(), second_original.as_ptr());
    drop(cancelled);

    // Authorization is the original call admission, before body or handler
    // disclosure. No separate socket authority or buffered fallback is used.
    let (mut denied, refused) = client.actor_call_input(&invocation, 1, 128).await?;
    assert_eq!(
        denied.send(Bytes::from_static(b"secret input")).await,
        Err(StreamError::AccessDenied)
    );
    futures::pin_mut!(refused);
    assert_eq!(
        refused.next().await.map(|reply| reply.map(|_| ())),
        Some(Err(StreamError::AccessDenied))
    );
    assert!(dispatches.try_recv().is_err());
    drop(denied);

    let (mut abandoned, abandoned_reply) = client.actor_call_input(&invocation, 0, 128).await?;
    let (mut input, _abandoned_answer) = dispatches.recv().await.ok_or("no consumer to retire")?;
    abandoned.send(original.clone()).await?;
    abandoned.send(second_original.clone()).await?;
    let escaped = input.next_chunk().await?.ok_or("no escaped input")?;
    let escaped_second = input.next_chunk().await?.ok_or("no second escaped input")?;
    let mut pending = Box::pin(abandoned.send(Bytes::from_static(b"blocked")));
    assert!(futures::poll!(pending.as_mut()).is_pending());
    drop(input);
    assert_eq!(
        pending.await,
        Err(StreamError::Unavailable),
        "consumer loss must wake without escaped credit returning"
    );
    futures::pin_mut!(abandoned_reply);
    assert!(matches!(
        abandoned_reply.next().await,
        Some(Err(StreamError::Unavailable))
    ));
    assert_eq!(escaped.as_ref().as_ptr(), original.as_ptr());
    assert_eq!(escaped_second.as_ref().as_ptr(), second_original.as_ptr());

    let (mut stopped, lost) = client.actor_call_input(&invocation, 0, 128).await?;
    let (mut input, _answer) = dispatches
        .recv()
        .await
        .ok_or("second handler was not admitted")?;
    stopped.send(original.clone()).await?;
    stopped.send(second_original.clone()).await?;
    let held = input.next_chunk().await?.ok_or("no held input")?;
    let held_second = input.next_chunk().await?.ok_or("no second held input")?;
    let mut finish = Box::pin(stopped.finish(Vec::new()));
    assert!(futures::poll!(finish.as_mut()).is_pending());
    let _withheld = terminals
        .recv()
        .await
        .ok_or("no blocked terminal before host loss")?;
    drop(host);
    assert!(
        matches!(input.next_chunk().await, Err(StreamError::Unavailable)),
        "producer loss cannot become EOF"
    );
    assert_eq!(finish.await, Err(StreamError::Unavailable));
    futures::pin_mut!(lost);
    assert!(matches!(
        lost.next().await,
        Some(Err(StreamError::Unavailable))
    ));
    assert_eq!(held.as_ref().as_ptr(), original.as_ptr());
    assert_eq!(held_second.as_ref().as_ptr(), second_original.as_ptr());

    // Exact generation-local sequence and input/reply grammar are disjoint.
    let data = wire::SessionData {
        after: Some(cursor(1)),
        accepted: 0,
        durable: 0,
        item: Some(Item::ActorCall(Bytes::from_static(b"input"))),
    };
    assert_eq!(wire_codec::session::input_sequence(0, &data)?, 1);
    assert_eq!(
        wire_codec::session::input_sequence(1, &data),
        Err(StreamError::InvalidArgument)
    );
    assert!(wire_codec::session::actor_call_sequence(0, &[], &data).is_err());
    Ok(())
}
impl CallProvider for Calls {
    fn resource(&self, _: &[u8]) -> Result<String, StreamError> {
        Ok("actors:/fixture".into())
    }
    fn call(
        &self,
        _: Bytes,
        authorization: CallAuthorization,
    ) -> crate::BoxProviderFuture<'static, Result<Bytes, StreamError>> {
        let calls = self.calls.clone();
        let body = self.body.clone();
        Box::pin(async move {
            let attributes = acyclic_auth::contract::RecordAttributes::try_new(vec![])
                .map_err(|_| StreamError::InvalidArgument)?;
            authorization.authorize(
                "actors:/fixture",
                &[],
                &attributes,
                acyclic_auth::authority::PolicyDecision::Allow,
            )?;
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(body)
        })
    }
}

#[tokio::test]
async fn follow_authority_cannot_dispatch_calls_or_stop_healthy_follows() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "healthy", Bytes::from_static(b"record")).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let client = Client::in_process_with_products(
        provider,
        testing::authority(),
        None,
        Arc::new(Calls {
            calls: calls.clone(),
            body: Bytes::from_static(b"unused"),
        }),
        Arc::new(crate::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(0, testing::credential())?;
    let mut records = client
        .follow(crate::FollowRequest::new(StreamPath::new("healthy")?, 0))
        .map_ok(crate::records_only)
        .await?;
    assert_eq!(
        client
            .actor_call(
                &Bytes::from_static(b"invocation"),
                0,
                crate::MAX_RECORD_BYTES + 7
            )
            .await,
        Err(StreamError::AccessDenied)
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "refusal must precede dispatch"
    );
    assert_eq!(
        records
            .next()
            .await
            .ok_or("healthy follow ended")??
            .value
            .as_ref(),
        b"record"
    );
    Ok(())
}

#[tokio::test]
async fn call_reply_uses_shared_credit_and_preserves_all_fragments() -> TestResult {
    use acyclic_auth::contract::*;
    let credential = acyclic_auth::testing::root(|block| {
        let key = &mut block
            .certificates
            .as_mut()
            .unwrap()
            .keys
            .first_mut()
            .unwrap()
            .key;
        key.grants = GrantSet {
            grants: vec![Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: "actors:/fixture".into(),
                        prefix: false,
                    },
                    actions: vec!["actors/invoke".into()],
                }),
            }],
        };
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let body = Bytes::from(vec![9; 3 * crate::MAX_RECORD_BYTES + 7]);
    let (host, lifetime) = tokio::sync::watch::channel(());
    let client = Client::in_process_with_products(
        Arc::new(MemoryStream::default()),
        testing::authority(),
        None,
        Arc::new(Calls {
            calls: calls.clone(),
            body: body.clone(),
        }),
        Arc::new(crate::runtime::PlatformExecutor),
        Some(lifetime),
    );
    client.register_credential(0, credential.into())?;
    let response = client
        .actor_call(&Bytes::from_static(b"invocation"), 0, body.len())
        .await?;
    assert_eq!(response.as_ref(), body.as_ref());
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // The convenience collector necessarily owns a different full allocation.
    // The streaming projection must instead expose each original source range.
    assert_ne!(response.as_ptr(), body.as_ptr());
    let chunks = client
        .actor_call_chunks(&Bytes::from_static(b"invocation"), 0, body.len())
        .await?;
    futures::pin_mut!(chunks);
    let mut received = 0;
    while let Some(chunk) = chunks.next().await {
        let (total, chunk) = chunk?;
        assert_eq!(total, body.len());
        let bytes = chunk.as_ref();
        assert!(!bytes.is_empty());
        assert_eq!(bytes.as_ptr(), body[received..].as_ptr());
        received += bytes.len();
        assert_eq!(bytes, &body[received - bytes.len()..received]);
    }
    assert_eq!(received, body.len());
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    let refused = client
        .actor_call_chunks(&Bytes::from_static(b"invocation"), 0, body.len() - 1)
        .await?;
    futures::pin_mut!(refused);
    assert!(matches!(
        refused.next().await,
        Some(Err(StreamError::InvalidResponse))
    ));

    let stopped = client
        .actor_call_chunks(&Bytes::from_static(b"invocation"), 0, body.len())
        .await?;
    futures::pin_mut!(stopped);
    let first = stopped
        .next()
        .await
        .ok_or("reply ended before first fragment")??;
    let second = stopped
        .next()
        .await
        .ok_or("reply ended before second fragment")??;
    // Both original channel credits remain held. Losing the real host still
    // closes the incomplete reply without waiting for either fragment to drop.
    drop(host);
    assert!(matches!(
        stopped.next().await,
        Some(Err(StreamError::Unavailable))
    ));
    assert_eq!(first.1.as_ref(), &body[..crate::MAX_RECORD_BYTES]);
    assert_eq!(
        second.1.as_ref(),
        &body[crate::MAX_RECORD_BYTES..2 * crate::MAX_RECORD_BYTES]
    );
    Ok(())
}
