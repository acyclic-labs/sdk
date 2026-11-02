use super::*;
use futures::TryFutureExt as _;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Calls {
    calls: Arc<AtomicUsize>,
    body: Bytes,
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
