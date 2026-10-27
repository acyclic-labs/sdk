use futures::TryFutureExt as _;
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Calls(Arc<AtomicUsize>);
impl CallProvider for Calls {
    fn resource(&self, _: &[u8]) -> Result<String, StreamError> {
        Ok("actors:/fixture".into())
    }
    fn call(
        &self,
        _: Bytes,
        authorization: CallAuthorization,
    ) -> crate::BoxProviderFuture<'static, Result<Bytes, StreamError>> {
        let calls = self.0.clone();
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
            Ok(vec![9; crate::MAX_RECORD_BYTES + 7].into())
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
        Arc::new(Calls(calls.clone())),
        Arc::new(crate::runtime::PlatformExecutor),
    );
    client.register_credential(0, testing::credential())?;
    let mut records = client
        .follow(crate::FollowRequest { path: StreamPath::new("healthy")?, from: 0, preceding: None, filter: Default::default(), progress: false }).map_ok(crate::records_only)
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
    let client = Client::in_process_with_products(
        Arc::new(MemoryStream::default()),
        testing::authority(),
        None,
        Arc::new(Calls(calls.clone())),
        Arc::new(crate::runtime::PlatformExecutor),
    );
    client.register_credential(0, credential.into())?;
    let response = client
        .actor_call(
            &Bytes::from_static(b"invocation"),
            0,
            crate::MAX_RECORD_BYTES + 7,
        )
        .await?;
    assert_eq!(response.as_ref(), vec![9; crate::MAX_RECORD_BYTES + 7]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}
