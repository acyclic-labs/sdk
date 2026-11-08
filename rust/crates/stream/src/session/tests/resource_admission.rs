use super::*;
use futures::FutureExt as _;

struct RefusingResources {
    error: StreamError,
    queued: Option<Arc<MemoryStream>>,
}
impl ResourceProvider for RefusingResources {
    fn follow(&self, _: ResourceFollowRequest) -> Result<ResourceSubscription, StreamError> {
        let Some(provider) = self.queued.clone() else {
            return Err(self.error.clone());
        };
        let error = self.error.clone();
        let (send, receive) = futures::channel::oneshot::channel();
        Ok(ResourceSubscription {
            revalidate: Arc::new(|| Ok(())),
            terminal: Box::pin(async move { receive.await.unwrap_or(StreamError::Unavailable) }),
            open: Box::pin(async move {
                let mut request = crate::FollowRequest::new(StreamPath::new("healthy")?, 0);
                request.until = Some(1);
                Ok(provider.follow(request).await?.map_cursor(|cursor| {
                    let mut send = Some(send);
                    cursor
                        .map(move |update| {
                            // The original record enters the queue before the
                            // independent owning-policy terminal is observed.
                            if matches!(&update, Ok(crate::FollowUpdate::Record(_)))
                                && let Some(send) = send.take()
                            {
                                let _ = send.send(error.clone());
                            }
                            update
                        })
                        .boxed()
                }))
            }),
        })
    }
}

#[tokio::test]
async fn resource_refusals_preserve_other_channels_in_the_same_interest() -> TestResult {
    use acyclic_auth::contract::{Grant, GrantKind, GrantSet, ResourceGrant, ResourcePattern};
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
            grants: [("streams:/", "streams/follow"), ("fs:/", "fs/list")]
                .into_iter()
                .map(|(name, action)| Grant {
                    kind: GrantKind::Resource(ResourceGrant {
                        resource: ResourcePattern {
                            name: name.into(),
                            prefix: true,
                        },
                        actions: vec![action.into()],
                    }),
                })
                .collect(),
        };
    });
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "healthy", Bytes::from_static(b"record")).await?;
    let cases = vec![
        (None, StreamError::Unsupported),
        (
            Some(Arc::new(RefusingResources {
                error: StreamError::AccessDenied,
                queued: None,
            }) as Arc<dyn ResourceProvider>),
            StreamError::AccessDenied,
        ),
    ];
    #[cfg(feature = "grpc")]
    let cases = {
        let mut cases = cases;
        for category in [
            StreamError::AccessDenied,
            StreamError::NotFound,
            StreamError::PrefixNotRetained,
        ] {
            let canonical = crate::wire_codec::error_status(&category);
            // Opaque detail bytes are the original Rust Objects ErrorDetail
            // writer's corpus output, never a hand-encoded remote status.
            let status = tonic::Status::with_details(
                canonical.code(),
                canonical.message(),
                Bytes::from_static(include_bytes!(
                    "../../../fuzz/corpus/grpc_status/objects-error-detail"
                )),
            );
            let error = crate::grpc::status(&status);
            assert!(matches!(error, StreamError::Refused(_)));
            cases.push((
                Some(Arc::new(RefusingResources {
                    error: error.clone(),
                    queued: Some(provider.clone()),
                }) as Arc<dyn ResourceProvider>),
                error,
            ));
        }
        cases
    };
    for (resources, error) in cases {
        let (sender, receiver) = mpsc::unbounded();
        let mut greeting = hello(1_000_000, 100);
        let Some(Body::Hello(body)) = greeting.body.as_mut() else {
            unreachable!()
        };
        body.credentials = vec![credential.clone().into()];
        sender.unbounded_send(Ok(greeting))?;
        let mut resource = subscribe(3, "unused", 0, false);
        let Some(Kind::Follow(follow)) = resource.kind.as_mut() else {
            unreachable!()
        };
        follow.resource = "fs:/00000000-0000-0000-0000-000000000001".into();
        sender.unbounded_send(Ok(interest(
            1,
            vec![resource, subscribe(1, "healthy", 0, false)],
        )))?;
        sender.unbounded_send(Ok(window(1)))?;
        let mut output = serve_on_with_products(
            provider.clone(),
            testing::authority(),
            Products {
                resources,
                ..Products::default()
            },
            "node".into(),
            Limits::default(),
            receiver.boxed(),
            None,
        )?;
        let (mut refused, mut applied, mut delivered) = (false, false, false);
        while !(refused && applied && delivered) {
            // Every producer is ready at this point. In particular a refused
            // resource must close with its original error at zero data credit.
            let (message, original) = output
                .next()
                .now_or_never()
                .ok_or("refusal waited for data credit")?
                .ok_or("session ended")??
                .into_parts();
            match message.body {
                Some(Body::Closed(closed)) => {
                    assert_eq!(message.channel_id, 3);
                    assert_eq!(closed.code, error.code());
                    assert_eq!(original, Some(error.clone()));
                    refused = true;
                }
                Some(Body::InterestApplied(_)) => applied = true,
                Some(Body::Data(data)) => {
                    assert_eq!(message.channel_id, 1);
                    assert!(
                        matches!(data.item, Some(Item::Record(record)) if record.value.as_ref() == b"record")
                    );
                    delivered = true;
                }
                Some(Body::Welcome(_) | Body::Opened(_)) => (),
                _ => return Err("unexpected session frame".into()),
            }
        }
    }
    Ok(())
}
