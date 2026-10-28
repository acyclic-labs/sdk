use super::*;

struct RefusingResources;
impl ResourceProvider for RefusingResources {
    fn follow(&self, _: ResourceFollowRequest) -> Result<ResourceSubscription, StreamError> {
        Err(StreamError::AccessDenied)
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
    for (resources, error) in [
        (None, StreamError::Unsupported),
        (
            Some(Arc::new(RefusingResources) as Arc<dyn ResourceProvider>),
            StreamError::AccessDenied,
        ),
    ] {
        let provider = Arc::new(MemoryStream::default());
        append(&provider, "healthy", Bytes::from_static(b"record")).await?;
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
        let mut output = serve_on_with_resources(
            provider,
            testing::authority(),
            resources,
            "node".into(),
            Limits::default(),
            receiver.boxed(),
            Arc::new(crate::runtime::PlatformExecutor),
        )?;
        let (mut refused, mut applied, mut delivered) = (false, false, false);
        while !(refused && applied && delivered) {
            let message = next(&mut output).await?;
            match message.body {
                Some(Body::Closed(closed)) => {
                    assert_eq!(message.channel_id, 3);
                    assert_eq!(closed.code, error.code());
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
