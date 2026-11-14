//! Real session authorization and socket lifecycle, without a transport mock.
use super::*;
use crate::{
    domain::ActorLimits,
    local::AppliedCut,
    route::{self, NodeActor, NodeId, NodePresence, Paths},
};
use acyclic_stream::FollowRequest;

#[tokio::test]
async fn node_links_bind_owner_credentials_and_publish_only_lifecycle_changes() -> TestResult {
    let backend = LocalSockets::start_with(faithful, |_| {
        Ok(Arc::new(
            NodeActor::new(ActorLimits::default()).map_err(|_| Error::Invalid("node actor"))?,
        ))
    })
    .await?;
    let paths = Paths::new(StreamPath::new("account/routing")?);
    let node = NodeId::new(b"first-node")?;
    let other = NodeId::new(b"other-node")?;
    let package = NodeActor::new(ActorLimits::default())?;
    let foreign = ActorReference::new(backend.account(), b"other-artifact", b"link")?;
    let foreign_inbox = StreamPath::new("other-artifact/inbox")?;
    let mut foreign_creation =
        crate::local::PreparedPublish::foreign(IdempotencyKey::new(b"other-artifact".to_vec())?);
    foreign_creation.join(backend.store.create(
        None,
        &foreign,
        crate::local::Creation {
            subscriptions: std::collections::BTreeMap::from([(
                "node-link".into(),
                crate::local::Subscription {
                    path: foreign_inbox.clone(),
                    cursor: 0,
                    paused: false,
                },
            )]),
            checkpoint: None,
            escrow: None,
        },
    )?)?;
    foreign_creation.join(acyclic_stream::CommitEffect::hard(
        vec![acyclic_stream::CommitCondition::Absent {
            path: foreign_inbox.clone(),
        }],
        vec![acyclic_stream::CommitMutation::Append {
            path: foreign_inbox,
            records: vec![Bytes::from_static(b"owned by another package").into()],
        }],
    ))?;
    foreign_creation.join(backend.store.activation(&foreign)?)?;
    let outcome = backend
        .store
        .provider()
        .commit(foreign_creation.into_request()?)
        .await?;
    crate::local::PreparedPublish::settle(&outcome)?;
    for identity in [&node, &other] {
        let request = package.creation(
            &backend.store,
            &paths,
            backend.account(),
            identity,
            IdempotencyKey::new(identity.as_bytes().to_vec())?,
        )?;
        let outcome = backend.store.provider().commit(request).await?;
        crate::local::PreparedPublish::settle(&outcome)?;
    }
    let client = backend.client()?;
    let open = route::open_request(backend.account(), &node)?;
    client.register_credential(
        1,
        testing::credential_with_grants(vec![ResourceGrant {
            resource: ResourcePattern {
                name: open.resource(),
                prefix: false,
            },
            actions: vec!["actors/invoke".into()],
        }]),
    )?;
    let provider = backend.store.provider();
    let presence_path = paths.presence(&node)?;
    let initial = provider.tail(presence_path.clone()).await?;
    let mut changes = acyclic_stream::records_only(
        provider
            .follow(FollowRequest::new(presence_path.clone(), initial))
            .await?,
    );
    let mut link = route::open_link(&client, 1, backend.account(), &node).await?;
    let online = changes.try_next().await?.ok_or("missing online event")?;
    let online = NodePresence::decode(&online.value)?;
    assert!(online.online && online.node == node && online.generation == 1);
    assert_eq!(
        backend
            .store
            .load(&foreign)
            .await?
            .state()
            .ok_or("foreign state missing")?
            .epoch(),
        0,
        "node runtime claimed another artifact's pending input"
    );
    assert!(
        route::open_link(&client, 1, backend.account(), &other)
            .await
            .is_err(),
        "one node's namespace grant opened another node"
    );
    assert!(
        NodePresence::read(provider.as_ref(), &paths, &other)
            .await?
            .is_some_and(|row| !row.online && row.generation == 0)
    );

    // A non-lifecycle frame must not generate presence traffic. Wait on its
    // actual input publication and applied cut, rather than probing readiness.
    let inbox_path = paths.node_inbox(&node)?;
    let tail = provider.tail(inbox_path.clone()).await?;
    let mut inputs = acyclic_stream::records_only(
        provider
            .follow(FollowRequest::new(inbox_path.clone(), tail))
            .await?,
    );
    link.send(
        SocketFrame::Message(SocketPayload::Binary {
            binary: Bytes::from_static(b"no presence update"),
        })
        .encode(),
    )
    .await?;
    let input = inputs.try_next().await?.ok_or("missing socket input")?;
    backend
        .store
        .wait_applied(
            &open.actor,
            AppliedCut::through("node-link".into(), inbox_path, &input)?,
        )
        .await?;
    assert_eq!(provider.tail(presence_path.clone()).await?, initial + 1);

    // A reconnect can overlap the old transport's Close. The old connection
    // must not make the replacement offline, even after its input is consumed.
    let replacement = route::open_link(&client, 1, backend.account(), &node).await?;
    let online = changes.try_next().await?.ok_or("missing reconnect event")?;
    let online = NodePresence::decode(&online.value)?;
    assert!(online.online && online.node == node && online.generation == 2);
    let inbox_path = paths.node_inbox(&node)?;
    let mut closes = acyclic_stream::records_only(
        provider
            .follow(FollowRequest::new(
                inbox_path.clone(),
                provider.tail(inbox_path.clone()).await?,
            ))
            .await?,
    );
    link.send(
        SocketFrame::Close {
            code: 1000,
            reason: "finished".into(),
            initiator: crate::guest::SocketCloseInitiator::Peer,
        }
        .encode(),
    )
    .await?;
    let close = closes.try_next().await?.ok_or("missing old Close input")?;
    backend
        .store
        .wait_applied(
            &open.actor,
            AppliedCut::through("node-link".into(), inbox_path, &close)?,
        )
        .await?;
    assert_eq!(provider.tail(presence_path.clone()).await?, initial + 2);
    assert!(
        NodePresence::read(provider.as_ref(), &paths, &node)
            .await?
            .is_some_and(|row| row.online && row.generation == 2)
    );
    replacement
        .send(
            SocketFrame::Close {
                code: 1000,
                reason: "finished".into(),
                initiator: crate::guest::SocketCloseInitiator::Peer,
            }
            .encode(),
        )
        .await?;
    let offline = changes.try_next().await?.ok_or("missing offline event")?;
    let offline = NodePresence::decode(&offline.value)?;
    assert!(!offline.online && offline.node == node && offline.generation == 2);
    assert_eq!(provider.tail(presence_path).await?, initial + 3);
    assert!(matches!(
        link.next()
            .await?
            .map(|frame| SocketFrame::decode(&frame))
            .transpose()?,
        Some(SocketFrame::Close { .. })
    ));
    Ok(())
}
