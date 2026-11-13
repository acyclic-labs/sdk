//! One real authenticated session serves routing, node sockets and commands.
use super::*;
use acyclic_actors::route::{
    self, AccountNode, Admission, AdmissionPolicy, Envelope, NodeId, NodePresence, Paths,
    Placement as RoutePlacement, Routed,
};
use acyclic_stream::{
    CommitEffect, CommitMutation, CommitOutcome, CommitRequest, FollowRequest, IdempotencyKey,
    StreamPath, StreamProvider as _,
};
use futures::TryStreamExt as _;

struct PeerMessages;

impl AdmissionPolicy for PeerMessages {
    fn authorize(
        &self,
        authorization: &CallAuthorization,
        message: &Envelope,
    ) -> std::result::Result<(), StreamError> {
        let principal = authorization.principal()?;
        if principal.id != acyclic_auth::encode_component(&message.sender.encode())
            || !principal
                .roles
                .iter()
                .any(|role| role == crate::agent::SEND_PEER)
        {
            return Err(StreamError::AccessDenied);
        }
        let [effect] = message.request.effects.as_slice() else {
            return Err(StreamError::AccessDenied);
        };
        let [CommitMutation::Append { path, records }] = effect.mutations.as_slice() else {
            return Err(StreamError::AccessDenied);
        };
        if effect.soft
            || !effect.conditions.is_empty()
            || records.len() != 1
            || path.as_str() != "remote/recipient/inbox"
        {
            return Err(StreamError::AccessDenied);
        }
        Ok(())
    }

    fn prepare<'a>(
        &'a self,
        _: &'a CallAuthorization,
        _: &'a Envelope,
        destination: Option<&'a RoutePlacement>,
    ) -> BoxProviderFuture<'a, std::result::Result<Admission, route::Error>> {
        Box::pin(async move {
            if destination.is_none() {
                return Err(route::Error::InvalidRecord);
            }
            Ok(Admission::Allow(Vec::new()))
        })
    }
}

#[tokio::test]
async fn account_routes_node_links_and_commands_share_authenticated_dispatch() -> Result {
    let bound = binding()?;
    let stream = bound.agents().provider().clone();
    let identity = AgentId::from_bytes([1; 16]);
    let Serving::Served(server) = serve(
        bound,
        Arc::new(Owner::default()),
        &IntentId::try_from("root")?,
        RootRequest {
            peer: true,
            agent: identity.clone(),
            fresh: vec![WorkspaceEntry::initial_volume(
                crate::agent::WorkspaceRole::Private,
                PRIVATE_VOLUME.into(),
                VolumeRef::new(
                    provider()?,
                    "private".into(),
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(crate::AgentId::from_bytes(identity.to_bytes())),
                    &mut None,
                )?,
            )],
            retained: Vec::new(),
            brief: None,
            policy: None,
        },
    )
    .await?
    else {
        return Err("root publication uncertain".into());
    };
    let paths = Paths::new(StreamPath::new("account/routing")?);
    let source = NodeId::new(b"source")?;
    let destination = NodeId::new(b"destination")?;
    let sender = ActorReference::new(b"account", b"remote", b"sender")?;
    let recipient = ActorReference::new(b"account", b"remote", b"recipient")?;
    let node = AccountNode::open(
        acyclic_actors::addressing::AccountId::new(b"account")?,
        stream.clone(),
        paths.clone(),
        Arc::new(PeerMessages),
        ActorLimits::default(),
        Arc::new(SystemClock),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
    )
    .await?
    .with_products(server.clone(), server.clone());
    node.enroll(
        &destination,
        IdempotencyKey::new(b"enroll-destination".to_vec())?,
    )
    .await?;
    for (actor, node) in [(&sender, &source), (&recipient, &destination)] {
        assert!(matches!(
            stream
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: IdempotencyKey::new(actor.encode())?,
                    effects: vec![
                        RoutePlacement {
                            actor: actor.clone(),
                            node: node.clone()
                        }
                        .effect(&paths)?
                    ],
                })
                .await?,
            CommitOutcome::Committed(_)
        ));
    }
    let node = Arc::new(node);
    let client = Client::in_process_with_products(
        stream.clone(),
        acyclic_stream::session::testing::authority(),
        None,
        node.clone(),
        Some(node.clone()),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(0, credential(server.resource()))?;
    let resource = route::relay_resource(sender.account(), &source);
    let socket = route::open_request(b"account", &source)?.resource();
    for (slot, peer) in [(1, true), (2, false)] {
        use acyclic_auth::contract::*;
        client.register_credential(
            slot,
            acyclic_auth::testing::root(|block| {
                let key = &mut block.certificates.as_mut().expect("signed fixture").keys[0].key;
                key.principal.id = acyclic_auth::encode_component(&sender.encode());
                key.principal.roles = if peer {
                    vec![crate::agent::SEND_PEER.into()]
                } else {
                    Vec::new()
                };
                key.grants = GrantSet {
                    grants: [&resource, &socket]
                        .into_iter()
                        .map(|resource| Grant {
                            kind: GrantKind::Resource(ResourceGrant {
                                resource: ResourcePattern {
                                    name: resource.clone(),
                                    prefix: false,
                                },
                                actions: vec!["actors/invoke".into()],
                            }),
                        })
                        .collect(),
                };
            })
            .into(),
        )?;
    }
    let message = Envelope {
        source: source.clone(),
        sender,
        to: recipient,
        request: CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: IdempotencyKey::new(b"original-message".to_vec())?,
            effects: vec![CommitEffect::hard(
                Vec::new(),
                vec![CommitMutation::Append {
                    path: StreamPath::new("remote/recipient/inbox")?,
                    records: vec!["peer input".into()],
                }],
            )],
        },
    };
    let mut presence = acyclic_stream::records_only(
        stream
            .follow(FollowRequest {
                when_absent: acyclic_stream::WhenAbsent::Wait,
                ..FollowRequest::new(paths.presence(&source)?, 1)
            })
            .await?,
    );
    let exercise = async {
        node.enroll(&source, IdempotencyKey::new(b"enroll-source".to_vec())?)
            .await?;
        assert!(
            matches!(call(&client, "agents", &AgentCommand::Root {}).await??, AgentReply::Root { agent } if agent == *server.root())
        );
        assert!(
            matches!(route::admit(&client, 0, &message, 4096).await, Err(route::Error::Stream(error)) if error.category() == StreamError::AccessDenied)
        );
        assert!(
            matches!(route::admit(&client, 2, &message, 4096).await, Err(route::Error::Stream(error)) if error.category() == StreamError::AccessDenied)
        );
        assert!(
            matches!(client.actor_call(&invocation("agents", &AgentCommand::Root {})?, 1, 4096).await, Err(error) if error.category() == StreamError::AccessDenied)
        );
        assert!(
            matches!(route::open_link(&client, 0, b"account", &source).await, Err(route::Error::Stream(error)) if error.category() == StreamError::AccessDenied)
        );
        assert!(
            matches!(route::open_link(&client, 1, b"account", &destination).await, Err(route::Error::Stream(error)) if error.category() == StreamError::AccessDenied)
        );
        let link = route::open_link(&client, 1, b"account", &source).await?;
        assert!(NodePresence::decode(&presence.try_next().await?.ok_or("no Open")?.value)?.online);
        let expected = Routed::Relayed {
            key: message.request.idempotency_key.clone(),
            relay_seq: 0,
        };
        assert_eq!(route::admit(&client, 1, &message, 4096).await?, expected);
        assert_eq!(route::admit(&client, 1, &message, 4096).await?, expected);
        assert_eq!(stream.tail(paths.relay(&destination)?).await?, 1);
        assert!(
            matches!(
                stream
                    .tail(StreamPath::new("remote/recipient/inbox")?)
                    .await,
                Err(StreamError::NotFound)
            ),
            "account router hosted a remote root"
        );
        let local = acyclic_stream::MemoryStream::default();
        assert!(matches!(
            local
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: IdempotencyKey::new(b"source-publication".to_vec())?,
                    effects: vec![message.outbox_effect(&paths)?],
                })
                .await?,
            CommitOutcome::Committed(_)
        ));
        // The account has admitted this intent, but the source crashes before
        // acknowledging it. Closing its link must cancel the pending callback.
        let control = link.sender();
        let (admitted, observed) = tokio::sync::oneshot::channel();
        let mut admitted = Some(admitted);
        let forwarding = route::run_link(
            link,
            &local,
            stream.as_ref(),
            &paths,
            &source,
            async |envelope| {
                assert_eq!(route::admit(&client, 1, &envelope, 4096).await?, expected);
                admitted
                    .take()
                    .ok_or(StreamError::InvalidResponse)?
                    .send(())
                    .map_err(|_| StreamError::Unavailable)?;
                std::future::pending::<std::result::Result<Routed, route::Error>>().await
            },
            async |_| Err(route::Error::InvalidRecord),
        );
        let disconnect = async {
            observed.await?;
            control
                .send(
                    acyclic_actors::sockets::SocketFrame::Close {
                        code: 1000,
                        reason: "disconnect before acknowledgment".into(),
                        initiator: acyclic_actors::guest::SocketCloseInitiator::Peer,
                    }
                    .encode(),
                )
                .await?;
            Ok(()) as Result
        };
        let (stopped, disconnected) = futures::join!(forwarding, disconnect);
        disconnected?;
        assert!(
            matches!(stopped, Err(route::Error::Stream(error)) if error.category() == StreamError::Unavailable)
        );
        assert!(
            !NodePresence::decode(&presence.try_next().await?.ok_or("no Close")?.value)?.online
        );
        assert!(matches!(
            local.tail(paths.outbox_cursor(&source)?).await,
            Err(StreamError::NotFound)
        ));
        let link = route::open_link(&client, 1, b"account", &source).await?;
        assert!(
            NodePresence::decode(&presence.try_next().await?.ok_or("no reconnect")?.value)?.online
        );
        let destination_socket = route::open_request(b"account", &destination)?.resource();
        client.register_credential(3, credential(&destination_socket))?;
        let receiving = route::open_link(&client, 3, b"account", &destination).await?;
        let destination_store = acyclic_stream::MemoryStream::default();
        let mut acknowledgments = acyclic_stream::records_only(
            local
                .follow(FollowRequest {
                    when_absent: acyclic_stream::WhenAbsent::Wait,
                    ..FollowRequest::new(paths.outbox_cursor(&source)?, 0)
                })
                .await?,
        );
        let mut drained = acyclic_stream::records_only(
            stream
                .follow(FollowRequest {
                    when_absent: acyclic_stream::WhenAbsent::Wait,
                    ..FollowRequest::new(paths.relay_cursor(&destination)?, 0)
                })
                .await?,
        );
        {
            let forwarding = route::run_link(
                link,
                &local,
                stream.as_ref(),
                &paths,
                &source,
                async |envelope| route::admit(&client, 1, &envelope, 4096).await,
                async |_| Err(route::Error::InvalidRecord),
            );
            let receiving = route::run_link(
                receiving,
                &destination_store,
                stream.as_ref(),
                &paths,
                &destination,
                async |_| Err(route::Error::InvalidRecord),
                async |envelope| match destination_store.commit(envelope.request).await? {
                    CommitOutcome::Committed(_) => Ok(()),
                    CommitOutcome::Conflict(_) => Err(route::Error::InvalidRecord),
                },
            );
            let pumps = futures::future::try_join(forwarding, receiving);
            let completed = async {
                acknowledgments
                    .try_next()
                    .await?
                    .ok_or("no source acknowledgment")?;
                drained.try_next().await?.ok_or("no drain acknowledgment")?;
                assert_eq!(
                    route::receipt(&local, &paths, &source, 0, &message.request.idempotency_key)
                        .await?,
                    Some(expected)
                );
                assert_eq!(stream.tail(paths.relay(&destination)?).await?, 1);
                assert_eq!(
                    destination_store
                        .tail(StreamPath::new("remote/recipient/inbox")?)
                        .await?,
                    1
                );
                Ok(()) as Result
            };
            futures::pin_mut!(pumps, completed);
            match futures::future::select(pumps, completed).await {
                futures::future::Either::Left((result, _)) => {
                    return Err(format!("link stopped before delivery: {result:?}").into());
                }
                futures::future::Either::Right((result, _)) => result?,
            }
        }
        assert!(
            !NodePresence::decode(&presence.try_next().await?.ok_or("no owner shutdown")?.value)?
                .online
        );
        assert!(matches!(
            call(&client, "agents", &AgentCommand::Root {}).await??,
            AgentReply::Root { .. }
        ));
        assert_eq!(
            server
                .bound()
                .agents()
                .state(server.root())
                .await?
                .conversation_tail,
            0,
            "node dispatcher executed a session root"
        );
        Ok(()) as Result
    };
    let running = node.run(|actor, error| panic!("{actor:?}: {error}"));
    futures::pin_mut!(exercise, running);
    match futures::future::select(running, exercise).await {
        futures::future::Either::Right((result, _)) => result?,
        futures::future::Either::Left((result, _)) => {
            return Err(format!("node runtime ended: {result:?}").into());
        }
    }
    Ok(())
}
