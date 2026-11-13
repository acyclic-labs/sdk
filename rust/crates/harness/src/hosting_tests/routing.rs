//! One real authenticated session serves routing, node sockets and commands.
use super::*;
use acyclic_actors::route::{
    self, AccountNode, Admission, AdmissionPolicy, Envelope, NodeId, NodePresence, Paths,
    Placement as RoutePlacement, Routed,
};
use acyclic_stream::{
    CommitCondition, CommitEffect, CommitMutation, CommitOutcome, CommitRequest, FollowRequest,
    IdempotencyKey, StreamPath, StreamProvider as _,
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
    let paths = Paths::new(StreamPath::new("account/routing")?);
    let source = NodeId::new(b"source")?;
    let bound = binding_routed(
        Arc::new(Never),
        Default::default(),
        Some(crate::agent::Routing {
            paths: paths.clone(),
            node: source.clone(),
        }),
    )?;
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
    let root_actor = server.bound().agents().actor(server.root())?;
    let placement = RoutePlacement {
        actor: root_actor.clone(),
        node: source.clone(),
    };
    assert_eq!(
        RoutePlacement::read(stream.as_ref(), &paths, &root_actor).await?,
        Some(placement)
    );
    assert_eq!(stream.tail(paths.placement(&root_actor)?).await?, 1);
    assert!(matches!(
        stream
            .commit(CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: IdempotencyKey::new(b"move-existing-root".to_vec())?,
                effects: vec![
                    RoutePlacement {
                        actor: root_actor.clone(),
                        node: NodeId::new(b"other-node")?
                    }
                    .effect(&paths)?
                ],
            })
            .await?,
        CommitOutcome::Conflict(_)
    ));
    let destination = NodeId::new(b"destination")?;
    let sender = ActorReference::new(b"account", b"remote", b"sender")?;
    let recipient = ActorReference::new(b"account", b"remote", b"recipient")?;
    let registry = Arc::new(crate::agent::SessionRegistry::new(
        sender.account().clone(),
        stream.clone(),
        paths.clone(),
        server.clone(),
    ));
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
    .with_products(registry, server.clone());
    node.enroll(
        &destination,
        IdempotencyKey::new(b"enroll-destination".to_vec())?,
    )
    .await?;
    for (actor, node) in [(&recipient, &destination)] {
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
    let placement_resource = route::placement_resource(sender.account(), &source);
    client.register_credential(4, credential(&placement_resource))?;
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
    let registration = route::Registration {
        placement: RoutePlacement {
            actor: message.sender.clone(),
            node: source.clone(),
        },
        intent: IdempotencyKey::new(b"source-root-creation".to_vec())?,
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
        assert!(
            route::admit(&client, 1, &message, 4096).await.is_err(),
            "unregistered sender was admitted"
        );
        assert!(
            route::register(&client, 1, &paths, &registration, 4096)
                .await
                .is_err(),
            "peer append grant registered a root"
        );
        let wrong_node = route::Registration {
            placement: RoutePlacement {
                actor: message.sender.clone(),
                node: destination.clone(),
            },
            intent: registration.intent.clone(),
        };
        assert!(
            route::register(&client, 4, &paths, &wrong_node, 4096)
                .await
                .is_err()
        );
        // The single-node case recovers the placement already created in the
        // original adoption commit, rather than attempting a second placement.
        let root_record = stream
            .record_at(paths.outbox(&source)?, 0)
            .await?
            .ok_or("root publication missing")?;
        let route::Publication::Placement(root_registration) =
            route::Publication::decode(&root_record.value)?
        else {
            return Err("root did not publish placement".into());
        };
        route::register(&client, 4, &paths, &root_registration, 4096).await?;
        let publication = crate::agent::SessionPublication {
            actor: root_actor.clone(),
            row: crate::agent::SessionRow::new(
                source.clone(),
                crate::agent::SessionProfile::project(
                    &crate::agent::Phase::Idle,
                    String::new(),
                    &[],
                    "",
                    0,
                ),
            ),
            intent: root_registration.intent.clone(),
        };
        let disabled_id = AgentId::from_bytes([2; 16]);
        let disabled = server
            .bound()
            .prepare_root(
                &IntentId::try_from("nonparticipating-root")?,
                RootRequest {
                    peer: false,
                    agent: disabled_id.clone(),
                    fresh: vec![WorkspaceEntry::initial_volume(
                        crate::agent::WorkspaceRole::Private,
                        PRIVATE_VOLUME.into(),
                        VolumeRef::new(
                            provider()?,
                            "private".into(),
                            VolumeClass::AgentPrivate,
                            VolumeOwner::Agent(crate::AgentId::from_bytes(disabled_id.to_bytes())),
                            &mut None,
                        )?,
                    )],
                    retained: Vec::new(),
                    brief: None,
                    policy: None,
                },
            )
            .await?;
        let mut disabled_row = publication.clone();
        disabled_row.actor = server
            .bound()
            .agents()
            .actor(&AgentPath::root(&disabled_id))?;
        let disabled_path = disabled_row.path()?;
        assert!(
            !disabled
                .request()
                .mutations()
                .any(|mutation| mutation.path() == &disabled_path)
        );
        let outbox = paths.outbox(&source)?;
        let metadata: Vec<_> = disabled
            .request()
            .mutations()
            .filter_map(|mutation| match mutation {
                CommitMutation::Append { path, records } if path == &outbox => Some(records),
                _ => None,
            })
            .flatten()
            .collect();
        assert_eq!(
            metadata.len(),
            1,
            "opted-out root published directory metadata"
        );
        assert!(matches!(
            route::Publication::decode(&metadata[0].value)?,
            route::Publication::Placement(_)
        ));
        assert!(
            publication.publish(&client, 1, 4096).await.is_err(),
            "peer append authority published directory metadata"
        );
        assert_eq!(stream.tail(publication.path()?).await?, 1);
        let mut foreign = publication.clone();
        foreign.actor = message.to.clone();
        assert!(
            foreign.publish(&client, 4, 4096).await.is_err(),
            "own-node credential wrote another node's root row"
        );
        assert!(matches!(
            stream.tail(foreign.path()?).await,
            Err(StreamError::NotFound)
        ));
        // The original root commit contains placement, row and ordered outbox
        // records. Source/account replay never publishes a second root row.
        let row_record = stream
            .head(publication.path()?)
            .await?
            .last
            .ok_or("root row missing")?;
        assert_eq!(row_record.commit_id, root_record.commit_id);
        let call_record = stream
            .record_at(paths.outbox(&source)?, 1)
            .await?
            .ok_or("root row call missing")?;
        assert_eq!(call_record.commit_id, root_record.commit_id);
        let row_call = route::Publication::decode(&call_record.value)?;
        assert!(matches!(&row_call, route::Publication::Call(_)));
        let route::Publication::Call(mut wrong_reply) = row_call.clone() else {
            return Err("root row call missing".into());
        };
        wrong_reply.reply = bytes::Bytes::from_static(b"wrong receipt");
        let interrupted = acyclic_stream::MemoryStream::default();
        assert!(matches!(
            interrupted
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: IdempotencyKey::new(b"wrong-metadata-receipt".to_vec())?,
                    effects: vec![wrong_reply.effect(&paths)?],
                })
                .await?,
            CommitOutcome::Committed(_)
        ));
        let refused = route::forward(
            &interrupted,
            &paths,
            &source,
            async |publication: route::Publication| {
                publication.publish(&client, 4, &paths, 4096).await
            },
            async |_| Err(route::Error::InvalidRecord),
        )
        .await;
        assert!(
            matches!(refused, Err(route::Error::Stream(error)) if error.category() == StreamError::InvalidResponse)
        );
        assert!(matches!(
            interrupted.tail(paths.outbox_cursor(&source)?).await,
            Err(StreamError::NotFound)
        ));
        row_call.publish(&client, 4, &paths, 4096).await?;
        publication.publish(&client, 4, 4096).await?;
        publication.publish(&client, 4, 4096).await?;
        assert_eq!(stream.tail(publication.path()?).await?, 1);
        let mut profile = publication
            .row
            .profile
            .clone()
            .ok_or("initial profile missing")?;
        profile.title = "Changed title".into();
        let changed = crate::agent::SessionPublication {
            actor: publication.actor.clone(),
            row: publication
                .row
                .changed(Some(profile))?
                .ok_or("profile did not change")?,
            intent: IdempotencyKey::new(b"changed-profile-publication".to_vec())?,
        };
        changed.publish(&client, 4, 4096).await?;
        changed.publish(&client, 4, 4096).await?;
        assert_eq!(stream.tail(changed.path()?).await?, 2);
        let mut altered = changed.clone();
        altered
            .row
            .profile
            .as_mut()
            .ok_or("changed profile missing")?
            .title = "Substituted".into();
        assert!(
            altered.publish(&client, 4, 4096).await.is_err(),
            "changed bytes reused the original row intent"
        );
        let removed = crate::agent::SessionPublication {
            actor: changed.actor.clone(),
            row: changed.row.changed(None)?.ok_or("row did not leave")?,
            intent: IdempotencyKey::new(b"remove-profile-publication".to_vec())?,
        };
        removed.publish(&client, 4, 4096).await?;
        publication.publish(&client, 4, 4096).await?;
        let head = stream.head(removed.path()?).await?;
        assert_eq!(head.tail, 3, "retry resurrected a removed row");
        assert_eq!(
            crate::agent::SessionRow::decode(&head.last.ok_or("removed row missing")?.value)?,
            removed.row
        );
        assert_eq!(
            stream
                .tail(paths.placement(&root_registration.placement.actor)?)
                .await?,
            1
        );
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
        let source_row = crate::agent::SessionPublication {
            actor: message.sender.clone(),
            row: crate::agent::SessionRow::new(
                source.clone(),
                crate::agent::SessionProfile::project(
                    &crate::agent::Phase::Idle,
                    "Source session".into(),
                    &[],
                    "Public task preview",
                    16,
                ),
            ),
            intent: registration.intent.clone(),
        };
        let source_row_path = source_row.path()?;
        let mut root_publication =
            acyclic_actors::local::PreparedPublish::foreign(registration.intent.clone());
        for effect in registration
            .effects(&paths)?
            .into_iter()
            .chain(source_row.effects(&paths)?)
        {
            root_publication.join(effect)?;
        }
        assert!(matches!(
            local.commit(root_publication.into_request()?).await?,
            CommitOutcome::Committed(_)
        ));
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
            async |publication: route::Publication| {
                publication.publish(&client, 4, &paths, 4096).await
            },
            async |envelope| {
                let row = stream
                    .head(source_row_path.clone())
                    .await?
                    .last
                    .ok_or(StreamError::InvalidResponse)?;
                assert_eq!(
                    crate::agent::SessionRow::decode(&row.value)
                        .map_err(|_| route::Error::InvalidRecord)?,
                    source_row.row
                );
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
        assert_eq!(local.tail(paths.outbox_cursor(&source)?).await?, 2);
        for sequence in 0..2 {
            assert!(
                route::receipt(
                    &local,
                    &paths,
                    &source,
                    sequence,
                    &message.request.idempotency_key
                )
                .await
                .is_err()
            );
        }
        assert_eq!(stream.tail(source_row.path()?).await?, 1);
        route::register(&client, 4, &paths, &registration, 4096).await?;
        assert_eq!(
            RoutePlacement::read(stream.as_ref(), &paths, &message.sender).await?,
            Some(registration.placement.clone())
        );
        let changed_intent = route::Registration {
            intent: IdempotencyKey::new(b"second-placement".to_vec())?,
            ..registration.clone()
        };
        assert!(
            route::register(&client, 4, &paths, &changed_intent, 4096)
                .await
                .is_err()
        );
        assert_eq!(stream.tail(paths.placement(&message.sender)?).await?, 1);
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
                    ..FollowRequest::new(paths.outbox_cursor(&source)?, 2)
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
                async |publication: route::Publication| {
                    publication.publish(&client, 4, &paths, 4096).await
                },
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
                async |_| Err(route::Error::InvalidRecord),
                async |record: route::RelayRecord| {
                    assert_eq!(
                        record
                            .receipt
                            .read(stream.as_ref(), &paths, &destination)
                            .await?,
                        record.envelope,
                    );
                    assert_eq!(record.receipt.sequence, 0);
                    let mut changed = *record.receipt.commit_id.as_bytes();
                    changed[0] ^= 1;
                    let unrelated = route::RelayReceipt {
                        commit_id: acyclic_stream::CommitId::from_bytes(changed),
                        ..record.receipt
                    };
                    assert!(
                        unrelated
                            .read(stream.as_ref(), &paths, &destination)
                            .await
                            .is_err()
                    );
                    // A fork preserves the original record's commit and sequence.
                    // Its copied bytes are not a receipt for the other node.
                    let other = NodeId::new(b"copied-relay")?;
                    let copied = paths.relay(&other)?;
                    assert!(matches!(
                        stream
                            .commit(CommitRequest {
                                deadline_unix_millis: None,
                                idempotency_key: IdempotencyKey::new(
                                    b"copy-relay-negative".to_vec()
                                )?,
                                effects: vec![CommitEffect::hard(
                                    vec![CommitCondition::Absent {
                                        path: copied.clone()
                                    }],
                                    vec![CommitMutation::Fork {
                                        source: paths.relay(&destination)?,
                                        destination: copied.clone(),
                                        at_tail: 1,
                                        exclude: Vec::new(),
                                        records: Vec::new(),
                                    }],
                                )],
                            })
                            .await?,
                        CommitOutcome::Committed(_)
                    ));
                    let copied = stream
                        .record_at(copied, 0)
                        .await?
                        .ok_or(route::Error::InvalidRecord)?;
                    assert_eq!(copied.commit_id, record.receipt.commit_id);
                    assert_eq!(copied.sequence, record.receipt.sequence);
                    assert!(
                        record
                            .receipt
                            .read(stream.as_ref(), &paths, &other)
                            .await
                            .is_err()
                    );
                    match destination_store.commit(record.envelope.request).await? {
                        CommitOutcome::Committed(_) => Ok(()),
                        CommitOutcome::Conflict(_) => Err(route::Error::InvalidRecord),
                    }
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
                    route::receipt(&local, &paths, &source, 2, &message.request.idempotency_key)
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
