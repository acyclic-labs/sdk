//! Real signed session admission and account publication, without a mock router.
use super::*;
use crate::{addressing::ActorReference, route::NodeId};
use acyclic_auth::contract::{
    Grant, GrantKind, GrantSet, PrincipalKind, ResourceGrant, ResourcePattern,
};
use acyclic_stream::{
    CommitMutation, CommitOutcome, CommitRequest, MemoryStream, StreamPath, StreamProviderExt as _,
};
use sha2::{Digest as _, Sha256};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

struct PeerGrant {
    paths: Paths,
    creation: tokio::sync::Mutex<Option<CreationCut>>,
}

struct CreationCut {
    observed: tokio::sync::oneshot::Sender<()>,
    created: tokio::sync::oneshot::Receiver<()>,
}

impl AdmissionPolicy for PeerGrant {
    fn authorize(
        &self,
        authorization: &CallAuthorization,
        message: &Envelope,
    ) -> std::result::Result<(), StreamError> {
        let principal = authorization.principal()?;
        if principal.kind != PrincipalKind::Agent
            || principal.id != acyclic_auth::encode_component(&message.sender.encode())
            || !principal.roles.iter().any(|role| role == "agent.send-peer")
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
            || path.as_str() != format!("{}/inbox", message.to.id())
        {
            return Err(StreamError::AccessDenied);
        }
        Ok(())
    }

    fn prepare<'a>(
        &'a self,
        _authorization: &'a CallAuthorization,
        message: &'a Envelope,
        destination: Option<&'a Placement>,
    ) -> BoxProviderFuture<'a, std::result::Result<Admission, Error>> {
        Box::pin(async move {
            if destination.is_some() {
                return Ok(Admission::Allow(Vec::new()));
            }
            let creation = self.creation.lock().await.take();
            if let Some(creation) = creation {
                creation
                    .observed
                    .send(())
                    .map_err(|_| StreamError::Unavailable)?;
                creation
                    .created
                    .await
                    .map_err(|_| StreamError::Unavailable)?;
            }
            let original = message
                .relay_request(&self.paths, &message.source)?
                .idempotency_key;
            let mut identity = Sha256::new();
            identity.update(b"notice\0");
            identity.update(original.as_bytes());
            Ok(Admission::Reject {
                reason: Rejection::NoSuchActor,
                notice: Envelope {
                    source: NodeId::new(b"account-node")?,
                    sender: ActorReference::new(b"account", b"nodes", b"router")?,
                    to: message.sender.clone(),
                    request: CommitRequest {
                        deadline_unix_millis: None,
                        idempotency_key: IdempotencyKey::new(identity.finalize().to_vec())?,
                        effects: vec![CommitEffect::hard(
                            Vec::new(),
                            vec![CommitMutation::Append {
                                path: StreamPath::new("sender/notices")?,
                                records: vec!["unknown recipient".into()],
                            }],
                        )],
                    },
                },
                effects: Vec::new(),
            })
        })
    }
}

fn credential(node: &NodeId, sender: &ActorReference, peer: bool) -> Result<Bytes> {
    let resource = relay_resource(&AccountId::new(b"account")?, node);
    Ok(acyclic_auth::testing::root(|block| {
        let key = &mut block.certificates.as_mut().expect("test certificate").keys[0].key;
        key.principal.kind = PrincipalKind::Agent;
        key.principal.id = acyclic_auth::encode_component(&sender.encode());
        key.principal.lineage = Vec::new();
        key.principal.roles = if peer {
            vec!["agent.send-peer".into()]
        } else {
            Vec::new()
        };
        key.grants = GrantSet {
            grants: vec![Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: resource.clone(),
                        prefix: false,
                    },
                    actions: vec!["actors/invoke".into()],
                }),
            }],
        };
    })
    .into())
}

async fn place(
    provider: &MemoryStream,
    paths: &Paths,
    actor: &ActorReference,
    node: &NodeId,
) -> Result {
    let result = provider
        .commit(CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: IdempotencyKey::new(actor.encode())?,
            effects: vec![
                Placement {
                    actor: actor.clone(),
                    node: node.clone(),
                }
                .effect(paths)?,
            ],
        })
        .await?;
    assert!(matches!(result, CommitOutcome::Committed(_)));
    Ok(())
}

#[tokio::test]
async fn account_calls_enforce_source_and_message_authority() -> Result {
    let provider = Arc::new(MemoryStream::default());
    let paths = Paths::new(StreamPath::new("routing")?);
    let source = NodeId::new(b"source-node")?;
    let other = NodeId::new(b"other-node")?;
    let sender = ActorReference::new(b"account", b"sessions", b"sender")?;
    let recipient = ActorReference::new(b"account", b"sessions", b"recipient")?;
    place(&provider, &paths, &sender, &source).await?;
    place(&provider, &paths, &recipient, &other).await?;
    let service = Arc::new(AccountService::new(
        AccountId::new(b"account")?,
        provider.clone(),
        paths.clone(),
        Arc::new(PeerGrant {
            paths: paths.clone(),
            creation: tokio::sync::Mutex::new(None),
        }),
    ));
    let client = Client::in_process_with_products(
        provider.clone(),
        acyclic_stream::session::testing::authority(),
        None,
        service,
        Some(Arc::new(
            crate::local::Sockets::open(
                crate::local::Store::new(provider.clone(), StreamPath::new("node-state")?),
                Arc::new(acyclic_stream::runtime::PlatformExecutor),
                Arc::new(crate::local::SystemClock),
            )
            .await?,
        )),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    let another_sender = ActorReference::new(b"account", b"sessions", b"another-sender")?;
    let another_namespace = ActorReference::new(b"account", b"another-namespace", b"sender")?;
    for (slot, node, subject, peer) in [
        (0, &source, &sender, true),
        (1, &source, &sender, false),
        (2, &other, &sender, true),
        (3, &source, &another_sender, true),
        (4, &source, &another_namespace, true),
    ] {
        client.register_credential(slot, credential(node, subject, peer)?)?;
    }
    assert!(
        matches!(crate::route::open_link(&client, 0, b"account", &source).await,
        Err(Error::Stream(error)) if error.category() == StreamError::AccessDenied)
    );
    // The source header is sufficient to deny an ungranted request. The
    // missing body must not be parsed first and turn denial into a codec error.
    let header: Bytes = prost::Message::encode_to_vec(&crate::route::SourceWire {
        version: 1,
        source: Bytes::copy_from_slice(source.as_bytes()),
        domain: crate::route::CALL_PROTOCOL.into(),
    })
    .into();
    assert!(matches!(client.actor_call(&header, 2, 4096).await,
        Err(error) if error.category() == StreamError::AccessDenied));
    assert!(matches!(client.actor_call(&header, 0, 4096).await,
        Err(error) if error.category() == StreamError::InvalidArgument));
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
                    path: StreamPath::new("recipient/inbox")?,
                    records: vec!["peer input".into()],
                }],
            )],
        },
    };
    assert!(
        matches!(client.actor_call(&message.encode(), 0, 4096).await,
        Err(error) if error.category() == StreamError::InvalidArgument)
    );
    for slot in [1, 2, 3, 4] {
        assert!(matches!(admit(&client, slot, &message, 4096).await,
            Err(Error::Stream(error)) if error.category() == StreamError::AccessDenied));
    }
    let mut forged = message.clone();
    forged.source = other.clone();
    assert!(matches!(admit(&client, 2, &forged, 4096).await,
        Err(Error::Stream(error)) if error.category() == StreamError::AccessDenied));
    assert!(matches!(
        provider.tail(paths.relay(&other)?).await,
        Err(StreamError::NotFound)
    ));
    let original = admit(&client, 0, &message, 4096).await?;
    assert!(matches!(original, Routed::Relayed { relay_seq: 0, .. }));
    assert_eq!(admit(&client, 0, &message, 4096).await?, original);
    assert_eq!(provider.tail(paths.relay(&other)?).await?, 1);
    assert!(matches!(
        provider.tail(StreamPath::new("recipient/inbox")?).await,
        Err(StreamError::NotFound)
    ));
    let mut directory_write = message.clone();
    directory_write.request.effects[0].mutations[0] = CommitMutation::Append {
        path: StreamPath::new("registry/other-session")?,
        records: vec!["control".into()],
    };
    assert!(matches!(admit(&client, 0, &directory_write, 4096).await,
        Err(Error::Stream(error)) if error.category() == StreamError::AccessDenied));
    let mut selected_read = message.clone();
    selected_read.request.effects[0]
        .conditions
        .push(CommitCondition::Present {
            path: StreamPath::new("private/unrelated")?,
        });
    assert!(matches!(admit(&client, 0, &selected_read, 4096).await,
        Err(Error::Stream(error)) if error.category() == StreamError::AccessDenied));
    assert_eq!(provider.tail(paths.relay(&other)?).await?, 1);

    let mut missing = message.clone();
    missing.to = ActorReference::new(b"account", b"sessions", b"missing")?;
    missing.request.idempotency_key = IdempotencyKey::new(b"missing-message".to_vec())?;
    missing.request.effects[0].mutations[0] = CommitMutation::Append {
        path: StreamPath::new("missing/inbox")?,
        records: vec!["peer input".into()],
    };
    assert_eq!(
        admit(&client, 0, &missing, 4096).await?,
        Routed::Rejected(Rejection::NoSuchActor)
    );
    place(&provider, &paths, &missing.to, &other).await?;
    assert_eq!(
        admit(&client, 0, &missing, 4096).await?,
        Routed::Rejected(Rejection::NoSuchActor)
    );
    assert_eq!(provider.tail(paths.relay(&source)?).await?, 1);
    assert_eq!(provider.tail(paths.relay(&other)?).await?, 1);
    Ok(())
}

#[tokio::test]
async fn account_admission_revalidates_a_concurrently_created_recipient() -> Result {
    let provider = Arc::new(MemoryStream::default());
    let paths = Paths::new(StreamPath::new("routing")?);
    let source = NodeId::new(b"source-node")?;
    let destination = NodeId::new(b"destination-node")?;
    let sender = ActorReference::new(b"account", b"sessions", b"sender")?;
    let recipient = ActorReference::new(b"account", b"sessions", b"created-concurrently")?;
    place(&provider, &paths, &sender, &source).await?;
    let (observed, wait_observed) = tokio::sync::oneshot::channel();
    let (created, wait_created) = tokio::sync::oneshot::channel();
    let service = Arc::new(AccountService::new(
        AccountId::new(b"account")?,
        provider.clone(),
        paths.clone(),
        Arc::new(PeerGrant {
            paths: paths.clone(),
            creation: tokio::sync::Mutex::new(Some(CreationCut {
                observed,
                created: wait_created,
            })),
        }),
    ));
    let client = Client::in_process_with_products(
        provider.clone(),
        acyclic_stream::session::testing::authority(),
        None,
        service,
        None,
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(0, credential(&source, &sender, true)?)?;
    let message = Envelope {
        source: source.clone(),
        sender,
        to: recipient.clone(),
        request: CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: IdempotencyKey::new(b"concurrent-message".to_vec())?,
            effects: vec![CommitEffect::hard(
                Vec::new(),
                vec![CommitMutation::Append {
                    path: StreamPath::new("created-concurrently/inbox")?,
                    records: vec!["peer input".into()],
                }],
            )],
        },
    };
    let request = admit(&client, 0, &message, 4096);
    let creation = async {
        wait_observed.await?;
        place(&provider, &paths, &recipient, &destination).await?;
        created
            .send(())
            .map_err(|_| "account admission ended before creation")?;
        Ok(()) as Result
    };
    futures::pin_mut!(request, creation);
    let routed = match futures::future::select(request, creation).await {
        futures::future::Either::Left((result, _)) => {
            return Err(format!("admission ended before recipient creation: {result:?}").into());
        }
        futures::future::Either::Right((result, request)) => {
            result?;
            request.await?
        }
    };
    assert!(matches!(routed, Routed::Relayed { relay_seq: 0, .. }));
    assert_eq!(provider.tail(paths.relay(&destination)?).await?, 1);
    assert!(matches!(
        provider.tail(paths.relay(&source)?).await,
        Err(StreamError::NotFound)
    ));
    Ok(())
}
