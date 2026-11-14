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
    preparation_unavailable: std::sync::atomic::AtomicBool,
}

struct CreationCut {
    observed: tokio::sync::oneshot::Sender<()>,
    created: tokio::sync::oneshot::Receiver<()>,
}

struct CreditCut {
    observed: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
    waiting: tokio::sync::oneshot::Sender<()>,
    available: tokio::sync::oneshot::Receiver<()>,
}

struct CreditRace {
    peer: PeerGrant,
    cut: tokio::sync::Mutex<Option<CreditCut>>,
}

struct GuardPolicy {
    peer: PeerGrant,
    guard: StreamPath,
    attempts: std::sync::atomic::AtomicUsize,
    repair: bool,
}

impl AdmissionPolicy for GuardPolicy {
    fn authorize(
        &self,
        authorization: &CallAuthorization,
        message: &Envelope,
    ) -> std::result::Result<(), StreamError> {
        self.peer.authorize(authorization, message)
    }

    fn prepare<'a>(
        &'a self,
        _: &'a CallAuthorization,
        _: &'a Envelope,
        _: Option<&'a Placement>,
    ) -> BoxProviderFuture<'a, std::result::Result<Admission, Error>> {
        Box::pin(async move {
            let attempt = self
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            // A broken progress check fails this test finitely, at its third
            // preparation, rather than issuing an unbounded storage loop.
            if attempt > 2 {
                return Err(Error::InvalidRecord);
            }
            Ok(Admission::Allow(vec![CommitEffect::hard(
                vec![CommitCondition::Tail {
                    path: self.guard.clone(),
                    expected: u64::from(self.repair && attempt > 1),
                }],
                Vec::new(),
            )]))
        })
    }
}

#[tokio::test]
async fn account_admission_stops_unchanged_refusals_but_allows_recomputed_guards() -> Result {
    for repair in [false, true] {
        let provider = Arc::new(MemoryStream::default());
        let paths = Paths::new(StreamPath::new("routing")?);
        let source = NodeId::new(b"source")?;
        let destination = NodeId::new(b"destination")?;
        let sender = ActorReference::new(b"account", b"sessions", b"sender")?;
        let recipient = ActorReference::new(b"account", b"sessions", b"recipient")?;
        place(&provider, &paths, &sender, &source).await?;
        place(&provider, &paths, &recipient, &destination).await?;
        let guard = StreamPath::new("reservation")?;
        provider
            .append(acyclic_stream::AppendRequest {
                path: guard.clone(),
                records: vec!["funded".into()],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::new(b"fund-reservation".to_vec())?,
            })
            .await?;
        let policy = Arc::new(GuardPolicy {
            peer: PeerGrant {
                paths: paths.clone(),
                creation: tokio::sync::Mutex::new(None),
                preparation_unavailable: false.into(),
            },
            guard,
            attempts: 0.into(),
            repair,
        });
        let service = Arc::new(AccountService::new(
            AccountId::new(b"account")?,
            provider.clone(),
            paths.clone(),
            policy.clone(),
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
            source,
            sender,
            to: recipient,
            request: CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: IdempotencyKey::new(b"guarded-input".to_vec())?,
                effects: vec![CommitEffect::hard(
                    Vec::new(),
                    vec![CommitMutation::Append {
                        path: StreamPath::new("recipient/inbox")?,
                        records: vec!["input".into()],
                    }],
                )],
            },
        };
        let result = admit(&client, 0, &message, 4096).await;
        assert_eq!(policy.attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
        if repair {
            assert!(matches!(result?, Routed::Relayed { relay_seq: 0, .. }));
            assert_eq!(provider.tail(paths.relay(&destination)?).await?, 1);
        } else {
            assert!(matches!(result, Err(Error::Stream(error))
                if error.category() == StreamError::InvalidResponse));
            assert!(matches!(
                provider.tail(paths.relay(&destination)?).await,
                Err(StreamError::NotFound)
            ));
        }
    }
    Ok(())
}

impl AdmissionPolicy for CreditRace {
    fn authorize(
        &self,
        authorization: &CallAuthorization,
        message: &Envelope,
    ) -> std::result::Result<(), StreamError> {
        self.peer.authorize(authorization, message)
    }

    fn prepare<'a>(
        &'a self,
        authorization: &'a CallAuthorization,
        message: &'a Envelope,
        destination: Option<&'a Placement>,
    ) -> BoxProviderFuture<'a, std::result::Result<Admission, Error>> {
        Box::pin(async move {
            let cut = self.cut.lock().await.take();
            if let Some(cut) = cut {
                cut.observed
                    .send(())
                    .map_err(|_| StreamError::Unavailable)?;
                cut.resume.await.map_err(|_| StreamError::Unavailable)?;
                return Ok(Admission::Credit {
                    changed: Box::pin(async move {
                        let _ = cut.waiting.send(());
                        cut.available.await.map_err(|_| StreamError::Unavailable)?;
                        Ok(())
                    }),
                });
            }
            self.peer.prepare(authorization, message, destination).await
        })
    }
}

#[tokio::test]
async fn original_admission_wins_a_concurrent_credit_wait() -> Result {
    let provider = Arc::new(MemoryStream::default());
    let paths = Paths::new(StreamPath::new("routing")?);
    let source = NodeId::new(b"source")?;
    let destination = NodeId::new(b"destination")?;
    let sender = ActorReference::new(b"account", b"sessions", b"sender")?;
    let recipient = ActorReference::new(b"account", b"sessions", b"recipient")?;
    place(&provider, &paths, &sender, &source).await?;
    place(&provider, &paths, &recipient, &destination).await?;
    let policy = Arc::new(CreditRace {
        peer: PeerGrant {
            paths: paths.clone(),
            creation: tokio::sync::Mutex::new(None),
            preparation_unavailable: false.into(),
        },
        cut: tokio::sync::Mutex::new(None),
    });
    let service = Arc::new(AccountService::new(
        AccountId::new(b"account")?,
        provider.clone(),
        paths.clone(),
        policy.clone(),
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
        source,
        sender,
        to: recipient,
        request: CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: IdempotencyKey::new(b"original".to_vec())?,
            effects: vec![CommitEffect::hard(
                Vec::new(),
                vec![CommitMutation::Append {
                    path: StreamPath::new("recipient/inbox")?,
                    records: vec!["input".into()],
                }],
            )],
        },
    };
    for replay in [true, false] {
        let (observed, wait_observed) = tokio::sync::oneshot::channel();
        let (resume, wait_resume) = tokio::sync::oneshot::channel();
        let (waiting, mut wait_waiting) = tokio::sync::oneshot::channel();
        let (available, wait_available) = tokio::sync::oneshot::channel();
        *policy.cut.lock().await = Some(CreditCut {
            observed,
            resume: wait_resume,
            waiting,
            available: wait_available,
        });
        let mut selected = message.clone();
        if !replay {
            selected.request.idempotency_key = IdempotencyKey::new(b"fresh".to_vec())?;
        }
        let request = admit(&client, 0, &selected, 4096);
        tokio::pin!(request);
        tokio::select! {
            observed = wait_observed => observed?,
            result = &mut request => panic!("request skipped its controlled preparation: {result:?}"),
        }
        if replay {
            // Another authenticated call commits after the first inspection,
            // before this caller observes the exhausted-credit result.
            let original = admit(&client, 0, &message, 4096).await?;
            assert!(matches!(original, Routed::Relayed { relay_seq: 0, .. }));
        }
        resume.send(()).map_err(|_| "preparation ended early")?;
        let routed = if replay {
            tokio::select! {
                result = &mut request => result?,
                entered = &mut wait_waiting => {
                    assert!(entered.is_err(), "accepted work requested new credit");
                    request.await?
                }
            }
        } else {
            // Negative control: a different intent really waits on the event.
            tokio::select! {
                entered = &mut wait_waiting => entered?,
                result = &mut request => panic!("fresh work bypassed credit: {result:?}"),
            }
            available.send(()).map_err(|_| "credit wait ended early")?;
            request.await?
        };
        assert_eq!(
            routed,
            Routed::Relayed {
                key: selected.request.idempotency_key.clone(),
                relay_seq: u64::from(!replay),
            }
        );
        assert_eq!(
            provider.tail(paths.relay(&destination)?).await?,
            if replay { 1 } else { 2 }
        );
    }
    Ok(())
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
            if self
                .preparation_unavailable
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(StreamError::Unavailable.into());
            }
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
    let policy = Arc::new(PeerGrant {
        paths: paths.clone(),
        creation: tokio::sync::Mutex::new(None),
        preparation_unavailable: false.into(),
    });
    let service = Arc::new(AccountService::new(
        AccountId::new(b"account")?,
        provider.clone(),
        paths.clone(),
        policy.clone(),
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
    assert_eq!(
        original,
        Routed::Relayed {
            key: message.request.idempotency_key.clone(),
            relay_seq: 0,
        }
    );
    policy
        .preparation_unavailable
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(admit(&client, 0, &message, 4096).await?, original);
    // Negative control: a new intent reaches the unavailable preparation path.
    // Only the accepted intent can recover without another storage admission.
    let mut fresh = message.clone();
    fresh.request.idempotency_key = IdempotencyKey::new(b"fresh-message".to_vec())?;
    assert!(matches!(admit(&client, 0, &fresh, 4096).await,
        Err(Error::Stream(error)) if error.category() == StreamError::Unavailable));
    let mut changed = message.clone();
    let CommitMutation::Append { records, .. } = &mut changed.request.effects[0].mutations[0]
    else {
        return Err("message has no append".into());
    };
    records[0].value = "changed input".into();
    assert!(matches!(admit(&client, 0, &changed, 4096).await,
        Err(Error::Stream(error)) if error.category() == StreamError::IdempotencyMismatch));
    assert!(matches!(admit(&client, 1, &message, 4096).await,
        Err(Error::Stream(error)) if error.category() == StreamError::AccessDenied));
    policy
        .preparation_unavailable
        .store(false, std::sync::atomic::Ordering::SeqCst);
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
            preparation_unavailable: false.into(),
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
