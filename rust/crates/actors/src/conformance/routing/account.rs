//! Signed account admission shares the backend's provider and running receiver.
use super::*;
use crate::route::{Admission, AdmissionPolicy};
use acyclic_auth::contract::{
    Grant, GrantKind, GrantSet, PrincipalKind, ResourceGrant, ResourcePattern,
};
use acyclic_stream::{
    BoxProviderFuture, HeadProjection, HeadsRequest,
    session::{CallAuthorization, Client},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct Reservation<P: StreamProvider + ?Sized> {
    provider: Arc<P>,
    paths: Paths,
    capacity: u64,
    prepared: AtomicUsize,
    waiting: Mutex<Option<futures::channel::oneshot::Sender<()>>>,
}

impl<P: StreamProvider + ?Sized> AdmissionPolicy for Reservation<P> {
    fn authorize(
        &self,
        authorization: &CallAuthorization,
        message: &Envelope,
    ) -> Result<(), StreamError> {
        let principal = authorization.principal()?;
        if principal.kind != PrincipalKind::Agent
            || principal.id != acyclic_auth::encode_component(&message.sender.encode())
            || !principal.roles.iter().any(|role| role == "agent.send-peer")
        {
            return Err(StreamError::AccessDenied);
        }
        Ok(())
    }

    fn prepare<'a>(
        &'a self,
        _: &'a CallAuthorization,
        _: &'a Envelope,
        destination: Option<&'a Placement>,
    ) -> BoxProviderFuture<'a, Result<Admission, crate::route::Error>> {
        Box::pin(async move {
            self.prepared.fetch_add(1, Ordering::SeqCst);
            let destination = destination.ok_or(crate::route::Error::InvalidRecord)?;
            let relay = self.paths.relay(&destination.node)?;
            let cursor = self.paths.relay_cursor(&destination.node)?;
            let request = HeadsRequest {
                retained: Default::default(),
                durable_through: None,
                projection: HeadProjection::Tail,
                paths: [relay.clone(), cursor.clone()].into_iter().collect(),
                children: None,
                limit: 2,
                at_least: Default::default(),
            };
            let cut = self.provider.heads(request.clone()).await?;
            cut.validate_for(&request)?;
            let admitted = cut.heads[0].as_ref().map_or(0, |head| head.tail);
            // The native drain commits one cursor record per consumed relay
            // record, and validates that this tail equals its consumed prefix.
            let drained = cut.heads[1].as_ref().map_or(0, |head| head.tail);
            let pending = admitted
                .checked_sub(drained)
                .ok_or(crate::route::Error::InvalidRecord)?;
            if pending >= self.capacity {
                let mut changes = acyclic_stream::records_only(
                    self.provider
                        .follow(acyclic_stream::FollowRequest {
                            when_absent: acyclic_stream::WhenAbsent::Wait,
                            ..acyclic_stream::FollowRequest::new(cursor, drained)
                        })
                        .await?,
                );
                if let Some(waiting) = self
                    .waiting
                    .lock()
                    .map_err(|_| StreamError::Unavailable)?
                    .take()
                {
                    waiting.send(()).map_err(|_| StreamError::Unavailable)?;
                }
                return Ok(Admission::Credit {
                    changed: Box::pin(async move {
                        changes.try_next().await?.ok_or(StreamError::Unavailable)?;
                        Ok(())
                    }),
                });
            }
            Ok(Admission::Allow(vec![CommitEffect::hard(
                vec![
                    CommitCondition::Tail {
                        path: relay,
                        expected: admitted,
                    },
                    CommitCondition::Tail {
                        path: cursor,
                        expected: drained,
                    },
                ],
                Vec::new(),
            )]))
        })
    }
}

fn credential(sender: &ActorReference, node: &NodeId, peer: bool) -> bytes::Bytes {
    let resource = crate::route::relay_resource(sender.account(), node);
    acyclic_auth::testing::root(|block| {
        let key = &mut block.certificates.as_mut().expect("signed fixture").keys[0].key;
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
    .into()
}

/// A configured one-record reservation admits the first send, holds the next
/// on the actual drain event, and delivers both to the backend's running actor.
/// Removing the credit wait makes the pre-drain observation fail immediately.
pub async fn credited_delivery<T: Backend + ?Sized>(target: &T, run: &str) -> Result<(), Failure> {
    let provider = target.store().provider().clone();
    let root = StreamPath::new(format!("{}/routing-credit", target.store().root()))
        .map_err(Error::from)?;
    let paths = Paths::new(root.clone());
    let source = NodeId::new(b"source").map_err(violation)?;
    let destination = NodeId::new(b"destination").map_err(violation)?;
    let mut sender =
        Fixture::spawn_in_account(target, run, "credit-source", None, b"account").await?;
    let mut receiver =
        Fixture::spawn_in_account(target, run, "credit-recipient", None, b"account").await?;
    for (fixture, node) in [(&mut sender, &source), (&mut receiver, &destination)] {
        fixture
            .creation
            .as_mut()
            .ok_or_else(|| violation("actor creation already published"))?
            .push(
                Placement {
                    actor: fixture.actor.clone(),
                    node: node.clone(),
                }
                .effect(&paths)
                .map_err(violation)?,
            );
        fixture.send("emit").await?;
        fixture.expect(&["r0"]).await?;
    }
    let policy = Arc::new(Reservation {
        provider: provider.clone(),
        paths: paths.clone(),
        capacity: 1,
        prepared: AtomicUsize::new(0),
        waiting: Mutex::new(None),
    });
    let service = Arc::new(crate::route::AccountService::new(
        sender.actor.account().clone(),
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
    client
        .register_credential(0, credential(&sender.actor, &source, true))
        .map_err(Error::from)?;
    client
        .register_credential(1, credential(&sender.actor, &source, false))
        .map_err(Error::from)?;
    client
        .register_credential(2, credential(&receiver.actor, &source, true))
        .map_err(Error::from)?;
    let other_namespace = ActorReference::new(
        sender.actor.account().as_bytes(),
        b"another-namespace",
        sender.actor.id().as_bytes(),
    )
    .map_err(violation)?;
    client
        .register_credential(3, credential(&other_namespace, &source, true))
        .map_err(Error::from)?;
    let mut message = Envelope {
        source,
        sender: sender.actor.clone(),
        to: receiver.actor.clone(),
        request: CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: key(&root, "first")?,
            effects: vec![CommitEffect::hard(
                Vec::new(),
                vec![CommitMutation::Append {
                    path: receiver.inbox.clone(),
                    records: vec!["emit".into()],
                }],
            )],
        },
    };
    for slot in [1, 2, 3] {
        require(
            matches!(crate::route::admit(&client, slot, &message, 4096).await,
            Err(crate::route::Error::Stream(error)) if error.category() == StreamError::AccessDenied),
            "missing or substituted sender authority admitted a relay",
        )?;
    }
    let mut forged_source = message.clone();
    forged_source.source = destination.clone();
    require(
        matches!(crate::route::admit(&client, 0, &forged_source, 4096).await,
            Err(crate::route::Error::Stream(error)) if error.category() == StreamError::AccessDenied),
        "source credential routed under another node's scope",
    )?;
    require(
        policy.prepared.load(Ordering::SeqCst) == 0,
        "denied authority reached storage reservation",
    )?;
    require(
        matches!(
            crate::route::admit(&client, 0, &message, 4096)
                .await
                .map_err(violation)?,
            Routed::Relayed { relay_seq: 0, .. }
        ),
        "funded first send was refused",
    )?;
    message.request.idempotency_key = key(&root, "second")?;
    let (waiting, observed) = futures::channel::oneshot::channel();
    *policy
        .waiting
        .lock()
        .map_err(|_| violation("reservation lock failed"))? = Some(waiting);
    let pending = crate::route::admit(&client, 0, &message, 4096);
    futures::pin_mut!(pending, observed);
    let pending = match futures::future::select(pending, observed).await {
        futures::future::Either::Left(_) => return Err(violation("unfunded send did not wait")),
        futures::future::Either::Right((observed, pending)) => {
            observed.map_err(|_| violation("credit observation ended"))?;
            pending
        }
    };
    require(
        provider
            .tail(paths.relay(&destination).map_err(Error::from)?)
            .await
            .map_err(Error::from)?
            == 1,
        "credit exhaustion appended another relay record",
    )?;
    receiver.expect(&["r0"]).await?;
    let drain = crate::route::drain(provider.as_ref(), &paths, &destination, async |record| {
        receiver.deliver(record.envelope.request).await?;
        Ok(())
    });
    let drained = until_ack(
        provider.as_ref(),
        paths.relay_cursor(&destination).map_err(Error::from)?,
        0,
        drain,
    );
    let (admitted, drained) = futures::join!(pending, drained);
    drained?;
    require(
        matches!(
            admitted.map_err(violation)?,
            Routed::Relayed { relay_seq: 1, .. }
        ),
        "drain event did not release the waiting send",
    )?;
    require(
        policy.prepared.load(Ordering::SeqCst) == 3,
        "credit was polled or reservation preparation was skipped",
    )?;
    let drain = crate::route::drain(provider.as_ref(), &paths, &destination, async |record| {
        receiver.deliver(record.envelope.request).await?;
        Ok(())
    });
    until_ack(
        provider.as_ref(),
        paths.relay_cursor(&destination).map_err(Error::from)?,
        1,
        drain,
    )
    .await?;
    receiver.expect(&["r0", "r1", "r2"]).await
}
