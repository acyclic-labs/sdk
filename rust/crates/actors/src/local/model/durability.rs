//! Observe the real local journal's durable cut at each Actors acknowledgement.
use super::*;
use acyclic_stream::{LocalDurability, LocalStream, LocalStreamLimits};

async fn durable(provider: &LocalStream, path: StreamPath, through: u64) -> Result<bool, Failure> {
    let observed = provider.watermarks(path).await?;
    Ok(through <= observed.durable && observed.durable <= observed.accepted)
}

/// Both device publication policies and an ambient deferred-write scope must
/// leave acknowledgements behind the same physical journal durability oracle.
async fn acknowledged_cuts(
    policy: LocalDurability,
    bypass: bool,
    confirmation_faults: bool,
) -> Result<bool, Failure> {
    let directory = tempfile::tempdir()?;
    let provider = Arc::new(Faulty {
        inner: LocalStream::open(
            directory.path(),
            LocalStreamLimits {
                durability: policy,
                ..LocalStreamLimits::default()
            },
        )
        .await?,
        plan: if confirmation_faults {
            vec![
                Fault::LoseDurabilityAck(StreamError::AccessDenied),
                Fault::None, // exact creation replay
                Fault::None, // claim
                Fault::None, // invocation admission
                Fault::LoseDurabilityAck(StreamError::AccessDenied),
            ]
        } else {
            Vec::new()
        },
        calls: AtomicUsize::new(0),
    });
    let store = Store::new(provider.clone(), path("durability/actors")?);
    let actor = ActorReference::new(b"account", b"agents", b"durability")?;
    acyclic_stream::deferring_durability(async {
        let mut creation = PreparedPublish::foreign(IdempotencyKey::new(b"create".to_vec())?);
        creation.join(store.create(
            None,
            &actor,
            Creation {
                subscriptions: BTreeMap::from([(
                    "inbox".into(),
                    Subscription {
                        path: path("durability/inbox")?,
                        paused: false,
                        cursor: 0,
                    },
                )]),
                ..Creation::default()
            },
        )?)?;
        creation.join(store.activation(&actor)?)?;
        if bypass {
            // Deliberately acknowledge the same real journal acceptance while
            // omitting the Actors durability boundary.
            PreparedPublish::settle(&provider.commit(creation.into_request()?).await?)?;
        } else {
            match store.submit(creation).await {
                Ok(()) if !confirmation_faults => {}
                Err(Error::Unknown {
                    publication,
                    source,
                }) if confirmation_faults && source == StreamError::AccessDenied => {
                    store.submit(*publication).await?;
                }
                _ => return Ok(false),
            }
        }
        if !durable(&provider.inner, store.path(&actor)?, 1).await? {
            return Ok(false);
        }
        store
            .claim(
                &store.load(&actor).await?,
                1,
                IdempotencyKey::new(b"claim".to_vec())?,
            )
            .await?;
        if !durable(&provider.inner, store.path(&actor)?, 2).await? {
            return Ok(false);
        }
        let invocation = crate::invocation::Invocation::new(
            path("durability/caller")?,
            actor.clone(),
            "inbox".into(),
            Bytes::from_static(b"operation"),
            Bytes::from_static(b"request"),
        )?;
        let ticket = store.enqueue_invoke(&invocation).await?;
        if !durable(
            &provider.inner,
            path("durability/inbox")?,
            ticket.record().sequence + 1,
        )
        .await?
        {
            return Ok(false);
        }
        let before = store.load(&actor).await?;
        let publication = store.prepare_publish(
            &before,
            1,
            &Trigger::Record {
                subscription: "inbox".into(),
                sequence: ticket.record().sequence,
            },
            Some(ticket.record()),
            Transition {
                pause: false,
                response: Some(Bytes::from_static(b"completed")),
                participant: None,
                checkpoint: checkpoint(1),
                alarm: AlarmChange::Set {
                    deadline_unix_ms: 10,
                },
                pause: false,
                effects: Vec::new(),
                subscriptions: Vec::new(),
            },
            IdempotencyKey::new(b"complete".to_vec())?,
        )?;
        // Stop at the real provider acceptance between Store's write and its
        // durability confirmation. A response observer runs at this cut.
        PreparedPublish::settle(&provider.commit(publication.request.clone()).await?)?;
        assert!(
            !durable(&provider.inner, store.path(&actor)?, before.tail + 1).await?,
            "the simulation must hold the publisher before its durable cut"
        );
        let call = ticket.call()?;
        let observed = store.wait_invocation(ticket.clone()).await;
        let original = match observed {
            Ok(completed) if !confirmation_faults => completed.receipt().call().clone(),
            Err(Error::InvocationDurabilityUnknown { receipt, source })
                if confirmation_faults && source == StreamError::AccessDenied =>
            {
                if receipt.outcome
                    != crate::call::Outcome::Ok(Bytes::from_static(b"completed").into())
                {
                    return Ok(false);
                }
                *receipt
            }
            _ => return Ok(false),
        };
        let observed_durable = durable(&provider.inner, store.path(&actor)?, before.tail + 1)
            .await?
            && durable(
                &provider.inner,
                call.log().clone(),
                original.terminal.sequence + 1,
            )
            .await?;
        // Resume the original publisher with its exact request. Its later
        // acknowledgement cannot retroactively certify the observer's reply.
        store.submit(publication).await?;
        let completed = store.wait_invocation(ticket).await?;
        Ok(observed_durable && *completed.receipt().call() == original)
    })
    .await
}

async fn replay_control<P: StreamProvider>(
    store: &Store<P>,
    actor: &ActorReference,
    fork: bool,
) -> Result<ActorReference, Error> {
    if fork {
        store
            .fork_actor(
                &[],
                &crate::wire::ForkActorRequest {
                    namespace: Some(actor.namespace().as_str().into()),
                    account_id: actor.account().as_bytes().to_vec(),
                    actor_id: actor.id().into(),
                    child_actor_id: "forked".into(),
                    subscriptions: vec![crate::wire::SubscriptionSpec {
                        subscription_id: "events".into(),
                        stream_path: "durability/events".into(),
                        start: Some(crate::wire::SubscriptionStart {
                            start: Some(crate::wire::subscription_start::Start::Cursor(0)),
                        }),
                        placement_anchor: false,
                    }],
                    idempotency_key: "fork-held".into(),
                },
            )
            .await
    } else {
        store
            .resume_subscription(
                &[],
                &crate::wire::ResumeSubscriptionRequest {
                    namespace: Some(actor.namespace().as_str().into()),
                    account_id: actor.account().as_bytes().to_vec(),
                    actor_id: actor.id().into(),
                    subscription_id: "events".into(),
                    idempotency_key: "resume-held".into(),
                },
            )
            .await
    }
}

async fn control_replay_requires_durability(
    policy: LocalDurability,
    fork: bool,
) -> Result<(), Failure> {
    let directory = tempfile::tempdir()?;
    let provider = Arc::new(Faulty {
        inner: LocalStream::open(
            directory.path(),
            LocalStreamLimits {
                durability: policy,
                ..LocalStreamLimits::default()
            },
        )
        .await?,
        plan: vec![
            Fault::None,
            Fault::HoldDurability(StreamError::AccessDenied),
        ],
        calls: AtomicUsize::new(0),
    });
    let root = path("durability/subscription-actors")?;
    let store = Store::new(provider.clone(), root.clone());
    let actor = ActorReference::new(b"account", b"agents", b"subscription")?;
    let target = if fork {
        ActorReference::new(b"account", b"agents", b"forked")?
    } else {
        actor.clone()
    };
    acyclic_stream::deferring_durability(async {
        let mut creation = PreparedPublish::foreign(IdempotencyKey::new(b"create-held".to_vec())?);
        creation.join(store.create(
            None,
            &actor,
            Creation {
                subscriptions: BTreeMap::from([(
                    "events".into(),
                    Subscription {
                        path: path("durability/events")?,
                        cursor: 0,
                        paused: true,
                    },
                )]),
                checkpoint: Some(checkpoint(9)),
                ..Creation::default()
            },
        )?)?;
        creation.join(store.activation(&actor)?)?;
        store.submit(creation).await?;
        let before = store.load(&actor).await?;
        let schedule = store.scheduling_path(store.scheduling_shard(&actor))?;
        let activations = provider.inner.tail(schedule.clone()).await?;
        assert!(matches!(
            replay_control(&store, &actor, fork).await,
            Err(Error::Unknown {
                source: StreamError::AccessDenied,
                ..
            })
        ));
        let accepted = store.load(&target).await?;
        let state = accepted.state().ok_or("missing accepted state")?;
        assert_eq!(state.cursor("events"), Some(0));
        assert_eq!(state.subscriptions()["events"].paused, fork);
        assert_eq!(
            state.checkpoint(),
            before.state().ok_or("missing held state")?.checkpoint()
        );
        if fork {
            assert_eq!(store.load(&actor).await?.tail, before.tail);
        } else {
            assert_eq!(accepted.tail, before.tail + 1);
            let mut expected = before.state().ok_or("missing held state")?.clone();
            expected
                .subscriptions
                .get_mut("events")
                .ok_or("missing held input")?
                .paused = false;
            assert_eq!(accepted.state(), Some(&expected));
        }
        assert!(!durable(&provider.inner, store.path(&target)?, accepted.tail).await?);
        let Err(Error::PublicationDurabilityUnknown { envelope, source }) =
            replay_control(&store, &actor, fork).await
        else {
            return Err("unconfirmed subscription replay was acknowledged".into());
        };
        assert_eq!(source, StreamError::AccessDenied);
        let state_path = store.path(&target)?;
        assert!(envelope.mutations.iter().any(|mutation| match mutation {
            acyclic_stream::CommittedMutation::Append(append) =>
                append.path == state_path && append.end == accepted.tail,
            acyclic_stream::CommittedMutation::Fork(fork) =>
                fork.destination == state_path && fork.tail == accepted.tail,
        }));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert!(!durable(&provider.inner, state_path.clone(), accepted.tail).await?);
        // Restore confirmation on the same actual device. The original receipt
        // is sealed, rather than publishing a new state or another activation.
        let restored = Store::new(Arc::new(provider.inner.clone()), root.clone());
        assert_eq!(replay_control(&restored, &actor, fork).await?, target);
        assert!(durable(&provider.inner, state_path, accepted.tail).await?);
        assert!(durable(&provider.inner, schedule.clone(), activations + 1).await?);
        replay_control(&restored, &actor, fork).await?;
        let recovered = restored.load(&target).await?;
        assert_eq!(recovered.tail, accepted.tail);
        assert_eq!(recovered.state(), accepted.state());
        assert_eq!(provider.inner.tail(schedule).await?, activations + 1);
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_actor_acknowledgement_is_covered_by_the_device_durable_cut() -> Result<(), Failure> {
    for policy in [LocalDurability::FullFlush, LocalDurability::Barrier] {
        for confirmation_faults in [false, true] {
            assert!(
                acknowledged_cuts(policy, false, confirmation_faults).await?,
                "Actors acknowledged a publication before its device durable cut: {policy:?}, confirmation_faults={confirmation_faults}"
            );
        }
        for fork in [false, true] {
            control_replay_requires_durability(policy, fork).await?;
        }
        super::composition::recovered_custody_requires_durability(policy).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn negative_control_unsealed_acceptance_fails_the_durable_cut_oracle() -> Result<(), Failure>
{
    assert!(!acknowledged_cuts(LocalDurability::FullFlush, true, false).await?);
    Ok(())
}
