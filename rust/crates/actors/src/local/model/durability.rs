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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_actor_acknowledgement_is_covered_by_the_device_durable_cut() -> Result<(), Failure> {
    for policy in [LocalDurability::FullFlush, LocalDurability::Barrier] {
        for confirmation_faults in [false, true] {
            assert!(
                acknowledged_cuts(policy, false, confirmation_faults).await?,
                "Actors acknowledged a publication before its device durable cut: {policy:?}, confirmation_faults={confirmation_faults}"
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn negative_control_unsealed_acceptance_fails_the_durable_cut_oracle() -> Result<(), Failure>
{
    assert!(!acknowledged_cuts(LocalDurability::FullFlush, true, false).await?);
    Ok(())
}
