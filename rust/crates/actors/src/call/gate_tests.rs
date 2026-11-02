//! The original actor publication, call journal and MemoryStream decide gate
//! races. There is no separate gate state machine or result stream here.
use super::*;
use crate::{
    addressing::ActorReference,
    guest::{AlarmChange, Checkpoint, CodecIdentity, Effect, Subscription, Transition, Trigger},
    local::{PreparedPublish, Store},
};
use acyclic_stream::{CommitOutcome, MemoryStream, StreamProviderExt as _};
use std::{collections::BTreeMap, sync::Arc};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn effects(commit: Commit) -> Vec<Effect> {
    let mut conditions = Some(commit.conditions);
    commit
        .mutations
        .into_iter()
        .map(|mutation| Effect {
            conditions: conditions.take().unwrap_or_default(),
            mutation,
        })
        .collect()
}

fn transition(commit: Commit) -> Transition {
    Transition {
        response: None,
        checkpoint: Checkpoint {
            codec: CodecIdentity {
                name: "gate-control".into(),
                version: "1".into(),
            },
            bytes: bytes::Bytes::from_static(b"decided"),
        },
        alarm: AlarmChange::Keep,
        effects: effects(commit),
        participant: None,
    }
}

async fn setup() -> Result<(
    Arc<MemoryStream>,
    Store<MemoryStream>,
    ActorReference,
    CallPath,
    Record,
)> {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), StreamPath::new("gates/actors")?);
    let actor = ActorReference::new(b"account", b"pool", b"gate")?;
    let input = StreamPath::new("gates/inbox")?;
    let call = CallPath::new(&StreamPath::new("caller")?, "request")?;
    let mut setup = PreparedPublish::foreign(IdempotencyKey::unique());
    for effect in store.create(
        &actor,
        BTreeMap::from([(
            "inbox".into(),
            Subscription {
                path: input.clone(),
                cursor: 0,
            },
        )]),
        None,
    )? {
        setup.join(effect)?;
    }
    for effect in effects(call.request(OwnedBytes::from(&b"original request"[..]))?) {
        setup.join(effect)?;
    }
    setup.join(Effect {
        conditions: Vec::new(),
        mutation: append(&input, AppendRecord::from(&b"judge"[..])),
    })?;
    PreparedPublish::settle(&provider.commit(setup.into_request()?).await?)?;
    let original = store.load(&actor).await?;
    store.claim(&original, 1, IdempotencyKey::unique()).await?;
    let delivered = provider
        .head(input)
        .await?
        .last
        .ok_or("missing gate delivery")?;
    Ok((provider, store, actor, call, delivered))
}

#[tokio::test]
async fn gate_decisions_share_the_original_actor_commit_and_caller_terminal() -> Result {
    let target = StreamPath::new("target/inbox")?;
    for winner in 0..3 {
        let (provider, store, actor, call, delivered) = setup().await?;
        let original = store.load(&actor).await?;
        let trigger = Trigger::Record {
            subscription: "inbox".into(),
            sequence: delivered.sequence,
        };
        let decisions = [
            call.forward(&target, AppendRecord::from(&b"final request"[..])),
            call.settle(None, &Outcome::Error("denied".into()))?,
            call.cancel("cancelled at gate")?,
        ];
        let mut publications = Vec::new();
        for decision in decisions {
            publications.push(
                store
                    .prepare_publish(
                        &original,
                        1,
                        &trigger,
                        Some(&delivered),
                        transition(decision),
                        IdempotencyKey::unique(),
                    )?
                    .into_request()?,
            );
        }
        let request = &publications[winner];
        let receipt = provider.commit(request.clone()).await?;
        PreparedPublish::settle(&receipt)?;
        assert_eq!(
            provider.commit(request.clone()).await?,
            receipt,
            "lost acknowledgement replays its receipt"
        );
        for (index, publication) in publications.into_iter().enumerate() {
            if index != winner {
                assert!(matches!(
                    provider.commit(publication).await?,
                    CommitOutcome::Conflict(_)
                ));
            }
        }
        let state = store.load(&actor).await?;
        assert_eq!(
            state.state().ok_or("missing decided gate")?.cursor("inbox"),
            Some(1)
        );
        if winner == 0 {
            let dispatched = provider.head(target.clone()).await?;
            assert_eq!(dispatched.tail, 1);
            let dispatched = dispatched.last.ok_or("missing final request")?;
            assert_eq!(dispatched.value.as_ref(), b"final request");
            assert_eq!(dispatched.commit_id, receipt_id(&receipt)?);
            assert_eq!(Some(dispatched.commit_id), state.follow_request().preceding);
            let accepted = call.accept(
                &Accepted {
                    executor: OwnedBytes::from(&b"target"[..]),
                    fence: None,
                },
                true,
            )?;
            PreparedPublish::settle(
                &provider
                    .commit(accepted.request(IdempotencyKey::unique()))
                    .await?,
            )?;
            let terminal = call.settle(None, &Outcome::Ok(OwnedBytes::from(&b"answer"[..])))?;
            PreparedPublish::settle(
                &provider
                    .commit(terminal.request(IdempotencyKey::unique()))
                    .await?,
            )?;
        } else {
            assert!(matches!(
                provider.head(target.clone()).await,
                Err(StreamError::NotFound)
            ));
            assert!(matches!(
                provider
                    .commit(
                        call.accept(
                            &Accepted {
                                executor: OwnedBytes::from(&b"late target"[..]),
                                fence: None
                            },
                            true
                        )?
                        .request(IdempotencyKey::unique())
                    )
                    .await?,
                CommitOutcome::Conflict(_)
            ));
        }
        let head = provider.head(call.log().clone()).await?;
        assert_eq!(head.tail, if winner == 0 { 3 } else { 2 });
        let outcome = Entry::decode(&head.last.ok_or("missing caller terminal")?)?;
        assert!(matches!(
            (&outcome, winner),
            (Entry::Terminal(Outcome::Ok(_)), 0)
                | (Entry::Terminal(Outcome::Error(_)), 1)
                | (Entry::Terminal(Outcome::Cancelled(_)), 2)
        ));
        assert_eq!(provider.head(call.settled().clone()).await?.tail, 1);
    }
    Ok(())
}

fn receipt_id(outcome: &CommitOutcome) -> Result<CommitId> {
    match outcome {
        CommitOutcome::Committed(receipt) => Ok(receipt.commit_id),
        _ => Err("publication conflicted".into()),
    }
}

#[tokio::test]
async fn cancellation_fences_a_prepared_gate_handoff_without_consuming_its_input() -> Result {
    for remove_guard in [false, true] {
        let (provider, store, actor, call, delivered) = setup().await?;
        let original = store.load(&actor).await?;
        let target = StreamPath::new("target/inbox")?;
        let record = AppendRecord::from(OwnedBytes::from(&b"final request"[..]));
        let body = record.value.clone();
        let mut forwarded = call.forward(&target, record);
        assert_eq!(forwarded.conditions.len(), 1);
        assert_eq!(forwarded.mutations.len(), 1);
        let CommitMutation::Append { records, .. } = &forwarded.mutations[0] else {
            return Err("handoff is not append".into());
        };
        assert!(
            std::ptr::eq(records[0].value.as_ptr(), body.as_ptr()),
            "handoff copied its payload"
        );
        if remove_guard {
            forwarded.conditions.clear();
        }
        let publication = store
            .prepare_publish(
                &original,
                1,
                &Trigger::Record {
                    subscription: "inbox".into(),
                    sequence: 0,
                },
                Some(&delivered),
                transition(forwarded),
                IdempotencyKey::unique(),
            )?
            .into_request()?;
        PreparedPublish::settle(
            &provider
                .commit(call.cancel("stop")?.request(IdempotencyKey::unique()))
                .await?,
        )?;
        let outcome = provider.commit(publication).await?;
        if remove_guard {
            // Red control: the original actor checkpoint fence alone allows
            // dispatch after the caller's terminal has already committed.
            PreparedPublish::settle(&outcome)?;
            assert_eq!(provider.head(target).await?.tail, 1);
        } else {
            assert!(matches!(outcome, CommitOutcome::Conflict(_)));
            assert_eq!(
                store.load(&actor).await?.follow_request(),
                original.follow_request()
            );
            assert!(matches!(
                provider.head(target).await,
                Err(StreamError::NotFound)
            ));
        }
        assert!(matches!(
            Entry::decode(
                &provider
                    .head(call.log().clone())
                    .await?
                    .last
                    .ok_or("missing cancelled terminal")?
            )?,
            Entry::Terminal(Outcome::Cancelled(_))
        ));
    }
    Ok(())
}
