use super::*;
use crate::agent::{ApprovalRequested, Cancellation, Queued, Sender, TurnFuture, TurnStep, Wait};
use acyclic_actors::guest::{CancellationToken, SubscriptionCuts, wire};
use acyclic_stream::{AppendRequest, CommitOutcome, MemoryStream, StreamProviderExt as _};
use tokio::sync::oneshot;

struct Opening(Mutex<Option<oneshot::Sender<u64>>>);

impl TurnDriver for Opening {
    fn step<'a>(
        &'a self,
        _: &'a AgentState,
        _: u64,
        input: Option<Queued>,
        cancellation: CancellationToken,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            let input = input.expect("refilled input").inbox;
            self.0.lock().unwrap().take().unwrap().send(input).unwrap();
            cancellation.cancelled().await;
            Ok(TurnStep {
                checkpoint: None,
                wait: Wait::Input,
                drops: None,
                outs: vec![Out::InputCancelled {
                    input,
                    conversation: 0,
                }],
            })
        })
    }
}

#[tokio::test]
async fn alarm_opening_from_overflow_accepts_only_its_selected_input_interrupt()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(MemoryStream::default());
    let id = AgentId::from_bytes([7; 16]);
    let adopted = ForkAdopted {
        agent: id.clone(),
        parent: None,
        profile: None,
        forked_at: 0,
        conversation_tail: 0,
        checkpoint: None,
        volumes: Vec::new(),
        brief: None,
        policy: None,
    };
    let created = crate::agent::create_root(&"create".try_into()?, adopted.clone())?;
    let path = created.child;
    let CommitOutcome::Committed(_) = provider.commit(created.request).await? else {
        panic!("creation refused");
    };
    provider
        .append(AppendRequest {
            path: path.inbox()?,
            records: vec![
                InboxRecord::Awaited.encode()?.into(),
                InboxRecord::ApprovalRequested(ApprovalRequested {
                    child: path.child(&AgentId::from_bytes([8; 16]))?,
                    id: "request".try_into()?,
                })
                .encode()?
                .into(),
            ],
            if_tail: Some(1),
            idempotency_key: crate::agent::intent_key("overflow", &[])?,
        })
        .await?;
    let mut state = AgentState::adopted(path.clone(), &adopted)?;
    state.overflow = Some(1);
    let checkpoint =
        crate::agent::checkpoint::from_encoded(crate::agent::checkpoint::encode(&state)?)?;
    let actor = ActorReference::new(b"account", b"agents", id.as_str().as_bytes())?;
    let store = crate::agent::actor_store(&provider)?;
    let subscriptions = BTreeMap::from([(
        INBOX.into(),
        Subscription {
            path: path.inbox()?,
            cursor: 3,
        },
    )]);
    let CommitOutcome::Committed(receipt) = provider
        .commit(acyclic_stream::CommitRequest {
            deadline_unix_millis: None,
            effects: vec![store.create(
                None,
                &actor,
                Creation {
                    checkpoint: Some(checkpoint.clone()),
                    subscriptions: subscriptions.clone(),
                    ..Creation::default()
                },
            )?],
            idempotency_key: crate::agent::intent_key("checkpoint", &[])?,
        })
        .await?
    else {
        panic!("actor checkpoint refused");
    };
    let current = crate::agent::settled(&provider, &actor, &path).await?;
    assert_eq!(current.input, Some(2));
    assert_eq!(current.phase, crate::agent::Phase::Running);
    assert!(!current.settled);
    let delivery = Delivery {
        actor: actor.clone(),
        epoch: 1,
        now_unix_ms: 100,
        trigger: Trigger::Alarm {
            generation: 1,
            now_unix_ms: 100,
        },
        record: None,
        subscriptions: SubscriptionCuts::new(store.path(&actor)?, receipt.commit_id, subscriptions),
    };
    let (started, starting) = oneshot::channel();
    let handler = AgentHandler::new(
        store,
        Opening(Mutex::new(Some(started))),
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
    )?;
    let stop = CancellationToken::new();
    let running = handler.on_record(delivery.clone(), Some(&checkpoint), stop.clone());
    tokio::pin!(running);
    tokio::select! {
        input = starting => assert_eq!(input?, 2),
        result = &mut running => panic!("opening returned before cancellation: {:?}", result.map(|_| ())),
    }
    provider
        .append(AppendRequest {
            path: path.conversation()?,
            records: vec![b"settled answer".to_vec().into()],
            if_tail: Some(0),
            idempotency_key: crate::agent::intent_key("answer", &[])?,
        })
        .await?;
    let stale = InboxRecord::Cancel(Cancellation {
        by: Sender::User {
            principal: "owner".into(),
        },
        id: "stale".try_into()?,
        input: 2,
        conversation: 0,
    });
    crate::agent::submit(
        provider.as_ref(),
        crate::agent::send(
            &path,
            &Sender::User {
                principal: "owner".into(),
            },
            &stale,
        )?,
    )
    .await?;
    let mut records = provider
        .read(ReadRequest {
            key: None,
            path: path.inbox()?,
            from: 3,
            limit: 1,
            at_least: Default::default(),
        })
        .await?;
    let stale = records.try_next().await?.expect("stale control");
    assert!(
        !wire::accepts_interrupt(
            &handler,
            &wire::encode_interrupt(&delivery, Some(&checkpoint), INBOX, &stale)
        )
        .await?
    );
    let mut rejected = delivery.clone();
    rejected.trigger = Trigger::Record {
        subscription: INBOX.into(),
        sequence: stale.sequence,
    };
    rejected.record = Some(stale);
    let transition = handler
        .on_record(rejected, Some(&checkpoint), CancellationToken::new())
        .await?;
    let retained = crate::agent::checkpoint_head(&transition.checkpoint)?.state;
    assert_eq!(retained.queued.front().expect("retained input").inbox, 2);
    assert_eq!(retained.phase, crate::agent::Phase::Idle);
    assert!(!retained.taken.contains(&2));
    let cancel = InboxRecord::Cancel(Cancellation {
        by: Sender::User {
            principal: "owner".into(),
        },
        id: "cancel".try_into()?,
        input: 2,
        conversation: 1,
    });
    crate::agent::submit(
        provider.as_ref(),
        crate::agent::send(
            &path,
            &Sender::User {
                principal: "owner".into(),
            },
            &cancel,
        )?,
    )
    .await?;
    let mut records = provider
        .read(ReadRequest {
            key: None,
            path: path.inbox()?,
            from: 4,
            limit: 1,
            at_least: Default::default(),
        })
        .await?;
    let mut record = records.try_next().await?.expect("cancel control");
    let query = wire::encode_interrupt(&delivery, Some(&checkpoint), INBOX, &record);
    assert!(wire::accepts_interrupt(&handler, &query).await?);
    record.value = InboxRecord::Cancel(Cancellation {
        by: Sender::User {
            principal: "owner".into(),
        },
        id: "other".try_into()?,
        input: 3,
        conversation: 0,
    })
    .encode()?
    .into();
    assert!(
        !wire::accepts_interrupt(
            &handler,
            &wire::encode_interrupt(&delivery, Some(&checkpoint), INBOX, &record)
        )
        .await?
    );
    stop.cancel();
    let transition = running.await?;
    let state = crate::agent::checkpoint_head(&transition.checkpoint)?.state;
    assert!(state.turn.is_none());
    assert_eq!(state.phase, crate::agent::Phase::Idle);
    Ok(())
}
