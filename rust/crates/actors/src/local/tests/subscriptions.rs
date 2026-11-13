use super::*;

#[tokio::test]
async fn subscription_controls_recover_original_intent_after_reopen_and_removal() -> TestResult {
    use acyclic_stream::{AppendOutcome, AppendRequest};
    let root = tempfile::tempdir()?;
    let provider = Arc::new(LocalStream::open(root.path(), Default::default()).await?);
    let store = Store::new(provider.clone(), state_root()?);
    let address = actor("subscriptions")?;
    place(&store, &address).await?;
    let snapshot = store.load(&address).await?;
    store
        .publish(
            &snapshot,
            1,
            &record(0),
            None,
            transition(
                7,
                AlarmChange::Set {
                    deadline_unix_ms: 42,
                },
            ),
            key("checkpoint")?,
        )
        .await?;
    let snapshot = store.load(&address).await?;
    store.set_paused(&snapshot, 1, true, key("pause")?).await?;
    let before = store.load(&address).await?;
    let input = StreamPath::new("inputs/subscriptions")?;
    let append = |identity: &str| -> Result<_, StreamError> {
        Ok(AppendRequest {
            path: input.clone(),
            records: vec![b"event".to_vec().into()],
            if_tail: None,
            idempotency_key: key(identity)?,
        })
    };
    assert!(matches!(
        provider.append(append("before")?).await?,
        AppendOutcome::Committed(_)
    ));
    let add = crate::wire::AddSubscriptionRequest {
        namespace: Some(address.namespace().as_str().into()),
        account_id: address.account().as_bytes().to_vec(),
        actor_id: address.id().into(),
        subscription: Some(crate::wire::SubscriptionSpec {
            subscription_id: "events".into(),
            stream_path: input.as_str().into(),
            start: Some(crate::wire::SubscriptionStart {
                start: Some(crate::wire::subscription_start::Start::CurrentHead(true)),
            }),
            placement_anchor: false,
        }),
        idempotency_key: "add".into(),
    };
    let schedule = store.scheduling_path(store.scheduling_shard(&address))?;
    let activations = provider.tail(schedule.clone()).await?;
    store.add_subscription(&[], &add).await?;
    let accepted = store.load(&address).await?;
    let mut expected = before.state().ok_or("missing original actor")?.clone();
    expected.subscriptions.insert(
        "events".into(),
        Subscription {
            path: input.clone(),
            cursor: 1,
        },
    );
    assert_eq!(accepted.state(), Some(&expected));
    assert_eq!(provider.tail(schedule.clone()).await?, activations + 1);
    assert!(matches!(
        store.set_paused(&before, 1, false, key("stale")?).await,
        Err(Error::Conflict)
    ));
    provider.append(append("after")?).await?;
    drop(store);
    drop(provider);

    let provider = Arc::new(LocalStream::open(root.path(), Default::default()).await?);
    let store = Store::new(provider.clone(), state_root()?);
    store.add_subscription(&[], &add).await?;
    assert_eq!(store.load(&address).await?.tail, accepted.tail);
    let mut changed = add.clone();
    changed
        .subscription
        .as_mut()
        .ok_or("missing subscription")?
        .subscription_id = "other".into();
    assert!(matches!(
        store.add_subscription(&[], &changed).await,
        Err(Error::Stream(StreamError::IdempotencyMismatch))
    ));
    let remove = crate::wire::RemoveSubscriptionRequest {
        namespace: add.namespace.clone(),
        account_id: add.account_id.clone(),
        actor_id: add.actor_id.clone(),
        subscription_id: "events".into(),
        idempotency_key: "r".repeat(256),
    };
    let mut invalid = remove.clone();
    invalid.subscription_id.clear();
    let actor_tail = store.load(&address).await?.tail;
    let activation_tail = provider.tail(schedule.clone()).await?;
    let mut empty_key = remove.clone();
    empty_key.idempotency_key.clear();
    let mut oversized_key = remove.clone();
    oversized_key.idempotency_key = "r".repeat(257);
    let mut oversized_utf8_key = remove.clone();
    oversized_utf8_key.idempotency_key = "é".repeat(129);
    for rejected in [invalid, empty_key, oversized_key, oversized_utf8_key] {
        assert!(matches!(
            store.remove_subscription(&[], &rejected).await,
            Err(Error::Invalid("invalid remove subscription request"))
        ));
    }
    assert_eq!(store.load(&address).await?.tail, actor_tail);
    assert_eq!(provider.tail(schedule).await?, activation_tail);
    // The rejected request must not consume the valid request's retry identity.
    store.remove_subscription(&[], &remove).await?;
    let removed = store.load(&address).await?;
    assert_eq!(removed.state(), before.state());
    store.add_subscription(&[], &add).await?;
    store.remove_subscription(&[], &remove).await?;
    assert_eq!(store.load(&address).await?.tail, removed.tail);
    let mut readd = add;
    readd.idempotency_key = "readd".into();
    store.add_subscription(&[], &readd).await?;
    assert_eq!(
        store
            .load(&address)
            .await?
            .state()
            .ok_or("missing readded actor")?
            .cursor("events"),
        Some(2)
    );
    // A settled remove must not remove a later binding of the same id.
    let rebound = store.load(&address).await?;
    store.remove_subscription(&[], &remove).await?;
    assert_eq!(store.load(&address).await?.tail, rebound.tail);
    Ok(())
}
