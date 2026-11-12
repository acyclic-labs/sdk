use super::*;

/// Seeing the same namespace and durable input never authorizes a second host
/// to claim. Only explicit, per-actor placement commands change the fence.
#[tokio::test]
async fn placement_commands_fence_same_namespace_nodes_without_self_claim() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let first = spawn_with(&store, "placed-first", &[b"emit"]).await?;
    let second = spawn_with(&store, "placed-second", &[b"emit"]).await?;
    let owner = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    let other = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    other.track(first.clone())?;
    assert!(matches!(
        other.drain_actor(&first).await,
        Err(Error::Conflict)
    ));
    assert_eq!(store.load(&first).await?.state().map(State::epoch), Some(0));

    owner.place(first.clone(), 1).await?;
    other.place(second.clone(), 1).await?;
    let cut = store.load(&first).await?;
    let forged = Transition {
        response: None,
        checkpoint: ledger("out/placed-first")?,
        alarm: AlarmChange::Keep,
        effects: vec![CommitEffect::hard(
            Vec::new(),
            vec![CommitMutation::Append {
                path: StreamPath::new(format!("{}/placement", cut.path()))?,
                records: vec![Bytes::from_static(b"forged placement").into()],
            }],
        )],
        participant: None,
        subscriptions: Vec::new(),
    };
    assert!(matches!(
        store.prepare_publish(
            &cut,
            1,
            &Trigger::Record {
                subscription: "inbox".into(),
                sequence: 0,
            },
            None,
            forged,
            key("forged-placement")?,
        ),
        Err(Error::Invalid("effect changes the host's control records"))
    ));
    assert!(matches!(
        other.place(first.clone(), 1).await,
        Err(Error::Conflict)
    ));
    owner.drain_actor(&first).await?;
    other.drain_actor(&second).await?;
    assert!(owner.release(&first));
    owner.track(first.clone())?;
    owner.drain_actor(&first).await?;
    owner.place(first.clone(), 1).await?;
    assert_eq!(store.load(&first).await?.state().map(State::epoch), Some(1));
    assert_eq!(
        store
            .load(&first)
            .await?
            .state()
            .and_then(|s| s.cursor("inbox")),
        Some(1)
    );

    other.place(first.clone(), 2).await?;
    assert_eq!(store.load(&first).await?.state().map(State::epoch), Some(2));
    assert!(matches!(
        owner.place(first.clone(), 1).await,
        Err(Error::Conflict)
    ));
    owner.track(first.clone())?;
    assert!(matches!(
        owner.drain_actor(&first).await,
        Err(Error::Conflict)
    ));
    other.drain_actor(&first).await?;
    assert_eq!(
        records(provider.as_ref(), "out/placed-first").await?.len(),
        1
    );
    assert_eq!(
        records(provider.as_ref(), "out/placed-second").await?.len(),
        1
    );

    let child = actor("placed-child")?;
    let parent = store.load(&first).await?;
    let mut fork = PreparedPublish::foreign(key("placement/fork")?);
    fork.join(store.create(
        Some(&parent),
        &child,
        crate::local::Creation {
            subscriptions: BTreeMap::new(),
            checkpoint: Some(ledger("out/placed-child")?),
            escrow: None,
        },
    )?)?;
    store.submit(fork).await?;
    // Forking data never copies the parent's placement authority or command
    // index. The new actor receives its own first explicit command.
    owner.place(child.clone(), 1).await?;
    assert_eq!(
        provider
            .tail(StreamPath::new(format!(
                "{}/placement",
                store.path(&child)?
            ))?)
            .await?,
        1
    );
    Ok(())
}
