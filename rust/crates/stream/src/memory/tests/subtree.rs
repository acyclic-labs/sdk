use super::super::*;
use futures::TryStreamExt as _;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn subtree_cut_and_private_suffix_match_flat_model(
        values in proptest::collection::vec(any::<u8>(), 1..32),
        exclude_items in any::<bool>(),
        root_cut in 0u64..=1,
    ) {
        let result: Result<(), StreamError> = futures::executor::block_on(async {
            let provider = MemoryStream::default();
            let root = StreamPath::new("tree")?;
            provider.append(AppendRequest { path: root.clone(), records: vec![Bytes::from_static(b"root").into()], if_tail: Some(0), idempotency_key: None }).await?;
            for (index, value) in values.iter().enumerate() {
                provider.append(AppendRequest { path: StreamPath::new(format!("tree/items/{index}"))?, records: vec![vec![*value].into()], if_tail: Some(0), idempotency_key: None }).await?;
            }
            provider.append(AppendRequest { path: StreamPath::new("tree/items-suffix/keep")?, records: vec![Bytes::from_static(b"keep").into()], if_tail: Some(0), idempotency_key: None }).await?;
            provider.commit(CommitRequest {
                conditions: vec![CommitCondition::Tail { path: root.clone(), expected: 1 },
                    CommitCondition::Absent { path: StreamPath::new("tree/children/new")? }],
                mutations: vec![CommitMutation::Fork { source: root.clone(), destination: StreamPath::new("tree/children/new")?, at_tail: root_cut, records: Vec::new(),
                    exclude: if exclude_items { vec![StreamPath::new("items")?] } else { Vec::new() } },
                    CommitMutation::Append { path: root, records: vec![Bytes::from_static(b"later").into()] }],
                idempotency_key: IdempotencyKey::new(b"fork".to_vec())?,
            }).await?;
            assert_eq!(provider.tail(StreamPath::new("tree/children/new")?).await?, root_cut);
            assert_eq!(provider.tail(StreamPath::new("tree")?).await?, 2);
            for (index, value) in values.iter().enumerate() {
                let target = StreamPath::new(format!("tree/children/new/items/{index}"))?;
                if exclude_items { assert_eq!(provider.tail(target).await, Err(StreamError::NotFound)); }
                else {
                    let records: Vec<_> = provider.read(ReadRequest { path: target.clone(), from: 0, limit: 2 }).await?.try_collect().await?;
                    assert_eq!(records[0].value.as_ref(), &[*value]);
                    provider.append(AppendRequest { path: target, records: vec![Bytes::from_static(b"private").into()], if_tail: Some(1), idempotency_key: None }).await?;
                }
                assert_eq!(provider.tail(StreamPath::new(format!("tree/items/{index}"))?).await?, 1);
            }
            assert_eq!(provider.tail(StreamPath::new("tree/children/new/items-suffix/keep")?).await?, 1);
            let state = provider.state.read().await;
            assert_eq!(state.paths.bytes(), state.paths.entries().iter().map(|(p,_)|p.as_str().len()).sum::<usize>());
            assert_eq!(state.paths.len(), state.paths.entries().len());
            Ok(())
        });
        prop_assert!(result.is_ok(), "{result:?}");
    }
}

#[tokio::test]
async fn fork_namespace_allocation_is_independent_of_descendant_count() -> Result<(), StreamError> {
    let mut costs = Vec::new();
    for count in [2, 1024] {
        let provider = MemoryStream::default();
        for index in 0..count {
            provider
                .append(AppendRequest {
                    path: StreamPath::new(format!("tree/{index}"))?,
                    records: vec![Bytes::from_static(b"body").into()],
                    if_tail: Some(0),
                    idempotency_key: None,
                })
                .await?;
        }
        let state = provider.state.read().await;
        let source = state.paths.node(&StreamPath::new("tree")?).unwrap().clone();
        let mut next = state.paths.clone();
        let count = std::cell::Cell::new(0);
        next.put(
            &StreamPath::new("copy")?,
            source.clone(),
            &mut NamespaceBuilder::new(&|_| {
                count.set(count.get() + 1);
                Ok(None)
            }),
        )?;
        assert!(Arc::ptr_eq(
            &source,
            next.node(&StreamPath::new("copy")?).unwrap()
        ));
        costs.push(count.get());
    }
    assert_eq!(costs[0], costs[1]);
    Ok(())
}

#[tokio::test]
async fn rejected_conditions_retain_no_key_or_namespace_state() -> Result<(), StreamError> {
    let provider = MemoryStream::default();
    let key = IdempotencyKey::new(b"stable-intent".to_vec())?;
    for _ in 0..128 {
        assert!(matches!(
            provider
                .commit(CommitRequest {
                    conditions: vec![CommitCondition::Tail {
                        path: StreamPath::new("absent")?,
                        expected: 1
                    }],
                    mutations: vec![CommitMutation::Append {
                        path: StreamPath::new("absent")?,
                        records: vec![Bytes::from_static(b"intent").into()]
                    }],
                    idempotency_key: key.clone(),
                })
                .await?,
            CommitOutcome::Conflict(_)
        ));
    }
    let state = provider.state.read().await;
    assert_eq!(state.replay_count, 0);
    assert!(state.retained.is_empty());
    assert_eq!(state.paths.len(), 0);
    assert_eq!(state.decision, 0);
    Ok(())
}

#[tokio::test]
async fn page_projection_charges_record_bytes_and_headers() -> Result<(), StreamError> {
    let provider = MemoryStream::default();
    // Each record fits; their projected page exceeds the shared command ceiling.
    let value = vec![0u8; crate::MAX_RECORD_BYTES / 2];
    let count = crate::MAX_COMMAND_BYTES / value.len() + 1;
    assert!(count < crate::MAX_ITEMS);
    for index in 0..count {
        provider
            .append(AppendRequest {
                path: StreamPath::new(format!("agents/{index:04}/status"))?,
                records: vec![value.clone().into()],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
    }
    let request = ChildrenPageRequest {
        parent: Some(StreamPath::new("agents")?),
        tail_stream: Some(StreamPath::new("status")?),
        after: None,
        hierarchy_version: None,
        limit: count as u32,
    };
    assert_eq!(
        provider.children_page(request).await,
        Err(StreamError::LimitExceeded)
    );
    Ok(())
}
