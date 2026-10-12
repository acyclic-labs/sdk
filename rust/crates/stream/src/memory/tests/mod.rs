use super::*;
use futures::TryStreamExt as _;
#[cfg(not(target_arch = "wasm32"))]
mod native_controls;
use std::sync::atomic::{AtomicU64, Ordering};

#[tokio::test]
async fn committed_record_times_never_regress_with_the_clock() -> Result<(), StreamError> {
    let clock = Arc::new(AtomicU64::default());
    let stream = MemoryStream::new_with_clock(MemoryLimits::default(), clock.clone());
    let path = StreamPath::new("accounting/authority")?;
    for (time, body) in [(100, b"first".as_slice()), (105, b"recovered")] {
        clock.store(time, Ordering::SeqCst);
        if time == 105 {
            clock.store(90, Ordering::SeqCst);
            let outcome = stream
                .commit(CommitRequest {
                    conditions: vec![CommitCondition::Tail {
                        path: path.clone(),
                        expected: 1,
                    }],
                    mutations: vec![CommitMutation::Append {
                        path: path.clone(),
                        records: vec![OwnedBytes::from_static(b"rollback")],
                    }],
                    idempotency_key: key(b"rollback-commit")?,
                })
                .await?;
            assert!(matches!(outcome, CommitOutcome::Committed(_)));
            clock.store(time, Ordering::SeqCst);
        }
        stream
            .append(AppendRequest {
                path: path.clone(),
                records: vec![OwnedBytes::copy_from_slice(body)],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
    }
    let records = stream
        .read(ReadRequest {
            path,
            from: 0,
            limit: 3,
        })
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        records
            .iter()
            .map(|record| record.committed_at_micros)
            .collect::<Vec<_>>(),
        vec![100_000, 100_000, 105_000]
    );
    Ok(())
}

fn path(value: &str) -> Result<StreamPath, StreamError> {
    StreamPath::new(value)
}

fn key(value: &'static [u8]) -> Result<crate::IdempotencyKey, StreamError> {
    crate::IdempotencyKey::new(OwnedBytes::from_static(value))
}

#[tokio::test]
async fn sibling_names_before_the_slash_do_not_hide_children() -> Result<(), StreamError> {
    let provider = MemoryStream::default();
    for name in ["a-/other", "a/first", "a/second", "a0/other"] {
        provider
            .append(AppendRequest {
                path: path(name)?,
                records: vec![OwnedBytes::from_static(b"record")],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
    }
    let mut request = ChildrenPageRequest {
        parent: Some(path("a")?),
        after: None,
        hierarchy_version: None,
        limit: 1,
    };
    let first = provider.children_page(request.clone()).await?;
    assert_eq!(
        first.children,
        vec![Child {
            path: path("a/first")?
        }]
    );
    request.after = first.next_after;
    request.hierarchy_version = Some(first.hierarchy_version);
    let second = provider.children_page(request).await?;
    assert_eq!(
        second.children,
        vec![Child {
            path: path("a/second")?
        }]
    );
    assert!(second.next_after.is_none());
    let children = provider
        .children(ChildrenRequest {
            parent: Some(path("a")?),
            limit: 2,
        })
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        children,
        vec![
            Child {
                path: path("a/first")?
            },
            Child {
                path: path("a/second")?
            }
        ]
    );
    Ok(())
}

#[tokio::test]
async fn paginates_more_children_than_one_wire_page_without_gaps() -> Result<(), StreamError> {
    let provider = MemoryStream::default();
    for index in 0..2_049 {
        provider
            .append(AppendRequest {
                path: path(&format!("agents/agent-{index:04}"))?,
                records: vec![OwnedBytes::from_static(b"bound")],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
    }
    let mut after = None;
    let mut revision = None;
    let mut found = Vec::new();
    loop {
        let page = provider
            .children_page(ChildrenPageRequest {
                parent: Some(path("agents")?),
                after,
                hierarchy_version: revision,
                limit: 127,
            })
            .await?;
        revision = Some(page.hierarchy_version);
        found.extend(page.children.into_iter().map(|child| child.path));
        after = page.next_after;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(found.len(), 2_049);
    assert!(
        found
            .windows(2)
            .all(|pair| matches!(pair, [left, right] if left < right))
    );
    Ok(())
}

#[tokio::test]
async fn deep_fork_history_is_dropped_without_recursive_stack_growth() -> Result<(), StreamError> {
    let provider = MemoryStream::default();
    let mut source = path("deep/root")?;
    provider
        .append(AppendRequest {
            path: source.clone(),
            records: vec![OwnedBytes::from_static(b"root")],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    for depth in 0..2_048 {
        let destination = path(&format!("deep/fork-{depth}"))?;
        provider
            .fork(ForkRequest {
                source,
                destination: destination.clone(),
                at_tail: None,
                idempotency_key: None,
            })
            .await?;
        source = destination;
    }
    drop(provider);
    Ok(())
}

#[tokio::test]
async fn provider_deadline_is_exclusive_and_replay_precedes_time() -> Result<(), StreamError> {
    let clock = Arc::new(AtomicU64::default());
    clock.store(100, Ordering::SeqCst);
    let provider = MemoryStream::new_with_clock(MemoryLimits::default(), clock.clone());
    let request = CommitRequest {
        conditions: vec![CommitCondition::Absent {
            path: path("deadline/accepted")?,
        }],
        mutations: vec![CommitMutation::Append {
            path: path("deadline/accepted")?,
            records: vec![OwnedBytes::from_static(b"accepted")],
        }],
        idempotency_key: key(b"deadline-accepted")?,
    };
    assert_eq!(
        provider.commit_before(request.clone(), 100).await,
        Err(StreamError::DeadlineElapsed)
    );
    assert_eq!(
        provider.commit_before(request.clone(), 99).await,
        Err(StreamError::DeadlineElapsed)
    );
    let committed = provider.commit_before(request.clone(), 101).await?;
    assert!(matches!(committed, CommitOutcome::Committed(_)));
    clock.store(1_000, Ordering::SeqCst);
    assert_eq!(provider.commit_before(request, 101).await?, committed);

    let retryable = CommitRequest {
        conditions: vec![CommitCondition::Absent {
            path: path("deadline/retry")?,
        }],
        mutations: vec![CommitMutation::Append {
            path: path("deadline/retry")?,
            records: vec![OwnedBytes::from_static(b"retry")],
        }],
        idempotency_key: key(b"deadline-retry")?,
    };
    assert_eq!(
        provider.commit_before(retryable.clone(), 1_000).await,
        Err(StreamError::DeadlineElapsed)
    );
    assert!(matches!(
        provider.commit_before(retryable, 1_001).await?,
        CommitOutcome::Committed(_)
    ));
    Ok(())
}

#[tokio::test]
async fn single_record_reads_seek_deep_history_and_prefixes() -> Result<(), StreamError> {
    let provider = MemoryStream::default();
    let source = path("seek/source")?;
    for index in 0..256_u64 {
        provider
            .append(AppendRequest {
                path: source.clone(),
                records: vec![OwnedBytes::from(index.to_string())],
                if_tail: Some(index),
                idempotency_key: None,
            })
            .await?;
    }
    let destination = path("seek/prefix")?;
    provider
        .fork(ForkRequest {
            source: source.clone(),
            destination: destination.clone(),
            at_tail: Some(128),
            idempotency_key: None,
        })
        .await?;

    for (path, expected_tail, expected) in [
        (&source, 256_u64, b"255".as_slice()),
        (&destination, 128_u64, b"127".as_slice()),
    ] {
        let record = provider
            .read(ReadRequest {
                path: path.clone(),
                from: expected_tail - 1,
                limit: 1,
            })
            .await?
            .next()
            .await
            .ok_or(StreamError::Unavailable)??;
        assert_eq!(record.sequence, expected_tail - 1);
        assert_eq!(record.value.as_ref(), expected);
    }
    Ok(())
}

#[tokio::test]
#[allow(
    clippy::indexing_slicing,
    reason = "index is preceded by an assert_eq!(forked.len(), 1), proving it in-bounds"
)]
async fn coordinated_conflict_changes_nothing_and_success_has_one_envelope()
-> Result<(), StreamError> {
    let provider = MemoryStream::default();
    let source = path("jobs")?;
    provider
        .append(AppendRequest {
            path: source.clone(),
            records: vec![OwnedBytes::from_static(b"ready")],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let request = CommitRequest {
        conditions: vec![CommitCondition::Absent {
            path: path("runs/a")?,
        }],
        mutations: vec![CommitMutation::Fork {
            source: source.clone(),
            destination: path("runs/a")?,
            at_tail: 1,
            records: Vec::new(),
        }],
        idempotency_key: key(b"commit")?,
    };
    provider
        .append(AppendRequest {
            path: source,
            records: vec![OwnedBytes::from_static(b"source-progress")],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await?;
    let outcome = provider.commit(request.clone()).await;
    let Ok(CommitOutcome::Committed(envelope)) = &outcome else {
        return Err(StreamError::Unavailable);
    };
    assert_eq!(provider.commit(request).await, outcome);
    assert_eq!(
        provider.read_commit(envelope.commit_id).await.as_ref(),
        Ok(envelope)
    );
    let forked = provider
        .read(ReadRequest {
            path: path("runs/a")?,
            from: 0,
            limit: 10,
        })
        .await?
        .collect::<Vec<_>>()
        .await;
    assert_eq!(forked.len(), 1);
    assert_eq!(
        forked[0].as_ref().map(|record| record.value.as_ref()),
        Ok(b"ready".as_slice())
    );
    Ok(())
}

#[tokio::test]
async fn exact_replay_precedes_capacity_and_changed_arguments_never_reclassify()
-> Result<(), StreamError> {
    let provider = MemoryStream::new(MemoryLimits {
        paths: 2,
        path_bytes: 4,
        records: 1,
        payload_bytes: 3,
        commits: 1,
        idempotency_results: 1,
    });
    let request = AppendRequest {
        path: path("a/b")?,
        records: vec![OwnedBytes::from_static(b"one")],
        if_tail: Some(0),
        idempotency_key: Some(key(b"only-key")?),
    };
    let committed = provider.append(request.clone()).await?;
    assert_eq!(
        provider.append(request.clone()).await,
        Ok(committed.clone())
    );
    let observation = provider
        .inspect_idempotency(key(b"only-key")?)
        .await?
        .ok_or(StreamError::Unavailable)?;
    assert_ne!(observation.request_digest, [0; 32]);
    assert_eq!(observation.outcome, IdempotencyOutcome::Append(committed));
    assert_eq!(
        provider
            .append(AppendRequest {
                records: vec![OwnedBytes::from_static(b"two")],
                ..request
            })
            .await,
        Err(StreamError::IdempotencyMismatch)
    );
    Ok(())
}

#[tokio::test]
async fn every_fork_cut_reads_the_exact_prefix_and_never_aliases_suffixes()
-> Result<(), StreamError> {
    let provider = MemoryStream::default();
    let source = path("lineage/source")?;
    let bodies = (0_u8..32)
        .map(|value| OwnedBytes::from(vec![value]))
        .collect::<Vec<_>>();
    provider
        .append(AppendRequest {
            path: source.clone(),
            records: bodies,
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    for cut in 0_u64..=32 {
        let destination = path(&format!("lineage/forks/{cut}"))?;
        provider
            .fork(ForkRequest {
                source: source.clone(),
                destination: destination.clone(),
                at_tail: Some(cut),
                idempotency_key: None,
            })
            .await?;
        let records = provider
            .read(ReadRequest {
                path: destination.clone(),
                from: 0,
                limit: 64,
            })
            .await?
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            u64::try_from(records.len()).map_err(|_| StreamError::LimitExceeded)?,
            cut
        );
        provider
            .append(AppendRequest {
                path: destination.clone(),
                records: vec![OwnedBytes::from_static(b"own"); 2],
                if_tail: Some(cut),
                idempotency_key: None,
            })
            .await?;
        let from = cut.saturating_sub(3);
        let window = provider
            .read(ReadRequest {
                path: destination,
                from,
                limit: 4,
            })
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let expected = (from..cut + 2)
            .take(4)
            .map(|sequence| {
                if sequence < cut {
                    OwnedBytes::from(vec![u8::try_from(sequence).unwrap_or_default()])
                } else {
                    OwnedBytes::from_static(b"own")
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            window
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            (from..from + expected.len() as u64).collect::<Vec<_>>()
        );
        assert_eq!(
            window
                .into_iter()
                .map(|record| record.value)
                .collect::<Vec<_>>(),
            expected
        );
    }
    assert_eq!(provider.tail(source).await?, 32);
    Ok(())
}

#[test]
fn permanent_path_validation_rejects_ambiguous_names() {
    for invalid in [
        "", "/a", "a/", "a//b", ".", "..", "a/./b", "a/../b", "a\n", "a b", "a\\b",
    ] {
        assert_eq!(StreamPath::new(invalid), Err(StreamError::InvalidPath));
    }
    assert!(StreamPath::new("a".repeat(crate::MAX_PATH_BYTES)).is_ok());
    assert_eq!(
        StreamPath::new("a".repeat(crate::MAX_PATH_BYTES + 1)),
        Err(StreamError::InvalidPath)
    );
    assert!(StreamPath::new("runs/run_42/agents/researcher").is_ok());
}

#[tokio::test]
async fn results_and_envelopes_never_expire_or_execute_again() -> Result<(), StreamError> {
    let clock = Arc::new(AtomicU64::default());
    let limits = MemoryLimits {
        idempotency_results: 1,
        commits: 1,
        ..MemoryLimits::default()
    };
    let provider = MemoryStream::new_with_clock(limits, clock.clone());
    let append = |name: &'static [u8]| AppendRequest {
        path: StreamPath::new("retained").unwrap_or_else(|_| unreachable!()),
        records: vec![OwnedBytes::from_static(name)],
        if_tail: None,
        idempotency_key: Some(
            crate::IdempotencyKey::new(OwnedBytes::from_static(name))
                .unwrap_or_else(|_| unreachable!()),
        ),
    };
    let AppendOutcome::Committed(first) = provider.append(append(b"first")).await? else {
        return Err(StreamError::Unavailable);
    };
    clock.store(1, Ordering::SeqCst);
    assert_eq!(
        provider.append(append(b"second")).await,
        Err(StreamError::Capacity),
        "the first result is still retained"
    );
    assert!(
        provider
            .inspect_idempotency(key(b"first")?)
            .await?
            .is_some()
    );
    assert!(provider.read_commit(first.commit_id).await.is_ok());

    for now in [0, 1, 24 * 60 * 60 * 1_000, u64::MAX] {
        clock.store(now, Ordering::SeqCst);
        assert_eq!(
            provider.append(append(b"second")).await,
            Err(StreamError::Capacity)
        );
        assert_eq!(
            provider.append(append(b"first")).await?,
            AppendOutcome::Committed(first.clone())
        );
        assert!(
            provider
                .inspect_idempotency(key(b"first")?)
                .await?
                .is_some()
        );
        assert!(provider.read_commit(first.commit_id).await.is_ok());
        assert_eq!(provider.tail(path("retained")?).await?, 1);
        let mut changed = append(b"first");
        changed.path = path("another/path")?;
        assert_eq!(
            provider.append(changed).await,
            Err(StreamError::IdempotencyMismatch)
        );
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig {
        rng_seed: proptest::test_runner::RngSeed::Fixed(0x5eed),
        failure_persistence: None,
        ..proptest::prelude::ProptestConfig::with_cases(64)
    })]

    #[test]
    fn pages_neither_skip_nor_repeat(
        names in proptest::collection::btree_set("[a-c]{1,2}(/[a-c])?", 1..12),
        records in 1..40_usize,
        limit in 1..6_u32,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let provider = MemoryStream::default();
        runtime.block_on(async {
            for name in &names {
                provider
                    .append(AppendRequest {
                        path: path(&format!("root/{name}"))?,
                        records: vec![OwnedBytes::from_static(b"x")],
                        if_tail: None,
                        idempotency_key: None,
                    })
                    .await?;
            }
            let log = path("log")?;
            let values: Vec<_> = (0..records).map(|value| OwnedBytes::from(value.to_string())).collect();
            for chunk in values.chunks(7) {
                provider
                    .append(AppendRequest {
                        path: log.clone(),
                        records: chunk.to_vec(),
                        if_tail: None,
                        idempotency_key: None,
                    })
                    .await?;
            }

            let mut read = Vec::new();
            loop {
                let page: Vec<_> = provider
                    .read(ReadRequest {
                        path: log.clone(),
                        from: read.len() as u64,
                        limit,
                    })
                    .await?
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<_, _>>()?;
                if page.is_empty() {
                    break;
                }
                proptest::prop_assert!(page.len() <= limit as usize);
                read.extend(page);
            }
            proptest::prop_assert!(read.iter().enumerate().all(|(index, record)| record.sequence == index as u64));
            proptest::prop_assert_eq!(read.into_iter().map(|record| record.value).collect::<Vec<_>>(), values);

            // Only first segments are direct children; deeper paths imply them.
            let expected: std::collections::BTreeSet<_> = names
                .iter()
                .map(|name| name.split('/').next().unwrap_or_default().to_owned())
                .collect();
            let (mut found, mut after, mut version) = (Vec::new(), None, None);
            loop {
                let page = provider
                    .children_page(ChildrenPageRequest {
                        parent: Some(path("root")?),
                        after,
                        hierarchy_version: version,
                        limit,
                    })
                    .await?;
                proptest::prop_assert!(page.children.len() <= limit as usize);
                version = Some(page.hierarchy_version);
                found.extend(page.children.into_iter().map(|child| child.path.to_string()));
                after = page.next_after;
                if after.is_none() {
                    break;
                }
            }
            let expected: Vec<_> = expected.into_iter().map(|name| format!("root/{name}")).collect();
            proptest::prop_assert_eq!(found, expected);
            Ok(())
        })?;
    }
}
