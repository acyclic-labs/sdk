use super::*;
use crate::{CommitCondition, CommitEffect, CommitMutation};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn captured_head_ownership_survives_lease_release() -> Result {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let durable = StreamPath::new("durable")?;
    let temporary = StreamPath::new("scratch/temporary")?;
    provider.append(append(&durable, 0)?).await?;
    let lease = provider
        .lease_ephemeral(StreamPath::new("scratch")?)
        .await?;
    provider.append(append(&temporary, 0)?).await?;
    provider.durability_barrier().await?;
    let request = crate::HeadsRequest {
        at: None,
        retained: Default::default(),
        durable_through: Some(1),
        projection: crate::HeadProjection::Watermarks,
        paths: [temporary.clone(), durable.clone()]
            .into_iter()
            .collect(),
        children: None,
        limit: 2,
        at_least: Default::default(),
    };
    let (captured, ownership) = provider
        .inner
        .provider
        .capture_heads_with_ephemeral(&request, crate::memory::HeadDurability::Deferred)
        .await?;
    // Force the disputed ordering: publication is captured before retirement,
    // and durability confirmation runs after it. No scheduler timing is needed.
    lease.settle().await?;
    assert_eq!(ownership.as_slice(), [true, false]);
    let temporary = captured.heads[0].as_ref().ok_or("temporary head missing")?;
    assert_eq!(temporary.tail, 1);
    assert_eq!(temporary.durable, 0);
    assert_eq!(temporary.retained_through, None);
    // Negative control: re-reading ownership after retirement loses precisely
    // the classification that the local adapter must retain for its response.
    assert_eq!(
        provider.inner.provider.ephemeral_mask(&request.paths).await,
        [false, false]
    );
    let confirmed = provider.heads(request).await?;
    assert!(confirmed.heads[0].is_none());
    let durable = confirmed.heads[1].as_ref().ok_or("durable control missing")?;
    assert_eq!(durable.durable, 1);
    assert_eq!(durable.retained_through, Some(1));
    Ok(())
}

/// One conditioned commit forking `source`, at tail 1, to `destination`: the
/// shape of a filesystem fork.
fn fork_into(
    source: &StreamPath,
    destination: &StreamPath,
    key: &'static [u8],
) -> std::result::Result<CommitRequest, StreamError> {
    Ok(CommitRequest {
        deadline_unix_millis: None,
        effects: vec![CommitEffect::hard(
            vec![
                CommitCondition::Tail {
                    path: source.clone(),
                    expected: 1,
                },
                CommitCondition::Absent {
                    path: destination.clone(),
                },
            ],
            vec![CommitMutation::Fork {
                exclude: Vec::new(),
                source: source.clone(),
                destination: destination.clone(),
                at_tail: 1,
                records: Vec::new(),
            }],
        )],
        idempotency_key: IdempotencyKey::new(Bytes::from_static(key))?,
    })
}

/// `request` also creating `path` with one record: an adopted path.
fn adopting(mut request: CommitRequest, path: &StreamPath) -> Result<CommitRequest> {
    request
        .effects
        .first_mut()
        .ok_or("no effect")?
        .mutations
        .push(CommitMutation::Append {
            path: path.clone(),
            records: vec![Bytes::from_static(b"adopted").into()],
        });
    Ok(request)
}

fn append(path: &StreamPath, tail: u64) -> std::result::Result<AppendRequest, StreamError> {
    Ok(AppendRequest {
        path: path.clone(),
        records: vec![Bytes::copy_from_slice(&tail.to_le_bytes()).into()],
        if_tail: Some(tail),
        idempotency_key: IdempotencyKey::unique(),
    })
}

fn frames(provider: &LocalStream) -> Result<(u64, u64)> {
    let journal = provider
        .inner
        .journal
        .lock()
        .map_err(|_| "journal poisoned")?;
    Ok((journal.operations, journal.bytes))
}

/// An ephemeral fork, the path it adopts and their appends write no journal
/// frame. A compaction while the lease is held snapshots none of them, and a
/// reopen without any release finds nothing, while a durable fork and the
/// adopted path's durable sibling survive both.
#[tokio::test]
async fn an_unreleased_ephemeral_fork_never_outlives_its_incarnation() -> Result {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits {
        compaction_operations: 8,
        ..LocalStreamLimits::default()
    };
    let provider = LocalStream::open(directory.path(), limits).await?;
    let source = StreamPath::new("agents/a")?;
    let root = StreamPath::new("scratch")?;
    let ephemeral = StreamPath::new("scratch/a")?;
    let durable = StreamPath::new("kept/a")?;
    let sibling = StreamPath::new("changes/base")?;
    let adopted = StreamPath::new("changes/scratch")?;
    provider.append(append(&source, 0)?).await?;
    provider.append(append(&sibling, 0)?).await?;
    let lease = provider.lease_ephemeral(root.clone()).await?;
    let before = frames(&provider)?;
    let CommitOutcome::Committed(_) = provider
        .commit(adopting(
            fork_into(&source, &ephemeral, b"ephemeral-fork")?,
            &adopted,
        )?)
        .await?
    else {
        return Err("ephemeral fork conflicted".into());
    };
    provider.append(append(&ephemeral, 1)?).await?;
    provider.append(append(&adopted, 1)?).await?;
    assert_eq!(frames(&provider)?, before, "no ephemeral frame");
    assert_eq!(provider.tail(ephemeral.clone()).await?, 2);
    assert_eq!(provider.tail(adopted.clone()).await?, 2);
    // Negative control: the same fork to a durable destination is journalled.
    provider
        .commit(fork_into(&source, &durable, b"durable-fork")?)
        .await?;
    assert_eq!(frames(&provider)?.0, before.0 + 1, "one durable frame");
    // Compact while the lease is held.
    for tail in 1..limits.compaction_operations {
        provider.append(append(&source, tail)?).await?;
    }
    assert_eq!(provider.tail(ephemeral.clone()).await?, 2);
    drop(provider);
    let snapshot = read_snapshot(directory.path(), limits)?.ok_or("snapshot missing")?;
    let restored = MemoryStream::default();
    restored.install_state(&snapshot.state).await?;
    for gone in [&root, &adopted] {
        assert_eq!(
            restored.tail(gone.clone()).await,
            Err(StreamError::NotFound)
        );
    }
    assert_eq!(restored.tail(durable.clone()).await?, 1);
    assert_eq!(restored.tail(sibling.clone()).await?, 1);

    let reopened = LocalStream::open(directory.path(), limits).await?;
    for gone in [&root, &ephemeral, &adopted] {
        assert_eq!(
            reopened.tail(gone.clone()).await,
            Err(StreamError::NotFound)
        );
    }
    assert_eq!(reopened.tail(durable.clone()).await?, 1);
    assert_eq!(reopened.tail(sibling).await?, 1);
    let page = reopened
        .children_page(crate::ChildrenPageRequest {
            tail_stream: None,
            parent: None,
            after: None,
            offset: 0,
            hierarchy_version: None,
            limit: 16,
        })
        .await?;
    assert_eq!(
        page.children
            .iter()
            .map(|child| child.path.as_str())
            .collect::<Vec<_>>(),
        ["agents", "changes", "kept"]
    );
    // Releasing into the closed incarnation touches nothing reopened, and the
    // root is free to lease again.
    lease.settle().await?;
    assert_eq!(reopened.tail(durable).await?, 1);
    reopened.lease_ephemeral(root).await?.settle().await?;
    Ok(())
}

/// Dropping a lease removes its root subtree and every adopted path at once:
/// followers waiting in either are woken with `NotFound`, and nothing else
/// moves.
#[tokio::test]
async fn dropping_a_lease_removes_its_subtree_and_wakes_its_followers() -> Result {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let source = StreamPath::new("agents/a")?;
    let ephemeral = StreamPath::new("scratch/agents/a")?;
    let sibling = StreamPath::new("changes/base")?;
    let adopted = StreamPath::new("changes/scratch")?;
    provider.append(append(&source, 0)?).await?;
    provider.append(append(&sibling, 0)?).await?;
    let lease = provider
        .lease_ephemeral(StreamPath::new("scratch")?)
        .await?;
    provider
        .commit(adopting(
            fork_into(&source, &ephemeral, b"ephemeral-fork")?,
            &adopted,
        )?)
        .await?;
    let mut follows = Vec::new();
    for path in [&ephemeral, &adopted] {
        follows.push(crate::records_only(
            provider
                .follow(crate::FollowRequest::new(path.clone(), 1))
                .await?,
        ));
    }
    for follow in &mut follows {
        assert!(
            futures::poll!(follow.next()).is_pending(),
            "waits at the tail"
        );
    }
    drop(lease);
    for follow in &mut follows {
        assert!(matches!(
            follow.next().await,
            Some(Err(StreamError::NotFound))
        ));
    }
    for gone in [&ephemeral, &adopted] {
        assert_eq!(
            provider.tail(gone.clone()).await,
            Err(StreamError::NotFound)
        );
    }
    assert_eq!(provider.tail(sibling).await?, 1);
    assert_eq!(
        provider.tail(StreamPath::new("scratch")?).await,
        Err(StreamError::NotFound)
    );
    assert_eq!(provider.tail(source).await?, 1);
    Ok(())
}

/// Skipped mutations cannot adopt a durable sibling or suppress its journal.
/// Retrying and replaying retain the original selection, even after the lease
/// condition changes; genuinely selected lease writes still remain ephemeral.
#[tokio::test]
async fn skipped_effects_do_not_change_the_selected_publication_scope() -> Result {
    for soft_first in [false, true] {
        for release_first in [false, true] {
            let directory = tempfile::tempdir()?;
            let limits = LocalStreamLimits::default();
            let provider = LocalStream::open(directory.path(), limits).await?;
            let base = StreamPath::new("kept/base")?;
            let kept = StreamPath::new("kept/selected")?;
            let unsafe_path = StreamPath::new("kept/unsafe")?;
            let root = StreamPath::new("scratch")?;
            let omitted = StreamPath::new("scratch/omitted")?;
            provider.append(append(&base, 0)?).await?;
            let mut lease = Some(provider.lease_ephemeral(root.clone()).await?);
            let before = frames(&provider)?;
            let soft = CommitEffect {
                conditions: vec![CommitCondition::Tail {
                    path: root.clone(),
                    expected: 1,
                }],
                mutations: vec![CommitMutation::Append {
                    path: omitted.clone(),
                    records: vec![Bytes::from_static(b"omitted").into()],
                }],
                soft: true,
            };
            let hard = CommitEffect::hard(
                vec![CommitCondition::Absent { path: kept.clone() }],
                vec![CommitMutation::Append {
                    path: kept.clone(),
                    records: vec![Bytes::from_static(b"durable").into()],
                }],
            );
            let request = CommitRequest {
                deadline_unix_millis: None,
                effects: if soft_first {
                    vec![soft, hard]
                } else {
                    vec![hard, soft]
                },
                idempotency_key: IdempotencyKey::unique(),
            };
            let CommitOutcome::Committed(receipt) = provider.commit(request.clone()).await? else {
                return Err("selected durable effect was refused".into());
            };
            assert_eq!(receipt.skipped, [if soft_first { 0 } else { 1 }]);
            assert_eq!(
                frames(&provider)?.0,
                before.0 + 1,
                "selected durable write must be journalled"
            );
            assert_eq!(provider.tail(kept.clone()).await?, 1);
            assert_eq!(
                provider.tail(omitted.clone()).await,
                Err(StreamError::NotFound)
            );

            // The skipped condition now passes, but an exact retry still owns
            // the original selected effects and its original durable receipt.
            provider.append(append(&root, 0)?).await?;
            let CommitOutcome::Committed(retry) = provider.commit(request.clone()).await? else {
                return Err("selected retry conflicted".into());
            };
            assert_eq!(retry.commit_id, receipt.commit_id);
            assert_eq!(retry.skipped, receipt.skipped);
            assert_eq!(
                provider.tail(omitted.clone()).await,
                Err(StreamError::NotFound)
            );
            assert_eq!(
                frames(&provider)?.0,
                before.0 + 1,
                "retry emits no new frame"
            );

            // A skipped observation that would become true after losing the
            // lease cannot be represented by the command journal. Refuse it
            // before any effect, rather than acknowledge an unreplayable write.
            let unsafe_request = CommitRequest {
                deadline_unix_millis: None,
                effects: vec![
                    CommitEffect {
                        conditions: vec![CommitCondition::Tail {
                            path: root.clone(),
                            expected: 0,
                        }],
                        mutations: vec![CommitMutation::Append {
                            path: omitted.clone(),
                            records: vec![Bytes::from_static(b"unstable").into()],
                        }],
                        soft: true,
                    },
                    CommitEffect::hard(
                        Vec::new(),
                        vec![CommitMutation::Append {
                            path: unsafe_path.clone(),
                            records: vec![Bytes::from_static(b"must not publish").into()],
                        }],
                    ),
                ],
                idempotency_key: IdempotencyKey::unique(),
            };
            assert_eq!(
                provider.commit(unsafe_request).await,
                Err(StreamError::InvalidArgument)
            );
            assert_eq!(
                provider.tail(unsafe_path.clone()).await,
                Err(StreamError::NotFound)
            );
            assert_eq!(frames(&provider)?.0, before.0 + 1);

            // Conversely, an omitted mutation of an existing durable path must
            // not reject or durably record a genuinely ephemeral publication.
            let CommitOutcome::Committed(selected) = provider
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    effects: vec![
                        CommitEffect {
                            conditions: vec![CommitCondition::Tail {
                                path: base.clone(),
                                expected: 2,
                            }],
                            mutations: vec![CommitMutation::Append {
                                path: base.clone(),
                                records: vec![Bytes::from_static(b"omitted durable").into()],
                            }],
                            soft: true,
                        },
                        CommitEffect::hard(
                            Vec::new(),
                            vec![CommitMutation::Append {
                                path: root.clone(),
                                records: vec![Bytes::from_static(b"ephemeral").into()],
                            }],
                        ),
                    ],
                    idempotency_key: IdempotencyKey::unique(),
                })
                .await?
            else {
                return Err("selected ephemeral effect was refused".into());
            };
            assert_eq!(selected.skipped, [0]);
            assert_eq!(provider.tail(root.clone()).await?, 2);
            assert_eq!(provider.tail(base.clone()).await?, 1);
            assert_eq!(
                frames(&provider)?.0,
                before.0 + 1,
                "selected lease write has no frame"
            );
            if release_first {
                lease.take().ok_or("missing lease")?.settle().await?;
                assert_eq!(provider.tail(kept.clone()).await?, 1);
            }
            drop(provider);
            let reopened = LocalStream::open(directory.path(), limits).await?;
            assert_eq!(reopened.tail(kept.clone()).await?, 1);
            assert_eq!(reopened.tail(base).await?, 1);
            assert_eq!(reopened.tail(root).await, Err(StreamError::NotFound));
            assert_eq!(reopened.tail(omitted).await, Err(StreamError::NotFound));
            assert_eq!(reopened.tail(unsafe_path).await, Err(StreamError::NotFound));
            let CommitOutcome::Committed(replayed) = reopened.commit(request).await? else {
                return Err("reopened retry conflicted".into());
            };
            assert_eq!(replayed.commit_id, receipt.commit_id);
            assert_eq!(replayed.skipped, receipt.skipped);
            if let Some(lease) = lease {
                lease.settle().await?;
            }
            assert_eq!(reopened.tail(kept).await?, 1);
        }
    }
    Ok(())
}

/// A retained success is independent of later conditions and lease lifetimes.
#[tokio::test]
async fn saved_commit_retries_keep_their_original_scope_after_conditions_change() -> Result {
    for local in [false, true] {
        for soft_first in [false, true] {
            let directory = tempfile::tempdir()?;
            let limits = LocalStreamLimits::default();
            // MemoryStream exercises the canonical planner directly. LocalStream
            // additionally exercises native pinned retries and journal recovery.
            let journal = if local {
                Some(LocalStream::open(directory.path(), limits).await?)
            } else {
                None
            };
            let provider: Arc<dyn StreamProvider> = match &journal {
                Some(local) => Arc::new(local.clone()),
                None => Arc::new(MemoryStream::default()),
            };
            let base = StreamPath::new("kept/base")?;
            let kept = StreamPath::new("kept/retried")?;
            let root = StreamPath::new("scratch")?;
            let omitted = StreamPath::new("scratch/omitted")?;
            provider.append(append(&base, 0)?).await?;
            let lease = provider.lease_ephemeral(root.clone()).await?;
            provider.append(append(&root, 0)?).await?;
            let soft = CommitEffect {
                conditions: vec![
                    CommitCondition::Tail {
                        path: root.clone(),
                        expected: 0,
                    },
                    CommitCondition::Tail {
                        path: base.clone(),
                        expected: 2,
                    },
                ],
                mutations: vec![CommitMutation::Append {
                    path: omitted.clone(),
                    records: vec![Bytes::from_static(b"must remain omitted").into()],
                }],
                soft: true,
            };
            let hard = CommitEffect::hard(
                vec![CommitCondition::Absent { path: kept.clone() }],
                vec![CommitMutation::Append {
                    path: kept.clone(),
                    records: vec![Bytes::from_static(b"retained success").into()],
                }],
            );
            let request = CommitRequest {
                deadline_unix_millis: None,
                effects: if soft_first {
                    vec![soft, hard]
                } else {
                    vec![hard, soft]
                },
                idempotency_key: IdempotencyKey::unique(),
            };
            let original = provider.commit(request.clone()).await?;
            let CommitOutcome::Committed(receipt) = &original else {
                return Err("durable selection was refused".into());
            };
            assert_eq!(receipt.skipped, [if soft_first { 0 } else { 1 }]);
            // The durable condition kept the omitted effect replay-safe at its
            // original publication. It now passes, including with the lease absent.
            provider.append(append(&base, 1)?).await?;
            let before_retry = journal.as_ref().map(frames).transpose()?;
            assert_eq!(
                provider.commit(request.clone()).await,
                Ok(original.clone()),
                "saved success must not recheck current conditions"
            );
            lease.settle().await?;
            assert_eq!(provider.commit(request.clone()).await?, original);
            assert_eq!(
                journal.as_ref().map(frames).transpose()?,
                before_retry,
                "retry emits no frame"
            );
            assert_eq!(provider.tail(kept.clone()).await?, 1);
            assert_eq!(
                provider.tail(omitted.clone()).await,
                Err(StreamError::NotFound)
            );
            drop(provider);
            drop(journal);
            if !local {
                continue;
            }
            let reopened = LocalStream::open(directory.path(), limits).await?;
            let CommitOutcome::Committed(replayed) = reopened.commit(request).await? else {
                return Err("reopened saved success conflicted".into());
            };
            assert_eq!(replayed.commit_id, receipt.commit_id);
            assert_eq!(replayed.skipped, receipt.skipped);
            assert_eq!(reopened.tail(kept).await?, 1);
            assert_eq!(reopened.tail(omitted).await, Err(StreamError::NotFound));
            assert_eq!(Some(frames(&reopened)?), before_retry);
        }
    }
    Ok(())
}
