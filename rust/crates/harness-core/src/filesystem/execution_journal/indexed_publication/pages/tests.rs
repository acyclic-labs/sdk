use super::*;

#[tokio::test]
async fn atomic_cut_confirms_context_and_epoch_contributions_at_original_positions() -> Result<()> {
    composed_cut_keeps_caller_members_and_original_request().await?;
    #[cfg(not(target_arch = "wasm32"))]
    composed_cut_retains_original_native_construction().await?;
    let (journal, _issuer) = crate::filesystem::execution_journal::test_support::journal().await?;
    let operation = OperationId::from_bytes([11; 16]);
    let execution = crate::distributed::execution_path(operation, &mut None)?;
    let head = journal.index_path(operation, "head", None)?;
    let pages = journal.index_path(operation, "pages", None)?;
    let context = StreamPath::new("contributed/context")?;
    let epoch = StreamPath::new("contributed/epoch")?;
    let observation = OwnedBytes::from_static(b"observation");
    let checkpoint = OwnedBytes::from_static(b"checkpoint");
    let records = [OwnedBytes::from_static(b"index page")];
    let envelope = OwnedBytes::from_static(b"original context envelope");
    let snapshot = OwnedBytes::from_static(b"original epoch checkpoint");
    for omitted in [None, Some(0), Some(1), Some(2), Some(3), Some(4)] {
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let seed = provider
            .append(acyclic_stream::AppendRequest {
                path: context.clone(),
                records: vec![OwnedBytes::from_static(b"prior context").into()],
                if_tail: None,
                idempotency_key: acyclic_stream::IdempotencyKey::unique(),
            })
            .await?;
        let acyclic_stream::AppendOutcome::Committed(seed) = seed else {
            unreachable!("unconditional initial append refused")
        };
        let contributions = [
            Contribution {
                condition: CommitCondition::Head {
                    path: context.clone(),
                    expected: 1,
                    commit_id: seed.commit_id,
                },
                record: &envelope,
            },
            Contribution {
                condition: CommitCondition::Absent {
                    path: epoch.clone(),
                },
                record: &snapshot,
            },
        ];
        let cut = Cut {
            operation,
            execution: &execution,
            head: &head,
            pages: &pages,
            execution_condition: CommitCondition::Absent {
                path: execution.clone(),
            },
            head_condition: CommitCondition::Absent { path: head.clone() },
            execution_tail: 0,
            head_tail: 0,
            base: 0,
            digest: [11; 32],
            contributions: &contributions,
        };
        let mut work = None;
        let mut request = cut.request(
            CommitCondition::Absent {
                path: pages.clone(),
            },
            &records,
            Some((&observation, &checkpoint)),
            &mut work,
        )?;
        assert_eq!(request.0.conditions.len(), 5);
        assert_eq!(request.0.mutations.len(), 5);
        if let Some(index) = omitted {
            request.0.mutations.remove(index);
        }
        let original = request.0.clone();
        let outcome = provider.commit(request.0).await?;
        let confirmed = cut.confirm(&outcome, 0, &records, &observation, &checkpoint);
        if omitted.is_some() {
            assert!(
                matches!(confirmed, Err(Error::Indeterminate(operation)) if operation == cut.operation)
            );
        } else {
            assert!(confirmed?.is_some());
            let CommitOutcome::Committed(mut receipt) = outcome else {
                unreachable!()
            };
            receipt.appends.swap(3, 4);
            assert!(matches!(
                cut.confirm(
                    &CommitOutcome::Committed(receipt),
                    0,
                    &records,
                    &observation,
                    &checkpoint
                ),
                Err(Error::Indeterminate(_))
            ));
            let mut racing = original;
            racing.idempotency_key = acyclic_stream::IdempotencyKey::unique();
            let refused = provider.commit(racing).await?;
            assert!(matches!(refused, CommitOutcome::Conflict(_)));
            assert!(
                cut.confirm(&refused, 0, &records, &observation, &checkpoint)?
                    .is_none()
            );
        }
        drop(request.1);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn composed_cut_retains_original_native_construction() -> Result<()> {
    let (journal, _issuer) = crate::filesystem::execution_journal::test_support::journal().await?;
    let operation = OperationId::from_bytes([13; 16]);
    let execution = crate::distributed::execution_path(operation, &mut None)?;
    let head = journal.index_path(operation, "head", None)?;
    let pages = journal.index_path(operation, "pages", None)?;
    let cut = Cut {
        operation,
        execution: &execution,
        head: &head,
        pages: &pages,
        execution_condition: CommitCondition::Absent {
            path: execution.clone(),
        },
        head_condition: CommitCondition::Absent { path: head.clone() },
        execution_tail: 0,
        head_tail: 0,
        base: 0,
        digest: [13; 32],
        contributions: &[],
    };
    let observation = OwnedBytes::from_static(b"observation");
    let checkpoint = OwnedBytes::from_static(b"checkpoint");
    let records = [OwnedBytes::from_static(b"index page")];
    for mode in 0..3 {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(1_000_000, 4 * 1024 * 1024);
        let original = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let foreign = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = original.live_allocation_bytes();
        let foreign_baseline = foreign.live_allocation_bytes();
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let store = acyclic_fs::StreamAuthorityStore::new(provider.clone());
        let result = {
            let io = PreparationIo::from_native_work(&original)?;
            let mut history = io.history_io();
            let mut work = Some(&mut history as &mut dyn crate::store::HistoryIo);
            let (key, held) = crate::distributed::digest_key(&[13; 32], &mut work)?;
            let (request, quota) = crate::distributed::commit_request(key, held, &mut work);
            let mut commit = acyclic_fs::FilesystemCommit::new(request);
            let context = if mode == 1 { &foreign } else { &original };
            let mut existing = context.claim(32, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
            commit
                .retain_native_custody(&mut existing, context)
                .map_err(acyclic_fs::FsError::from)?;
            if mode == 2 {
                cancellation.cancel();
            }
            let result = cut.contribute(
                (commit, quota),
                CommitCondition::Absent {
                    path: pages.clone(),
                },
                &records,
                &observation,
                &checkpoint,
            );
            match result {
                Ok((commit, quota)) => {
                    assert_eq!(mode, 0, "foreign or cancelled preparation was accepted");
                    assert_eq!(quota.retained(), 0);
                    drop(quota);
                    assert_eq!(io.owned_bytes()?, 0);
                    Some(commit)
                }
                Err(error) => {
                    match mode {
                        1 => assert!(matches!(
                            error,
                            Error::NativeWork {
                                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                                ..
                            }
                        )),
                        2 => assert!(matches!(
                            error,
                            Error::NativeWork {
                                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                                ..
                            }
                        )),
                        _ => panic!("original construction refused: {error:?}"),
                    }
                    drop(error);
                    None
                }
            }
        };
        if let Some(commit) = result {
            // Both preparation adapters are gone. The original unsent batch
            // still pays for every journal vector and its pre-existing guard.
            assert!(original.live_allocation_bytes() > baseline + 32);
            assert_eq!(commit.request().mutations.len(), 3);
            let mut work = acyclic_fs::AuthorityWork::native(&original, &cancellation);
            let outcome = store
                .submit(commit, &mut work)
                .await
                .map_err(acyclic_fs::FsError::from)?;
            assert!(matches!(
                outcome,
                acyclic_fs::FilesystemCommitOutcome::Committed(_)
            ));
        } else {
            assert_eq!(
                provider.tail(execution.clone()).await,
                Err(acyclic_stream::StreamError::NotFound)
            );
        }
        drop(store);
        drop(provider);
        assert_eq!(original.live_allocation_bytes(), baseline, "mode {mode}");
        assert_eq!(foreign.live_allocation_bytes(), foreign_baseline);
    }
    Ok(())
}

async fn composed_cut_keeps_caller_members_and_original_request() -> Result<()> {
    let (journal, _issuer) = crate::filesystem::execution_journal::test_support::journal().await?;
    let provider = Arc::new(acyclic_stream::MemoryStream::default());
    let operation = OperationId::from_bytes([12; 16]);
    let execution = crate::distributed::execution_path(operation, &mut None)?;
    let head = journal.index_path(operation, "head", None)?;
    let pages = journal.index_path(operation, "pages", None)?;
    let context = StreamPath::new("contributed/context")?;
    let caller = StreamPath::new("contributed/delta")?;
    let epoch = StreamPath::new("contributed/epoch")?;
    let fork = StreamPath::new("contributed/fork")?;
    provider
        .append(acyclic_stream::AppendRequest {
            path: context.clone(),
            records: vec![OwnedBytes::from_static(b"prior context").into()],
            if_tail: None,
            idempotency_key: acyclic_stream::IdempotencyKey::unique(),
        })
        .await?;
    let observation = OwnedBytes::from_static(b"observation");
    let checkpoint = OwnedBytes::from_static(b"checkpoint");
    let records = [OwnedBytes::from_static(b"index page")];
    let envelope = OwnedBytes::from_static(b"original context envelope");
    let snapshot = OwnedBytes::from_static(b"original epoch checkpoint");
    let contributions = [
        Contribution {
            condition: CommitCondition::Tail {
                path: context.clone(),
                expected: 1,
            },
            record: &envelope,
        },
        Contribution {
            condition: CommitCondition::Absent {
                path: epoch.clone(),
            },
            record: &snapshot,
        },
    ];
    let cut = Cut {
        operation,
        execution: &execution,
        head: &head,
        pages: &pages,
        execution_condition: CommitCondition::Absent {
            path: execution.clone(),
        },
        head_condition: CommitCondition::Absent { path: head.clone() },
        execution_tail: 0,
        head_tail: 0,
        base: 0,
        digest: [12; 32],
        contributions: &contributions,
    };
    let key = acyclic_stream::IdempotencyKey::unique();
    let mut work = None;
    let (mut request, mut quota) = crate::distributed::commit_request(key.clone(), 0, &mut work);
    // The caller's original batch owns its key and members before the Cut is added.
    request.conditions.extend([
        CommitCondition::Absent {
            path: caller.clone(),
        },
        CommitCondition::Absent { path: fork.clone() },
    ]);
    add_records(
        &mut request,
        &mut quota,
        &caller,
        &[OwnedBytes::from_static(b"caller effect")],
    )?;
    request.mutations.push(CommitMutation::Fork {
        source: context.clone(),
        destination: fork,
        at_tail: 1,
        records: Vec::new(),
        exclude: Vec::new(),
    });
    let mut prepared = cut.contribute(
        (acyclic_fs::FilesystemCommit::new(request), quota),
        CommitCondition::Absent {
            path: pages.clone(),
        },
        &records,
        &observation,
        &checkpoint,
    )?;
    assert_eq!(prepared.0.request().idempotency_key, key);
    let store = acyclic_fs::StreamAuthorityStore::new(provider.clone());
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut fs_work =
        acyclic_fs::AuthorityWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation);
    store
        .seal(&mut prepared.0, &mut fs_work)
        .map_err(|_| invalid(None))?;
    let original = prepared.0.request().clone();
    let outcome = provider.commit(original.clone()).await?;
    let CommitOutcome::Committed(receipt) = &outcome else {
        panic!("fresh composed cut refused")
    };
    let committed = receipt.commit_id;
    assert!(
        cut.confirm(&outcome, 0, &records, &observation, &checkpoint)
            .is_err()
    );
    let (mut commit, quota) = prepared;
    for omitted in 0..receipt.appends.len() {
        let mut incomplete = receipt.clone();
        incomplete.appends.remove(omitted);
        let Err(acyclic_fs::FilesystemCommitOutcome::Unknown {
            commit: retained, ..
        }) = store.prepare_settlement(commit, CommitOutcome::Committed(incomplete))
        else {
            panic!("missing canonical caller or journal member was accepted")
        };
        commit = retained;
        assert_eq!(commit.request(), &original);
    }
    let mut incomplete = receipt.clone();
    incomplete.forks.clear();
    let Err(acyclic_fs::FilesystemCommitOutcome::Unknown {
        commit: retained, ..
    }) = store.prepare_settlement(commit, CommitOutcome::Committed(incomplete))
    else {
        panic!("missing caller fork was accepted")
    };
    let validated = store
        .prepare_settlement(retained, outcome)
        .map_err(|_| invalid(None))?;
    // Full membership has already been checked by its original owner. A semantic
    // mismatch cannot retire the prepared result or require a second validation.
    assert!(
        cut.confirm_composed(&validated, 1, &records, &observation, &checkpoint)
            .is_err()
    );
    assert_eq!(
        cut.confirm_composed(&validated, 0, &records, &observation, &checkpoint)?,
        Some(committed)
    );
    let acyclic_fs::FilesystemCommitOutcome::Committed(result) = store
        .finish_settlement(validated)
        .map_err(|_| invalid(None))?
    else {
        panic!("confirmed original batch did not finish")
    };
    assert_eq!(result.commit_id, committed);
    let mut replay_commit = acyclic_fs::FilesystemCommit::new(original.clone());
    store
        .seal(&mut replay_commit, &mut fs_work)
        .map_err(|_| invalid(None))?;
    let replay = provider.commit(original.clone()).await?;
    let validated = store
        .prepare_settlement(replay_commit, replay)
        .map_err(|_| invalid(None))?;
    assert_eq!(
        cut.confirm_composed(&validated, 0, &records, &observation, &checkpoint)?,
        Some(committed)
    );
    drop(validated);
    let mut racing = original;
    racing.idempotency_key = acyclic_stream::IdempotencyKey::unique();
    let mut racing = acyclic_fs::FilesystemCommit::new(racing);
    store
        .seal(&mut racing, &mut fs_work)
        .map_err(|_| invalid(None))?;
    let loser = provider.commit(racing.request().clone()).await?;
    let loser = store
        .prepare_settlement(racing, loser)
        .map_err(|_| invalid(None))?;
    assert_eq!(
        cut.confirm_composed(&loser, 0, &records, &observation, &checkpoint)?,
        None
    );
    assert!(matches!(
        store.finish_settlement(loser).map_err(|_| invalid(None))?,
        acyclic_fs::FilesystemCommitOutcome::Rejected(_)
    ));
    drop(quota);
    Ok(())
}

#[tokio::test]
async fn atomic_cut_rejects_missing_members_and_preserves_conditional_refusal() -> Result<()> {
    let (journal, _issuer) = crate::filesystem::execution_journal::test_support::journal().await?;
    let operation = OperationId::from_bytes([9; 16]);
    let execution = crate::distributed::execution_path(operation, &mut None)?;
    let head = journal.index_path(operation, "head", None)?;
    let pages = journal.index_path(operation, "pages", None)?;
    let observation = OwnedBytes::from_static(b"observation");
    let checkpoint = OwnedBytes::from_static(b"checkpoint");
    let records = [
        OwnedBytes::from_static(b"first"),
        OwnedBytes::from_static(b"second"),
    ];
    let cut = Cut {
        operation,
        execution: &execution,
        head: &head,
        pages: &pages,
        execution_condition: CommitCondition::Absent {
            path: execution.clone(),
        },
        head_condition: CommitCondition::Absent { path: head.clone() },
        execution_tail: 0,
        head_tail: 0,
        base: 0,
        digest: [9; 32],
        contributions: &[],
    };
    // Each control submits the original request through the real provider.
    // Omitting one member still produces a valid Stream commit, but cannot
    // authorize an execution publication with its complete checkpoint/index.
    for omitted in [None, Some(0), Some(1), Some(2)] {
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let mut work = None;
        let mut request = cut.request(
            CommitCondition::Absent {
                path: pages.clone(),
            },
            &records,
            Some((&observation, &checkpoint)),
            &mut work,
        )?;
        if let Some(index) = omitted {
            request.0.mutations.remove(index);
        }
        let original = request.0.clone();
        let outcome = provider.commit(request.0).await?;
        let confirmed = cut.confirm(&outcome, 0, &records, &observation, &checkpoint);
        if omitted.is_some() {
            assert!(
                matches!(confirmed, Err(Error::Indeterminate(operation))
                if operation == cut.operation),
                "partial cut authorized execution"
            );
        } else {
            assert!(confirmed?.is_some());
            let CommitOutcome::Committed(mut receipt) = outcome else {
                panic!("fresh atomic cut refused");
            };
            receipt.appends.swap(0, 1);
            assert!(
                matches!(
                    cut.confirm(
                        &CommitOutcome::Committed(receipt),
                        0,
                        &records,
                        &observation,
                        &checkpoint
                    ),
                    Err(Error::Indeterminate(_))
                ),
                "receipt ranges lost their original request membership"
            );
            let mut racing = original;
            racing.idempotency_key = acyclic_stream::IdempotencyKey::unique();
            let refused = provider.commit(racing).await?;
            assert!(matches!(refused, CommitOutcome::Conflict(_)));
            assert!(
                cut.confirm(&refused, 0, &records, &observation, &checkpoint)?
                    .is_none()
            );
        }
        drop(request.1);
    }
    Ok(())
}

#[tokio::test]
async fn oversized_snapshots_stage_pages_and_publish_one_final_cut() -> Result<()> {
    for native in [false, true] {
        for large in [false, true] {
            check_publication(native, large).await?;
        }
    }
    Ok(())
}

async fn check_publication(native: bool, large: bool) -> Result<()> {
    let provider = Arc::new(acyclic_stream::MemoryStream::default());
    let execution = StreamPath::new("snapshot/execution")?;
    let head = StreamPath::new("snapshot/head")?;
    let pages = StreamPath::new("snapshot/pages")?;
    let record = if large {
        OwnedBytes::from(vec![7; acyclic_stream::MAX_RECORD_BYTES])
    } else {
        OwnedBytes::from_static(b"page")
    };
    let count = if large {
        acyclic_stream::MAX_COMMAND_BYTES / acyclic_stream::MAX_RECORD_BYTES + 1
    } else {
        acyclic_stream::MAX_ITEMS + 1
    };
    let records = vec![record; count];
    let observation = OwnedBytes::from_static(b"observation");
    let checkpoint = OwnedBytes::from_static(b"checkpoint");
    let cut = Cut {
        operation: OperationId::from_bytes([7; 16]),
        execution: &execution,
        head: &head,
        pages: &pages,
        execution_condition: CommitCondition::Absent {
            path: execution.clone(),
        },
        head_condition: CommitCondition::Absent { path: head.clone() },
        execution_tail: 0,
        head_tail: 0,
        base: 0,
        digest: [7; 32],
        contributions: &[],
    };
    let mut old_work = None;
    let old = cut.request(
        CommitCondition::Absent {
            path: pages.clone(),
        },
        &records,
        Some((&observation, &checkpoint)),
        &mut old_work,
    )?;
    assert!(
        matches!(
            provider.commit(old.0).await,
            Err(StreamError::LimitExceeded)
        ),
        "one-command negative control accepted an oversized snapshot"
    );
    drop(old.1);
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(256 * 1024 * 1024, 16 * 1024 * 1024);
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let native_baseline = work.live_allocation_bytes();
    let io = if native {
        PreparationIo::from_native_work(&work)?
    } else {
        PreparationIo::from_budget(budget, &cancellation)?
    };
    let mut history = Some(io.history_io());
    let mut work_io = history
        .as_mut()
        .map(|io| io as &mut dyn crate::store::HistoryIo);
    assert!(
        cut.publish(&provider, &records, &observation, &checkpoint, &mut work_io)
            .await?
            .is_some()
    );
    let execution_record =
        crate::store::operations::one_record(&provider, &execution, 0, &mut None)
            .await?
            .expect("published observation");
    let head_record = crate::store::operations::one_record(&provider, &head, 0, &mut None)
        .await?
        .expect("published checkpoint");
    let first_page = crate::store::operations::one_record(&provider, &pages, 0, &mut None)
        .await?
        .expect("staged page");
    let last_page =
        crate::store::operations::one_record(&provider, &pages, count as u64 - 1, &mut None)
            .await?
            .expect("last page");
    assert_eq!(execution_record.value, observation);
    assert_eq!(head_record.value, checkpoint);
    assert_eq!(head_record.commit_id, execution_record.commit_id);
    assert_eq!(last_page.commit_id, head_record.commit_id);
    assert_ne!(
        first_page.commit_id, head_record.commit_id,
        "snapshot was not staged before its final cut"
    );
    drop((execution_record, head_record, first_page, last_page));
    drop(provider);
    assert_eq!(
        io.owned_bytes()?,
        0,
        "snapshot publication retained acknowledgment custody"
    );
    if native {
        assert_eq!(
            work.live_allocation_bytes(),
            native_baseline,
            "snapshot publication retained native custody"
        );
    }
    Ok(())
}
