use super::*;

#[tokio::test]
async fn atomic_cut_rejects_missing_members_and_preserves_conditional_refusal() -> Result<()> {
    let execution = StreamPath::new("atomic/execution")?;
    let head = StreamPath::new("atomic/head")?;
    let pages = StreamPath::new("atomic/pages")?;
    let observation = OwnedBytes::from_static(b"observation");
    let checkpoint = OwnedBytes::from_static(b"checkpoint");
    let records = [
        OwnedBytes::from_static(b"first"),
        OwnedBytes::from_static(b"second"),
    ];
    let cut = Cut {
        operation: OperationId::from_bytes([9; 16]),
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
            assert!(confirmed?);
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
            assert!(!cut.confirm(&refused, 0, &records, &observation, &checkpoint)?);
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
