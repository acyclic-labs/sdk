use super::*;

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
        owner: None,
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

#[tokio::test]
async fn staged_pages_do_not_publish_a_claim_after_its_owner_is_revoked() -> Result<()> {
    let provider = Arc::new(acyclic_stream::MemoryStream::default());
    let execution = StreamPath::new("fenced-snapshot/execution")?;
    let head = StreamPath::new("fenced-snapshot/head")?;
    let pages = StreamPath::new("fenced-snapshot/pages")?;
    let owner = StreamPath::new("fenced-snapshot/owner")?;
    let first = provider
        .append(acyclic_stream::AppendRequest {
            path: owner.clone(),
            records: vec![OwnedBytes::from_static(b"owner").into()],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    assert!(matches!(first, AppendOutcome::Committed(_)));
    let mut cut = Cut {
        operation: OperationId::from_bytes([8; 16]),
        execution: &execution,
        head: &head,
        pages: &pages,
        execution_condition: CommitCondition::Absent {
            path: execution.clone(),
        },
        head_condition: CommitCondition::Absent { path: head.clone() },
        owner: Some(CommitCondition::Tail {
            path: owner.clone(),
            expected: 1,
        }),
        execution_tail: 0,
        head_tail: 0,
        base: 0,
        digest: [8; 32],
    };
    let records = vec![OwnedBytes::from_static(b"page"); acyclic_stream::MAX_ITEMS + 1];
    let staged = cut
        .stage(&provider, 0, &records, &mut None)
        .await?
        .expect("staged snapshot");
    assert!(matches!(
        provider.tail(execution.clone()).await,
        Err(StreamError::NotFound)
    ));
    assert!(matches!(
        provider.tail(head.clone()).await,
        Err(StreamError::NotFound)
    ));
    let revoked = provider
        .append(acyclic_stream::AppendRequest {
            path: owner,
            records: vec![OwnedBytes::from_static(b"revoked").into()],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await?;
    assert!(matches!(revoked, AppendOutcome::Committed(_)));
    cut.base = staged as u64;
    let observation = OwnedBytes::from_static(b"claim");
    let checkpoint = OwnedBytes::from_static(b"checkpoint");
    assert!(
        !cut.publish(
            &provider,
            &records[staged..],
            &observation,
            &checkpoint,
            &mut None
        )
        .await?
    );
    assert!(
        matches!(
            provider.tail(execution.clone()).await,
            Err(StreamError::NotFound)
        ),
        "revoked owner published a claim"
    );
    // Omitting only the original owner fence admits the same final cut.
    cut.owner = None;
    cut.digest = [9; 32];
    assert!(
        cut.publish(
            &provider,
            &records[staged..],
            &observation,
            &checkpoint,
            &mut None
        )
        .await?,
        "unfenced negative control did not admit the revoked owner's claim"
    );
    assert_eq!(provider.tail(execution).await?, 1);
    Ok(())
}
