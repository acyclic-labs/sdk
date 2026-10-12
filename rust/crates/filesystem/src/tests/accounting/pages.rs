use super::*;

#[tokio::test]
async fn dense_accounting_pages_preserve_old_generations_and_cursor_after_later_append()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs
        .create_workspace(
            "dense-accounting",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace.write_text("/file", "old").await?;
    let old = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value
        .id();
    workspace.write_text("/file", "newer").await?;
    let newer = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value
        .id();
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let cancellation = CancellationToken::new();
    let first = accounting_page(
        &authority,
        &objects,
        authority_id,
        AccountingCursor::genesis(),
        1,
        BUDGET,
        &cancellation,
    )
    .await?;
    assert_eq!(first.facts.len(), 1);
    assert_eq!(first.next.sequence, Sequence::new(1));
    assert_eq!(first.watermark_sequence, Sequence::new(3));
    assert!(!first.complete);
    workspace.write_text("/file", "latest!").await?;
    let latest = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value
        .id();
    let second = accounting_page(
        &authority,
        &objects,
        authority_id,
        first.next,
        2,
        BUDGET,
        &cancellation,
    )
    .await?;
    assert_eq!(second.facts.len(), 2);
    assert_eq!(second.next.sequence, Sequence::new(3));
    assert_eq!(second.watermark_sequence, Sequence::new(4));
    assert!(!second.complete);
    assert_eq!(
        second.facts[0].transition,
        AccountingTransition::Published {
            volume_id: workspace.id().volume_id(),
            generation_root: root(old),
            logical_regular_file_bytes: 3,
        }
    );
    assert_eq!(
        second.facts[1].transition,
        AccountingTransition::Published {
            volume_id: workspace.id().volume_id(),
            generation_root: root(newer),
            logical_regular_file_bytes: 5,
        }
    );
    let last = authority
        .accounting_page(
            &objects,
            authority_id,
            second.next,
            1,
            BUDGET,
            &cancellation,
        )
        .await?;
    assert!(last.complete);
    assert_eq!(last.facts.len(), 1);
    assert_eq!(
        last.facts[0].transition,
        AccountingTransition::Published {
            volume_id: workspace.id().volume_id(),
            generation_root: root(latest),
            logical_regular_file_bytes: 7,
        }
    );
    let durable = records(&authority, authority_id).await?;
    assert_eq!(durable.len(), 4);
    for (fact, record) in first
        .facts
        .iter()
        .chain(&second.facts)
        .chain(&last.facts)
        .zip(&durable)
    {
        assert_eq!(fact.sequence, record.sequence);
        assert_eq!(fact.operation_id, record.operation_id);
        assert_eq!(fact.commit_digest, record.digest);
        assert_eq!(Some(fact.settled_at_micros), record.settled_at_micros);
        assert!(fact.settled_at_micros > 0);
        assert_eq!(
            fact.operation_class,
            AccountingOperationClass::CustomerMutation
        );
    }
    assert_eq!(last.next.digest, durable[3].digest);
    assert_eq!(
        Some(last.watermark_settled_at_micros),
        durable[3].settled_at_micros
    );
    assert!(first.work.authority_records_read > 0);
    assert!(second.work.object_bytes_read > 0);
    first.work.verify(BUDGET)?;
    second.work.verify(BUDGET)?;
    last.work.verify(BUDGET)?;
    let empty = authority
        .accounting_page(&objects, authority_id, last.next, 2, BUDGET, &cancellation)
        .await?;
    assert!(empty.complete);
    assert!(empty.facts.is_empty());
    assert_eq!(empty.next, last.next);
    let replayed = accounting_page(
        &authority,
        &objects,
        authority_id,
        first.next,
        2,
        BUDGET,
        &cancellation,
    )
    .await?;
    assert_eq!(replayed.facts, second.facts);
    assert_eq!(replayed.next, second.next);
    Ok(())
}

#[tokio::test]
async fn accounting_projects_retention_and_deleted_workspace_without_reopening_retired_authority()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs
        .create_workspace(
            "retained-accounting",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace.write_text("/file", "retained").await?;
    let generation = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value
        .id();
    workspace.checkpoint("checkpoint").await?;
    workspace.pin("pin").await?;
    let cancellation = CancellationToken::new();
    for (kind, label) in [
        (RetentionKind::Checkpoint, "checkpoint"),
        (RetentionKind::Pin, "pin"),
    ] {
        let authority_id = retention_authority_id(workspace.id().volume_id(), kind, label);
        let page = authority
            .accounting_page(
                &objects,
                authority_id,
                AccountingCursor::genesis(),
                1,
                BUDGET,
                &cancellation,
            )
            .await?;
        assert!(page.complete);
        assert_eq!(page.facts.len(), 1);
        assert_eq!(
            page.facts[0].transition,
            AccountingTransition::Retained {
                volume_id: workspace.id().volume_id(),
                owner_volume_id: workspace.id().volume_id(),
                kind,
                generation_root: root(generation),
                logical_regular_file_bytes: 8,
            }
        );
        assert_eq!(
            page.facts[0].operation_class,
            AccountingOperationClass::CustomerMutation
        );
        assert_eq!(
            Some(page.facts[0].settled_at_micros),
            records(&authority, authority_id).await?[0].settled_at_micros
        );
    }
    let fork = workspace
        .fork::<_, _, crate::WorkspaceError>(
            "accounting-fork",
            crate::ForkOptions::from_generation(
                workspace
                    .head(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                    )
                    .await?
                    .value,
                IdempotencyKey::new(),
            ),
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    let fork_retention = retention_authority_id(
        workspace.id().volume_id(),
        RetentionKind::ForkBase,
        &hex::encode(fork.id().volume_id().into_bytes()),
    );
    let retained = authority
        .accounting_page(
            &objects,
            fork_retention,
            AccountingCursor::genesis(),
            1,
            BUDGET,
            &cancellation,
        )
        .await?;
    assert!(retained.complete);
    assert_eq!(retained.facts.len(), 1);
    assert_eq!(
        retained.facts[0].transition,
        AccountingTransition::Retained {
            volume_id: workspace.id().volume_id(),
            owner_volume_id: fork.id().volume_id(),
            kind: RetentionKind::ForkBase,
            generation_root: root(generation),
            logical_regular_file_bytes: 8,
        }
    );
    assert_eq!(
        retained.facts[0].operation_class,
        AccountingOperationClass::InternalForkRetention
    );
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let before = authority
        .accounting_page(
            &objects,
            authority_id,
            AccountingCursor::genesis(),
            2,
            BUDGET,
            &cancellation,
        )
        .await?;
    assert!(before.complete);
    workspace.delete(IdempotencyKey::new()).await?;
    let deleted = authority
        .accounting_page(
            &objects,
            authority_id,
            before.next,
            1,
            BUDGET,
            &cancellation,
        )
        .await?;
    assert!(deleted.complete);
    assert_eq!(deleted.facts.len(), 1);
    assert_eq!(
        deleted.facts[0].transition,
        AccountingTransition::WorkspaceDeleted {
            volume_id: workspace.id().volume_id(),
        }
    );
    assert_eq!(
        deleted.facts[0].operation_class,
        AccountingOperationClass::CustomerMutation
    );
    for failure in [
        ({
            let mut __authority_work = crate::AuthorityWork::new(BUDGET, &cancellation);
            let __authority_result = authority.head(authority_id, &mut __authority_work).await;
            __authority_work.receipt(__authority_result)
        })
        .err(),
        ({
            let mut __authority_work = crate::AuthorityWork::new(BUDGET, &cancellation);
            let __authority_result = authority
                .fence(
                    authority_id,
                    Head {
                        epoch: Epoch::GENESIS,
                        sequence: deleted.next.sequence,
                        digest: deleted.next.digest,
                    },
                    &mut __authority_work,
                )
                .await;
            __authority_work.receipt(__authority_result)
        })
        .err(),
    ] {
        assert!(matches!(
            failure
                .ok_or("retired ordinary authority operation succeeded")?
                .error,
            AuthorityStoreError::Retired
        ));
    }
    let repeated = authority
        .accounting_page(
            &objects,
            authority_id,
            before.next,
            1,
            BUDGET,
            &cancellation,
        )
        .await?;
    assert_eq!(repeated.facts, deleted.facts);
    assert_eq!(repeated.next, deleted.next);
    Ok(())
}
