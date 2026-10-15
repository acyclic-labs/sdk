use super::*;

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn dense_paging_preserves_original_facts_after_publication_and_retirement(
        lengths in proptest::collection::vec(1_usize..65, 1..13),
        page_size in 1_u32..9,
    ) {
        let result = futures::executor::block_on(dense_paging(&lengths, page_size));
        proptest::prop_assert!(result.is_ok(), "{result:?}");
    }
}

async fn dense_paging(lengths: &[usize], page_size: u32) -> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let cancel = CancellationToken::new();
    let workspace = fs
        .create_workspace("dense-accounting", B, &cancel)
        .await?
        .value;
    let id = volume_authority_id(workspace.id().volume_id());
    let mut expected = vec![(root(workspace.head(B, &cancel).await?.value.id()), 0_u128)];
    for (index, length) in lengths.iter().copied().enumerate() {
        workspace
            .write("/file", bytes::Bytes::from(vec![index as u8 + 1; length]))
            .await?;
        expected.push((
            root(workspace.head(B, &cancel).await?.value.id()),
            length as u128,
        ));
    }
    let original = records(&authority, id).await?;
    assert_eq!(original.len(), expected.len());
    let mut position = AccountingCursor::genesis();
    let mut consumed = Vec::new();
    let mut facts = Vec::new();
    loop {
        let next = page(&authority, &objects, id, position, page_size).await?;
        assert!(!next.facts.is_empty());
        assert!(next.facts.len() <= page_size as usize);
        assert_eq!(
            next.watermark_sequence,
            original.last().ok_or("no records")?.sequence
        );
        assert_eq!(next.facts[0].sequence.get(), position.sequence.get() + 1);
        next.work.verify(BUDGET)?;
        consumed.push((position, next.facts.clone()));
        facts.extend(next.facts);
        position = next.next;
        if next.complete {
            break;
        }
    }
    assert_eq!(facts.len(), original.len());
    for ((fact, record), (generation_root, logical_regular_file_bytes)) in
        facts.iter().zip(&original).zip(expected)
    {
        assert_eq!(fact.sequence, record.sequence);
        assert_eq!(fact.operation_id, record.operation_id);
        assert_eq!(fact.commit_digest, record.digest);
        assert_eq!(Some(fact.settled_at_micros), record.settled_at_micros);
        assert_eq!(
            fact.operation_class,
            AccountingOperationClass::CustomerMutation
        );
        assert_eq!(
            fact.transition,
            AccountingTransition::Published {
                volume_id: workspace.id().volume_id(),
                generation_root,
                logical_regular_file_bytes
            }
        );
    }
    workspace
        .write("/file", bytes::Bytes::from(vec![99; 73]))
        .await?;
    let later_root = root(workspace.head(B, &cancel).await?.value.id());
    let later = records(&authority, id)
        .await?
        .pop()
        .ok_or("later record missing")?;
    let deletion = IdempotencyKey::new();
    workspace.delete(deletion).await?;
    for (anchor, facts) in consumed {
        assert_eq!(
            authority
                .accounting_page(&objects, id, anchor, facts.len() as u32, BUDGET, &cancel)
                .await?
                .facts,
            facts
        );
    }
    let terminal = authority
        .accounting_page(&objects, id, position, 2, BUDGET, &cancel)
        .await?;
    assert!(terminal.complete);
    assert_eq!(terminal.facts.len(), 2);
    assert_eq!(
        terminal.facts[0].transition,
        AccountingTransition::Published {
            volume_id: workspace.id().volume_id(),
            generation_root: later_root,
            logical_regular_file_bytes: 73
        }
    );
    assert_eq!(terminal.facts[0].operation_id, later.operation_id);
    assert_eq!(terminal.facts[0].commit_digest, later.digest);
    assert_eq!(
        Some(terminal.facts[0].settled_at_micros),
        later.settled_at_micros
    );
    assert_eq!(
        terminal.facts[1].transition,
        AccountingTransition::WorkspaceDeleted {
            volume_id: workspace.id().volume_id()
        }
    );
    assert_eq!(terminal.facts[1].operation_id, deletion.operation_id());
    assert!(
        terminal
            .facts
            .iter()
            .all(|fact| fact.operation_class == AccountingOperationClass::CustomerMutation)
    );
    assert_eq!(terminal.next.digest, terminal.facts[1].commit_digest);
    let empty = authority
        .accounting_page(&objects, id, terminal.next, 2, BUDGET, &cancel)
        .await?;
    assert!(empty.complete && empty.facts.is_empty());
    assert_eq!(empty.next, terminal.next);
    assert!(matches!(
        authority
            .head(id, &mut AuthorityWork::new(BUDGET, &cancel))
            .await,
        Err(AuthorityStoreError::Retired)
    ));
    assert!(matches!(
        authority
            .fence(
                id,
                Head {
                    epoch: later.epoch,
                    sequence: terminal.next.sequence,
                    digest: terminal.next.digest
                },
                &mut AuthorityWork::new(BUDGET, &cancel)
            )
            .await,
        Err(AuthorityStoreError::Retired)
    ));
    Ok(())
}

#[tokio::test]
async fn retention_pages_distinguish_customer_pins_and_internal_fork_retention()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let cancel = CancellationToken::new();
    let workspace = fs
        .create_workspace("retained-accounting", B, &cancel)
        .await?
        .value;
    workspace.write_text("/file", "retained").await?;
    let generation = workspace.head(B, &cancel).await?.value;
    workspace.checkpoint("checkpoint").await?;
    workspace.pin("pin").await?;
    let fork = workspace
        .fork::<_, _, crate::WorkspaceError>(
            "accounting-fork",
            crate::ForkOptions::from_generation(generation.clone(), IdempotencyKey::new()),
            crate::PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    let fork_label = hex::encode(fork.id().volume_id().into_bytes());
    for (kind, label, owner, class) in [
        (
            RetentionKind::Checkpoint,
            "checkpoint",
            workspace.id().volume_id(),
            AccountingOperationClass::CustomerMutation,
        ),
        (
            RetentionKind::Pin,
            "pin",
            workspace.id().volume_id(),
            AccountingOperationClass::CustomerMutation,
        ),
        (
            RetentionKind::ForkBase,
            fork_label.as_str(),
            fork.id().volume_id(),
            AccountingOperationClass::InternalForkRetention,
        ),
    ] {
        let id = retention_authority_id(workspace.id().volume_id(), kind, label);
        let retained = page(&authority, &objects, id, AccountingCursor::genesis(), 1).await?;
        assert!(retained.complete);
        assert_eq!(retained.facts.len(), 1);
        assert_eq!(
            retained.facts[0].transition,
            AccountingTransition::Retained {
                volume_id: workspace.id().volume_id(),
                owner_volume_id: owner,
                kind,
                generation_root: root(generation.id()),
                logical_regular_file_bytes: 8
            }
        );
        assert_eq!(retained.facts[0].operation_class, class);
        assert_eq!(
            Some(retained.facts[0].settled_at_micros),
            records(&authority, id).await?[0].settled_at_micros
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_later_publication_changes_the_watermark_without_changing_consumed_facts()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let cancel = CancellationToken::new();
    let workspace = fs
        .create_workspace("interleaved-pages", B, &cancel)
        .await?
        .value;
    let id = volume_authority_id(workspace.id().volume_id());
    workspace.write_text("/file", "old").await?;
    let old = root(workspace.head(B, &cancel).await?.value.id());
    workspace.write_text("/file", "newer").await?;
    let newer = root(workspace.head(B, &cancel).await?.value.id());
    let first = page(&authority, &objects, id, AccountingCursor::genesis(), 1).await?;
    assert_eq!(first.watermark_sequence, Sequence::new(3));
    assert!(!first.complete);
    workspace.write_text("/file", "latest!").await?;
    let latest = root(workspace.head(B, &cancel).await?.value.id());
    let second = page(&authority, &objects, id, first.next, 2).await?;
    assert_eq!(second.watermark_sequence, Sequence::new(4));
    assert!(!second.complete);
    let last = page(&authority, &objects, id, second.next, 1).await?;
    assert!(last.complete);
    for (fact, (generation_root, logical_regular_file_bytes)) in second
        .facts
        .iter()
        .chain(&last.facts)
        .zip([(old, 3), (newer, 5), (latest, 7)])
    {
        assert_eq!(
            fact.transition,
            AccountingTransition::Published {
                volume_id: workspace.id().volume_id(),
                generation_root,
                logical_regular_file_bytes
            }
        );
    }
    assert_eq!(
        page(&authority, &objects, id, first.next, 2).await?.facts,
        second.facts
    );
    Ok(())
}
