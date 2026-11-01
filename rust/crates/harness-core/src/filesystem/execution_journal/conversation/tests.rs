use super::*;
use crate::conversation::wire::{Family, ItemKind, Provenance, WireItem};
use serde_json::value::RawValue;

#[tokio::test]
async fn envelope_admission_retains_original_position_and_rejects_retry_substitution() -> Result<()>
{
    let (journal, _issuer) = super::super::test_support::journal().await?;
    let operation = OperationId::new();
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(10_000_000, 16 * 1024 * 1024),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&original)?;
    let envelope = item(operation, 1, &io)?;
    assert!(matches!(
        journal
            .append_envelope_if_tail(0, "unbound", 0, &envelope, &io)?
            .await,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    let journal = journal.with_turn(operation, operation);
    journal
        .append(
            operation,
            "start",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            Some(&io),
        )?
        .await?;
    assert!(
        journal
            .append_envelope_if_tail(1, "item", 0, &envelope, &io)?
            .await?
    );
    assert!(
        journal
            .append_envelope_if_tail(1, "item", 0, &envelope, &io)?
            .await?
    );
    let changed = item(operation, 2, &io)?;
    assert!(
        journal
            .append_envelope_if_tail(1, "item", 0, &changed, &io)?
            .await
            .is_err()
    );
    let foreign = item(OperationId::new(), 1, &io)?;
    assert!(
        journal
            .append_envelope_if_tail(2, "foreign", 0, &foreign, &io)?
            .await
            .is_err()
    );
    let later = item(operation, 3, &io)?;
    assert!(
        !journal
            .append_envelope_if_tail(1, "later", 0, &later, &io)?
            .await?
    );
    let records = journal.replay(operation, 0, 10, Some(&io))?.await?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].sequence, 2);
    let ExecutionEvent::ConversationItem {
        envelope: reference,
        ..
    } = &records[1].event
    else {
        panic!("original conversation observation missing")
    };
    let bytes = journal.load(reference, Some(&io))?.await?;
    let (restored, hold) = Envelope::decode(&bytes, &io)?;
    assert_eq!(restored.intent(), operation);
    assert_eq!(restored.profile(), [7; 32]);
    let Payload::Wire { item } = restored.payload() else {
        panic!("wire item missing")
    };
    assert_eq!(item.wire().get(), "{ \"value\" : 1 }");
    drop(restored);
    drop(hold);
    Ok(())
}

fn item(intent: OperationId, value: u8, io: &PreparationIo) -> Result<Envelope> {
    // Generated opaque item bytes exercise storage, not an external protocol fixture.
    let item = WireItem::capture(
        Provenance {
            family: Family::Responses,
            provider: "codec".into(),
            model: "codec".into(),
            endpoint: "codec".into(),
            credential_scope: Some([9; 32]),
        },
        ItemKind::Message,
        RawValue::from_string(format!("{{ \"value\" : {value} }}")).unwrap(),
        Default::default(),
    )
    .unwrap();
    Envelope::new(intent, [7; 32], Payload::Wire { item }, io)
}

#[tokio::test]
async fn envelope_contributions_share_original_cut_and_reject_retry_substitution() -> Result<()> {
    use super::super::indexed_publication::Contribution;
    use acyclic_stream::{CommitCondition, OwnedBytes, StreamPath};

    let (journal, _issuer) = super::super::test_support::journal().await?;
    let operation = OperationId::new();
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(10_000_000, 16 * 1024 * 1024),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&original)?;
    let journal = journal.with_turn(operation, operation);
    journal
        .append(
            operation,
            "start",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            Some(&io),
        )?
        .await?;
    let envelope = item(operation, 1, &io)?;
    // Internal checkpoint records; no external fixture or source authority is inferred.
    let epoch = StreamPath::new("contributed/epoch")?;
    let source = StreamPath::new("contributed/source")?;
    let epoch_record = OwnedBytes::from_static(b"original epoch checkpoint");
    let source_record = OwnedBytes::from_static(b"original publication cursor");
    let contributions = [
        Contribution::Conditioned {
            condition: CommitCondition::Absent {
                path: epoch.clone(),
            },
            record: &epoch_record,
        },
        Contribution::Conditioned {
            condition: CommitCondition::Absent {
                path: source.clone(),
            },
            record: &source_record,
        },
    ];
    assert!(
        journal
            .append_contributed_conversation(1, "item", 0, &envelope, &contributions, &io)?
            .await?
    );
    assert!(
        journal
            .append_contributed_conversation(1, "item", 0, &envelope, &contributions, &io)?
            .await?
    );

    let changed_record = OwnedBytes::from_static(b"substituted epoch checkpoint");
    let substituted = [
        Contribution::Conditioned {
            condition: CommitCondition::Absent {
                path: epoch.clone(),
            },
            record: &changed_record,
        },
        Contribution::Conditioned {
            condition: CommitCondition::Absent {
                path: source.clone(),
            },
            record: &source_record,
        },
    ];
    assert!(
        journal
            .append_contributed_conversation(1, "item", 0, &envelope, &substituted, &io)?
            .await
            .is_err()
    );
    let changed_fence = [
        Contribution::Conditioned {
            condition: CommitCondition::Tail {
                path: epoch.clone(),
                expected: 0,
            },
            record: &epoch_record,
        },
        Contribution::Conditioned {
            condition: CommitCondition::Absent {
                path: source.clone(),
            },
            record: &source_record,
        },
    ];
    assert!(
        journal
            .append_contributed_conversation(1, "item", 0, &envelope, &changed_fence, &io)?
            .await
            .is_err()
    );
    assert!(
        journal
            .append_contributed_conversation(1, "item", 0, &envelope, &contributions[..1], &io)?
            .await
            .is_err()
    );
    assert!(
        journal
            .append_envelope_if_tail(1, "item", 0, &envelope, &io)?
            .await
            .is_err()
    );

    let mut work = None;
    let execution = crate::distributed::execution_path(operation, &mut work)?;
    let observation =
        crate::store::operations::one_record(&journal.stream, &execution, 1, &mut work)
            .await?
            .unwrap();
    let epoch_value = crate::store::operations::one_record(&journal.stream, &epoch, 0, &mut work)
        .await?
        .unwrap();
    let source_value = crate::store::operations::one_record(&journal.stream, &source, 0, &mut work)
        .await?
        .unwrap();
    assert_eq!(observation.commit_id, epoch_value.commit_id);
    assert_eq!(observation.commit_id, source_value.commit_id);
    assert_eq!(&*epoch_value.value, &*epoch_record);
    assert_eq!(&*source_value.value, &*source_record);

    // These now-stale extra fences lose with the journal tail unchanged.
    let next = item(operation, 2, &io)?;
    assert!(
        !journal
            .append_contributed_conversation(2, "next", 0, &next, &contributions, &io)?
            .await?
    );
    let records = journal.replay(operation, 0, 10, Some(&io))?.await?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].sequence, 2);
    Ok(())
}
