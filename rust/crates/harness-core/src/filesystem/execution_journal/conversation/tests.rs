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
        },
        ItemKind::Message,
        RawValue::from_string(format!("{{ \"value\" : {value} }}")).unwrap(),
        Default::default(),
    )
    .unwrap();
    Envelope::new(intent, [7; 32], Payload::Wire { item }, io)
}
