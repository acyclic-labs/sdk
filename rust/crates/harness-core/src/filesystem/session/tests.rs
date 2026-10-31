use super::*;
use acyclic_fs::WorkBudget;

#[tokio::test]
async fn retained_agent_slots_share_admission_and_stop_with_the_runtime_owner() -> Result<()> {
    let cancellation = CancellationToken::new();
    let original = NativeMaterializationWork::new(
        WorkBudget {
            peak_allocation_bytes: 8 * 1024 * 1024,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = original.live_allocation_bytes();
    let (owner, session) = SessionWork::admit(original.clone())?;
    let id = AgentId::from_bytes([1; 16]);
    let selected = session.agent(&id)?;
    let repeat = session.agent(&id)?;
    assert!(Arc::ptr_eq(&selected, &repeat));
    assert!(
        selected
            .preparation()?
            .original_native_work()
            .unwrap()
            .shares_ledger(&original)
    );

    // A real turn scope retires without closing the retained runtime admission.
    {
        let (turn, turn_cancellation) = original.invocation_view()?;
        let turn_io = PreparationIo::from_native_work(&turn)?;
        turn_cancellation.cancel();
        drop((turn_io, turn, turn_cancellation));
    }
    selected.preparation()?;

    for number in 2_u128..1026 {
        session.agent(&AgentId::from_bytes(number.to_be_bytes()))?;
    }
    let before = original.receipt();
    assert!(Arc::ptr_eq(&selected, &session.agent(&id)?));
    let after = original.receipt();
    assert_eq!(after.allocation_operations, before.allocation_operations);
    assert!(after.items_examined - before.items_examined <= 2 * (128 + 16));

    {
        let work = selected.preparation()?.original_native_work().unwrap();
        let stopped = work.cancellation_wait()?;
        futures::pin_mut!(stopped);
        assert!(futures::poll!(&mut stopped).is_pending());
        drop(owner);
        assert!(futures::poll!(&mut stopped).is_ready());
    }
    assert!(selected.preparation().is_err());
    assert!(session.agent(&id).is_err());
    // Closing one runtime does not cancel its caller or a later runtime.
    original.remaining()?;
    let (successor, next) = SessionWork::admit(original.clone())?;
    next.agent(&id)?.preparation()?;
    drop((successor, next, repeat));
    drop(selected);
    drop(session);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}
