pub(super) fn record_view_change(
    ledger: &Arc<ViewLedger>,
    change: &ViewChange<'_>,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<(), MountSourceError> {
    if carrier.original_native_work().is_some() {
        return ledger.record(
            change,
            carrier,
            &mut super::view_ledger::ViewLedgerAdmission::default(),
        );
    }
    let (slot, mut admission) = carrier.with_work(|work| ledger.take_for_work(work))?;
    let recorded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ledger.record(change, carrier, &mut admission)
    }));
    carrier.with_work(|work| work.restore_view_ledger_admission(slot, admission));
    match recorded {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
