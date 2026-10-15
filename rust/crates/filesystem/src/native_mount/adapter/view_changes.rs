pub(super) fn record_view_change(
    ledger: &Arc<ViewLedger>,
    change: &ViewChange<'_>,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<(), MountSourceError> {
    ledger.record(change, carrier)
}
