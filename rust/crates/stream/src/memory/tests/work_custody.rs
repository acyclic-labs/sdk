use super::*;
use crate::test_meter::Meter;
use std::sync::atomic::Ordering;

#[test]
fn the_original_work_ledger_funds_its_complete_shared_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::default());
    let mut output = None;
    let physical = acyclic_test_support::measure(|| {
        output = Some(Work::new(meter.original()));
    });
    let work = output.ok_or("work constructor was not called")??;
    assert_eq!(physical.count_current, 1);
    assert_eq!(
        meter.held.load(Ordering::SeqCst),
        u64::try_from(physical.bytes_current)?,
        "shared work omitted its original physical allocation"
    );
    let clone = Arc::clone(&work);
    drop(work);
    assert_eq!(
        meter.held.load(Ordering::SeqCst),
        u64::try_from(physical.bytes_current)?
    );
    drop(clone);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}
