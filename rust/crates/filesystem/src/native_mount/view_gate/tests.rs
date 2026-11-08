use super::*;
use crate::demand::DemandWork;
use crate::{CancellationToken, WorkBudget, WorkCounters};

fn measured_read(
    gate: &Arc<ViewGate>,
    owner: CallbackOwner,
    work: &mut DemandWork,
) -> Result<ViewReadLease, MountSourceError> {
    let mut carrier = super::super::adapter::NativeDemandCarrier::Borrowed(work);
    gate.try_read_as(ViewReaderId::Callback(owner), None, &mut carrier)?
        .ok_or(MountSourceError::Stale)
}

#[test]
fn native_queue_custody_preserves_non_lifo_cleanup_quota() {
    let gate = Arc::new(ViewGate::new());
    let cancellation = CancellationToken::new();
    let budget = WorkBudget::UNBOUNDED;
    let mut work = DemandWork::new(budget, &cancellation);
    let first = measured_read(&gate, CallbackOwner(1), &mut work).expect("first actual read");
    let second = measured_read(&gate, CallbackOwner(2), &mut work).expect("second actual read");
    let before = work.work.items_examined;
    let mut queued = work.take_for_native_queue();
    assert_eq!(queued.budget.items_examined, budget.items_examined - 2);
    drop(first);
    queued
        .record(WorkCounters::UNCHARGED)
        .expect("first actual cleanup");
    assert_eq!(queued.work.items_examined, before + 1);
    assert_eq!(queued.budget.items_examined, budget.items_examined - 1);
    drop(second);
    queued
        .record(WorkCounters::UNCHARGED)
        .expect("second actual cleanup");
    assert_eq!(queued.work.items_examined, before + 2);
    assert_eq!(queued.budget.items_examined, budget.items_examined);
    queued
        .record(WorkCounters::UNCHARGED)
        .expect("same original meter finishes");
    assert_eq!(gate.state.lock().expect("gate state").readers, 0);
}

#[tokio::test]
async fn measured_reentrant_reads_do_not_wait_for_queued_writer() -> Result<(), MountSourceError> {
    let gate = Arc::new(ViewGate::new());
    let cancellation = CancellationToken::new();
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let owner = ViewGate::callback_owner()?;
    let outer = measured_read(&gate, owner, &mut work).expect("outer callback");
    let mut writer = std::pin::pin!(gate.write());
    assert!(futures::poll!(writer.as_mut()).is_pending());
    let nested = measured_read(&gate, owner, &mut work).expect("same original callback reenters");
    assert_eq!(gate.state.lock().expect("gate state").readers, 2);
    drop(outer);
    drop(nested);
    drop(writer.await);
    let before = work.work.items_examined;
    work.record(WorkCounters::UNCHARGED)
        .expect("two actual callback releases");
    let receipt = work.receipt();
    assert_eq!(receipt.items_examined, before + 2);
    Ok(())
}

#[tokio::test]
async fn measured_callback_wait_preserves_original_lifetime_cancellation() {
    let gate = Arc::new(ViewGate::new());
    let writer = gate.write().await;
    let cancellation = CancellationToken::new();
    let lifetime = CancellationToken::new();
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
    {
        let mut carrier = super::super::adapter::NativeDemandCarrier::Borrowed(&mut work);
        let mut read = std::pin::pin!(gate.read_for_callback_with_carrier(
            CallbackOwner(1),
            None,
            &mut carrier,
            &cancellation,
            Some(&lifetime),
        ));
        assert!(futures::poll!(read.as_mut()).is_pending());
        lifetime.cancel();
        assert!(matches!(read.await, Err(MountSourceError::Stale)));
    }
    assert!(!cancellation.is_cancelled());
    drop(writer);
    work.record(WorkCounters::default())
        .expect("original wait cleanup");
    assert_eq!(gate.state.lock().expect("gate state").readers, 0);
}

#[tokio::test]
async fn original_mutation_phase_allows_new_sdk_readers_but_not_later_writers() {
    let gate = Arc::new(ViewGate::new());
    let cancellation = CancellationToken::new();
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let writer = gate.write_stable(None).await.expect("original writer");
    let phase = writer.into_mutation_phase();
    assert_eq!(phase.generation, ViewGate::INITIAL_GENERATION);
    assert!(!gate.state.lock().expect("original state").writer);
    let mut next = std::pin::pin!(gate.write_stable(None));
    assert!(futures::poll!(next.as_mut()).is_pending());
    let mut carrier = super::super::adapter::NativeDemandCarrier::Borrowed(&mut work);
    let reader = gate
        .read_for_callback_with_carrier(
            CallbackOwner(10),
            Some(phase.generation),
            &mut carrier,
            &cancellation,
            None,
        )
        .await
        .expect("new SDK callback reads original frozen view");
    assert!(futures::poll!(next.as_mut()).is_pending());
    drop(phase);
    assert!(
        futures::poll!(next.as_mut()).is_pending(),
        "original reader still owns its view"
    );
    drop(reader);
    work.record(WorkCounters::UNCHARGED)
        .expect("actual callback cleanup");
    drop(next.await.expect("later original writer"));
    assert_eq!(work.budget.items_examined, u64::MAX);
}

#[tokio::test]
async fn shared_original_read_and_write_gate_preserve_custody_and_tokens() {
    let gate = Arc::new(ViewGate::new());
    let cancellation = CancellationToken::new();
    let meter =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
            .expect("original native work");
    let baseline = meter.live_allocation_bytes();
    let mut carrier = super::super::adapter::NativeDemandCarrier::Native(&meter);
    let lease = gate
        .read_for_callback_with_carrier(CallbackOwner(1), None, &mut carrier, &cancellation, None)
        .await
        .expect("original shared read");
    let before = meter.receipt().items_examined;
    let mut denied = DemandWork::new(
        WorkBudget {
            peak_allocation_bytes: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    assert!(measured_read(&gate, CallbackOwner(2), &mut denied).is_err());
    assert_eq!(gate.state.lock().expect("original gate").readers, 1);
    let lifetime = CancellationToken::new();
    {
        let mut carrier = super::super::adapter::NativeDemandCarrier::Native(&meter);
        let mut writer = std::pin::pin!(gate.write_stable_with_carrier(
            Some(ViewGate::INITIAL_GENERATION),
            &mut carrier,
            &cancellation,
            Some(&lifetime),
        ));
        assert!(futures::poll!(writer.as_mut()).is_pending());
        assert!(
            meter.state.0.try_lock().is_ok(),
            "pending writer retained the original work mutex"
        );
        lifetime.cancel();
        assert!(matches!(writer.await, Err(MountSourceError::Stale)));
    }
    assert!(!cancellation.is_cancelled());
    assert_eq!(gate.state.lock().expect("original gate").waiting_writers, 0);
    drop(lease);
    meter
        .with_original_leaf(|work| work.record(WorkCounters::default()))
        .expect("physical original cleanup");
    assert!(meter.receipt().items_examined > before);
    assert_eq!(
        meter.with_original_leaf(|work| work.budget.items_examined),
        crate::performance::BOUNDARY_BUDGET.items_examined
    );
    assert_eq!(
        meter.live_allocation_bytes(),
        baseline,
        "read ownership retained historical credit"
    );
    let mut carrier = super::super::adapter::NativeDemandCarrier::Native(&meter);
    let writer = gate
        .write_stable_with_carrier(
            Some(ViewGate::INITIAL_GENERATION),
            &mut carrier,
            &cancellation,
            None,
        )
        .await
        .expect("same original exclusive gate");
    assert!(gate.state.lock().expect("original gate").writer);
    drop(writer);
    assert!(!gate.state.lock().expect("original gate").writer);
}
