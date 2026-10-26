use super::*;
use crate::demand::DemandWork;
use crate::{CancellationToken, WorkBudget, WorkCounters};

fn measured_read(
    gate: &Arc<ViewGate>,
    owner: CallbackOwner,
    work: &mut DemandWork,
) -> Result<ViewReadLease, MountSourceError> {
    work.check().map_err(MountSourceError::from)?;
    let (result, budget) = {
        let (proofs, allocations, counters, mut budget) = work
            .admissions(|roots| &mut roots.view_gates)
            .map_err(MountSourceError::from)?;
        let result = gate.try_read_for_callback_measured(
            owner,
            None,
            proofs,
            allocations,
            counters,
            &mut budget,
        );
        (result, budget)
    };
    work.budget = budget;
    result?.ok_or(MountSourceError::Stale)
}

#[test]
fn native_queue_custody_preserves_non_lifo_cleanup_quota() {
    let gate = Arc::new(ViewGate::new());
    let cancellation = CancellationToken::new();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.items_examined = 100;
    let mut work = DemandWork::new(budget, &cancellation);
    let first = measured_read(&gate, CallbackOwner(1), &mut work).expect("first actual read");
    let second = measured_read(&gate, CallbackOwner(2), &mut work).expect("second actual read");
    let before = work.work.items_examined;
    let mut queued = work.take_for_native_queue();
    assert_eq!(queued.budget.items_examined, budget.items_examined - 2);
    drop(first);
    let _ = queued
        .admissions(|roots| &mut roots.view_gates)
        .expect("first actual cleanup");
    assert_eq!(queued.work.items_examined, before + 1);
    assert_eq!(queued.budget.items_examined, budget.items_examined - 1);
    drop(second);
    let _ = queued
        .admissions(|roots| &mut roots.view_gates)
        .expect("second actual cleanup");
    assert_eq!(queued.work.items_examined, before + 2);
    assert_eq!(queued.budget.items_examined, budget.items_examined);
    queued
        .record(WorkCounters::UNCHARGED)
        .expect("same original meter finishes");
    assert_eq!(gate.state.lock().expect("gate state").readers, 0);
}

#[test]
fn denied_registry_allocation_and_copy_do_not_grant_reads() {
    let gate = Arc::new(ViewGate::new());
    let cancellation = CancellationToken::new();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.allocation_operations = 0;
    let mut work = DemandWork::new(budget, &cancellation);
    assert!(matches!(
        measured_read(&gate, CallbackOwner(1), &mut work),
        Err(MountSourceError::Work(_)),
    ));
    assert_eq!(work.work.allocation_operations, 0);
    assert_eq!(
        gate.state
            .lock()
            .expect("gate state")
            .reader_slots
            .capacity(),
        0
    );
    assert_eq!(gate.state.lock().expect("gate state").readers, 0);

    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_copied = 0;
    let mut work = DemandWork::new(budget, &cancellation);
    let first = measured_read(&gate, CallbackOwner(1), &mut work).expect("initial registry");
    let allocations_before = work.work.allocation_operations;
    let generation = gate.reader_generation.load(Ordering::Acquire);
    assert!(matches!(
        measured_read(&gate, CallbackOwner(2), &mut work),
        Err(MountSourceError::Work(_)),
    ));
    assert_eq!(work.work.allocation_operations, allocations_before);
    assert_eq!(work.work.bytes_copied, 0);
    assert_eq!(gate.reader_generation.load(Ordering::Acquire), generation);
    assert_eq!(gate.state.lock().expect("gate state").readers, 1);
    drop(first);
    work.record(WorkCounters::UNCHARGED)
        .expect("only admitted read releases");
}

#[tokio::test]
async fn external_registry_growth_reconciles_original_backing_proof() {
    let gate = Arc::new(ViewGate::new());
    let cancellation = CancellationToken::new();
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let first = measured_read(&gate, CallbackOwner(1), &mut work).expect("measured original read");
    assert!(work.live_allocation_bytes() > 0);
    let generation = gate.reader_generation.load(Ordering::Acquire);
    let independent = gate
        .read(None)
        .await
        .expect("actual independent reader grows registry");
    assert!(gate.reader_generation.load(Ordering::Acquire) > generation);
    let _ = work
        .admissions(|roots| &mut roots.view_gates)
        .expect("same root reconciles freed original backing");
    assert_eq!(work.live_allocation_bytes(), 0);
    drop(independent);
    let before = work.work.items_examined;
    drop(first);
    work.record(WorkCounters::UNCHARGED)
        .expect("original physical read cleanup");
    let receipt = work.receipt();
    assert_eq!(receipt.items_examined, before + 1);
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

#[test]
fn independent_box_payloads_release_non_lifo_without_losing_quota() {
    let gate = Arc::new(ViewGate::new());
    let cancellation = CancellationToken::new();
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let bytes = std::mem::size_of::<ViewReadLease>() as u64;
    let mut first = measured_read(&gate, CallbackOwner(1), &mut work).expect("first read");
    work.claim_allocation(
        bytes,
        u64::from((bytes) != 0),
        crate::WorkCounters::UNCHARGED,
    )
    .expect("first original Box");
    first
        .attach_owned_payload(bytes)
        .expect("first payload custody");
    let first = Box::new(first);
    let mut second = measured_read(&gate, CallbackOwner(1), &mut work).expect("nested read");
    work.claim_allocation(
        bytes,
        u64::from((bytes) != 0),
        crate::WorkCounters::UNCHARGED,
    )
    .expect("second original Box");
    second
        .attach_owned_payload(bytes)
        .expect("second payload custody");
    let second = Box::new(second);
    let live = work.live_allocation_bytes();
    let before = work.work.items_examined;
    drop(first);
    work.record(WorkCounters::default())
        .expect("first physical cleanup");
    assert_eq!(work.work.items_examined, before + 1);
    assert_eq!(work.live_allocation_bytes(), live - bytes);
    assert_eq!(work.budget.items_examined, u64::MAX - 1);
    drop(second);
    work.record(WorkCounters::default())
        .expect("second physical cleanup");
    assert_eq!(work.work.items_examined, before + 2);
    assert_eq!(work.live_allocation_bytes(), live - bytes * 2);
    assert_eq!(work.budget.items_examined, u64::MAX);
    // Finished leases release their payloads; the original meter still owns
    // the physical registry which records those finished releases.
    let registry_bytes = {
        let state = gate.state.lock().expect("owned reader registry");
        assert_eq!(state.readers, 0);
        (state.reader_slots.capacity() * std::mem::size_of::<Option<ReadSlot>>()) as u64
    };
    assert_eq!(work.live_allocation_bytes(), registry_bytes);
    drop(work);
    assert_eq!(
        gate.state
            .lock()
            .expect("retired reader registry")
            .reader_slots
            .capacity(),
        0
    );
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
        "original reader still owns its lane"
    );
    drop(reader);
    work.record(WorkCounters::UNCHARGED)
        .expect("actual callback cleanup");
    drop(next.await.expect("later original writer"));
    assert_eq!(work.budget.items_examined, u64::MAX);
}

#[tokio::test]
async fn foreign_existing_registry_capacity_remains_borrowed() {
    let gate = Arc::new(ViewGate::new());
    let first = gate
        .read_for_callback(CallbackOwner(1), None)
        .await
        .expect("existing first reader");
    let second = gate
        .read_for_callback(CallbackOwner(2), None)
        .await
        .expect("existing second reader");
    let spare = gate
        .read_for_callback(CallbackOwner(3), None)
        .await
        .expect("existing spare reader");
    drop(spare);
    let cancellation = CancellationToken::new();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.allocation_operations = 0;
    budget.peak_allocation_bytes = 0;
    let mut work = DemandWork::new(budget, &cancellation);
    let reader =
        measured_read(&gate, CallbackOwner(1), &mut work).expect("reuse borrowed original slots");
    assert_eq!(work.work.allocation_operations, 0);
    assert_eq!(work.live_allocation_bytes(), 0);
    drop(reader);
    work.record(WorkCounters::UNCHARGED)
        .expect("only actual lease cleanup");
    drop(second);
    drop(first);
}

#[tokio::test]
async fn shared_original_read_and_write_gate_preserve_custody_and_tokens() {
    let gate = Arc::new(ViewGate::new());
    let cancellation = CancellationToken::new();
    let meter = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .expect("original native work");
    let mut carrier = super::super::adapter::NativeDemandCarrier::Native(&meter);
    let lease = gate
        .read_for_callback_with_carrier(CallbackOwner(1), None, &mut carrier, &cancellation, None)
        .await
        .expect("original shared read");
    let before = meter.receipt().items_examined;
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
        u64::MAX
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
