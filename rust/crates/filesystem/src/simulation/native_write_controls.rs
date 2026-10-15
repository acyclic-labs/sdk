use super::*;
use crate::materializer::NativeMaterializationWork;
use crate::storage::{HashedObject, ObjectKind};
use crate::{CancellationToken, WorkBudget};

async fn seeded_object() -> (crate::memory::MemoryObjectStore, HashedObject) {
    let memory = crate::memory::MemoryObjectStore::default();
    let object = HashedObject::new(ObjectKind::TreePage, OwnedBytes::from_static(b"body"));
    memory
        .put(
            object.object_id(),
            object.bytes().clone(),
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await
        .expect("seed memory");
    (memory, object)
}

fn budget() -> WorkBudget {
    let mut value = serde_json::to_value(WorkBudget::UNBOUNDED).expect("budget encoding");
    for field in value.as_object_mut().expect("budget fields").values_mut() {
        *field = serde_json::Value::from(1_000_000_u64);
    }
    serde_json::from_value(value).expect("finite budget")
}

#[tokio::test]
async fn native_read_trace_denial_and_cancellation_precede_interceptor_and_backend() {
    let (memory, object) = seeded_object().await;
    let simulation = Simulation::wrap(reference_backends().0, memory, SimulationOptions::default())
        .expect("bounded simulation");
    let (_, store) = simulation.stores();
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(
        WorkBudget {
            allocation_operations: 1,
            ..budget()
        },
        &cancellation,
    )
    .expect("only actual accounting constructor");
    let before = work.receipt();
    assert!(
        (store
            .read(
                object.object_id(),
                4,
                &mut crate::AuthorityWork::native(&work, &cancellation)
            )
            .await)
            .is_err()
    );
    assert_eq!(work.receipt(), before);
    assert!(simulation.trace().expect("trace").is_empty());
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        store
            .read(
                object.object_id(),
                4,
                &mut crate::AuthorityWork::native(&work, &cancelled)
            )
            .await,
        Err(ObjectStoreError::Cancelled)
    ));
    assert_eq!(work.receipt(), before);
}

#[tokio::test]
async fn native_read_partial_copy_keeps_original_capacity_until_last_byte_alias() {
    let (memory, object) = seeded_object().await;
    let simulation = Simulation::wrap(reference_backends().0, memory, SimulationOptions::default())
        .expect("bounded simulation");
    simulation
        .schedule_next(
            SimulationOperation::ObjectRead,
            SimulationFault::PartialObjectRead,
        )
        .expect("partial fault");
    let (_, store) = simulation.stores();
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original meter");
    let baseline = work.live_allocation_bytes();
    let read = (store
        .read(
            object.object_id(),
            4,
            &mut crate::AuthorityWork::native(&work, &cancellation),
        )
        .await)
        .expect("actual partial SDK copy");
    assert_eq!(read.bytes.as_ref(), b"bod");
    let alias = read.bytes.clone();
    let held = work.live_allocation_bytes();
    drop(read);
    assert_eq!(work.live_allocation_bytes(), held);
    drop(alias);
    assert!(work.live_allocation_bytes() + 3 <= held);
    drop(store);
    drop(simulation);
    assert_eq!(work.live_allocation_bytes(), baseline);
}

#[tokio::test]
async fn native_read_batch_preserves_one_real_memory_frontier_with_fault_custody() {
    let (memory, object) = seeded_object().await;
    let simulation = Simulation::wrap(reference_backends().0, memory, SimulationOptions::default())
        .expect("bounded simulation");
    simulation
        .schedule_next(
            SimulationOperation::ObjectReadMany,
            SimulationFault::CorruptObjectRead,
        )
        .expect("batch fault");
    let (_, store) = simulation.stores();
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original meter");
    let baseline = work.live_allocation_bytes();
    let requests = [ObjectReadRequest {
        object_id: object.object_id(),
        maximum_bytes: 4,
    }; 2];
    let batch = (store
        .read_many(
            &requests,
            &mut crate::AuthorityWork::native(&work, &cancellation),
        )
        .await
        .map(|(values, allocation)| (values, allocation.expect("native batch allocation"))))
    .expect("one genuine batch");
    assert_eq!(batch.0.len(), 2);
    assert_eq!(batch.0[0].bytes[0], b'b' ^ 0x80);
    assert_eq!(batch.0[1].bytes.as_ref(), b"body");
    assert_eq!(work.receipt().object_probes, 2);
    drop(batch);
    drop(store);
    drop(simulation);
    assert_eq!(work.live_allocation_bytes(), baseline);
}

#[tokio::test]
async fn native_delayed_visibility_preserves_intrinsic_alias_custody() {
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
    let baseline = work.live_allocation_bytes();
    let mut custody = work
        .claim(4, 1, crate::WorkCounters::UNCHARGED)
        .expect("payload capacity");
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(4).expect("payload buffer");
    bytes.extend_from_slice(b"body");
    let object = HashedObject::from_native_vec(
        ObjectKind::TreePage,
        &mut bytes,
        &mut custody,
        &work,
        &cancellation,
    )
    .expect("intrinsic byte owner");
    let alias = object.bytes().clone();
    let payload_live = work.live_allocation_bytes();
    let hashed = work.receipt().bytes_hashed;
    let simulation = Simulation::default();
    simulation
        .schedule_next(
            SimulationOperation::ObjectPut,
            SimulationFault::DelayObjectVisibility,
        )
        .expect("delay schedule");
    let (_, store) = simulation.stores();
    (store
        .put_hashed(
            object,
            &mut crate::AuthorityWork::native(&work, &cancellation),
        )
        .await)
        .expect("delayed admission");
    assert_eq!(work.receipt().bytes_hashed, hashed);
    assert_eq!(work.receipt().backend_write_operations, 0);
    assert_eq!(
        simulation.state.lock().expect("state").pending_object_bytes,
        4
    );
    assert!(work.live_allocation_bytes() >= payload_live);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let before = work.receipt();
    assert!(matches!(
        simulation
            .flush_object_visibility(&mut crate::AuthorityWork::native(&work, &cancelled))
            .await,
        Err(ObjectStoreError::Cancelled)
    ));
    assert_eq!(work.receipt(), before);
    assert_eq!(
        simulation.state.lock().expect("state").pending_object_bytes,
        4
    );
    drop(store);
    drop(simulation);
    assert_eq!(work.live_allocation_bytes(), payload_live);
    drop(alias);
    assert_eq!(work.live_allocation_bytes(), baseline);
}

#[tokio::test]
async fn native_put_trace_denial_precedes_interceptor_and_provider_effects() {
    let cancellation = CancellationToken::new();
    let mut denied = budget();
    denied.allocation_operations = 1;
    let work = NativeMaterializationWork::new(denied, &cancellation).expect("ledger only");
    let simulation = Simulation::default();
    simulation
        .schedule_next(
            SimulationOperation::ObjectPut,
            SimulationFault::DelayObjectVisibility,
        )
        .expect("delay schedule");
    let (_, store) = simulation.stores();
    let object = HashedObject::new(ObjectKind::TreePage, OwnedBytes::from_static(b"body"));
    let before = work.receipt();
    assert!(
        (store
            .put(
                object.object_id(),
                object.bytes().clone(),
                &mut crate::AuthorityWork::native(&work, &cancellation)
            )
            .await)
            .is_err()
    );
    assert_eq!(work.receipt(), before);
    let state = simulation.state.lock().expect("state");
    assert!(state.trace.is_empty());
    assert!(state.occurrences.iter().all(|occurrence| *occurrence == 0));
    assert!(state.pending_objects.is_empty());
    assert_eq!(state.faults.len(), 1);
    drop(state);
    assert!(
        (store
            .put_hashed(
                object,
                &mut crate::AuthorityWork::native(&work, &cancellation)
            )
            .await)
            .is_err()
    );
    assert_eq!(work.receipt(), before);
}

#[tokio::test]
async fn native_scheduled_rejection_does_not_hash_or_write_payload() {
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
    let simulation = Simulation::default();
    simulation
        .schedule_next(
            SimulationOperation::ObjectPut,
            SimulationFault::RejectBefore,
        )
        .expect("rejection schedule");
    let (_, store) = simulation.stores();
    let object = HashedObject::new(ObjectKind::TreePage, OwnedBytes::from_static(b"body"));
    assert!(matches!(
        store
            .put(
                object.object_id(),
                object.bytes().clone(),
                &mut crate::AuthorityWork::native(&work, &cancellation)
            )
            .await,
        Err(ObjectStoreError::Simulation("scheduled rejection"))
    ));
    assert_eq!(work.receipt().bytes_hashed, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    assert!(
        simulation
            .state
            .lock()
            .expect("state")
            .pending_objects
            .is_empty()
    );
}
#[tokio::test]
async fn native_write_hooks_preserve_original_cancellation_before_effects() {
    let original = CancellationToken::new();
    let argument = CancellationToken::new();
    let budget = budget();
    let work = crate::materializer::NativeMaterializationWork::new(budget, &original)
        .expect("original ledger");
    original.cancel();
    let before = work.receipt();
    let simulation = Simulation::default();
    let (_, store) = simulation.stores();
    let object = crate::storage::HashedObject::new(
        crate::storage::ObjectKind::TreePage,
        OwnedBytes::from_static(b"body"),
    );
    assert!(
        (store
            .put(
                object.object_id(),
                object.bytes().clone(),
                &mut crate::AuthorityWork::native(&work, &argument)
            )
            .await)
            .is_err()
    );
    assert!(
        (store
            .put_hashed(
                object.clone(),
                &mut crate::AuthorityWork::native(&work, &argument)
            )
            .await)
            .is_err()
    );
    let writes = [crate::storage::ObjectWrite {
        object_id: object.object_id(),
        bytes: object.bytes().clone(),
    }];
    assert!(
        (store
            .put_many(&writes, &mut crate::AuthorityWork::native(&work, &argument))
            .await)
            .is_err()
    );
    assert!(
        (store
            .flush_before_publish(
                crate::async_storage::PublicationScope::Everything,
                &mut crate::AuthorityWork::native(&work, &argument)
            )
            .await)
            .is_err()
    );
    assert!(
        simulation
            .flush_object_visibility(&mut crate::AuthorityWork::native(&work, &argument))
            .await
            .is_err()
    );
    assert_eq!(work.receipt(), before);
    let state = simulation.state.lock().expect("state");
    assert!(state.trace.is_empty());
    assert!(state.pending_objects.is_empty());
}
