use super::*;
use acyclic_fs::{CancellationToken, WorkBudget, WorkCounters};

fn control_budget() -> WorkBudget {
    WorkBudget {
        allocation_operations: 1,
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes(),
        ..WorkBudget::default()
    }
}

fn completion_budget(maximum: WorkCounters, pool: u64) -> WorkBudget {
    let mut budget = control_budget().checked_add(maximum).unwrap();
    budget.peak_allocation_bytes += pool;
    budget
}

#[test]
fn sdk_output_adoption_keeps_original_ownership() {
    let cancellation = CancellationToken::new();
    let mut budget = completion_budget(
        WorkCounters {
            bytes_copied: 8,
            ..Default::default()
        },
        128,
    );
    budget.allocation_operations += 1;
    let original = NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let foreign = NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&original).unwrap(),
    );
    for producer in [&original, &foreign] {
        let before = producer.live_allocation_bytes();
        let meter = producer.runtime_meter();
        let mut allocation = acyclic_native_runtime::ReadAllocation::new(&meter, 8).unwrap();
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(8).unwrap();
        allocation.reconcile_capacity(bytes.capacity()).unwrap();
        meter.admit_codec(0, 8, 0).unwrap();
        bytes.extend_from_slice(b"original");
        meter.record_codec(0, 8, 0).unwrap();
        let output = acyclic_stream::NativeStreamOutput::new(bytes, allocation);
        let result = adopt_native_sdk_output(&original, output, &mut Some(&mut io));
        if producer.shares_allocation_context(&original) {
            let (bytes, held) = result.unwrap();
            assert_eq!(bytes, b"original");
            drop(bytes);
            io.release_owned(held).unwrap();
        } else {
            assert!(matches!(
                result,
                Err(Error::NativeWork {
                    cause: NativeTreeMaterializationError::InvalidLayout,
                    ..
                })
            ));
        }
        assert_eq!(producer.live_allocation_bytes(), before);
    }
}

#[test]
fn accepted_completion_uses_only_reserved_credit() {
    let cancellation = CancellationToken::new();
    let maximum = WorkCounters {
        bytes_copied: 4,
        ..Default::default()
    };
    let work =
        NativeMaterializationWork::new(completion_budget(maximum, 16), &cancellation).unwrap();
    let token = work.reserve_work(maximum).unwrap();
    let pool = work.claim(16, 0, WorkCounters::default()).unwrap();
    assert!(
        work.charge(WorkCounters {
            bytes_copied: 1,
            ..Default::default()
        })
        .is_err()
    );
    let mut io = AcceptedCompletionIo::new(&work, token, pool).unwrap();
    io.copy_bytes(3).unwrap();
    let prefix = work.receipt();
    assert!(io.admit_copy(2).is_err());
    assert_eq!(work.receipt(), prefix);
    assert!(io.copy_bytes(2).is_err());
    assert_eq!(work.receipt().bytes_copied, 5);
    assert_eq!(io.completion_work().unwrap(), work.journal_work().unwrap());
    let pool = io.into_custody();
    assert_eq!(pool.bytes(), 16);
    assert!(pool.work().shares_allocation_context(&work));
    drop(pool);
    assert!(
        work.charge(WorkCounters {
            bytes_copied: 1,
            ..Default::default()
        })
        .is_err()
    );
    assert_eq!(work.receipt().bytes_copied, 5);
}

#[test]
fn accepted_completion_rejects_foreign_ownership() {
    let cancellation = CancellationToken::new();
    let maximum = WorkCounters {
        backend_write_operations: 1,
        ..Default::default()
    };
    let budget = completion_budget(maximum, 16);
    let original = NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let foreign = NativeMaterializationWork::new(budget, &cancellation).unwrap();
    for (token_source, pool_source) in [(&foreign, &original), (&original, &foreign)] {
        let token = token_source.reserve_work(maximum).unwrap();
        let pool = pool_source.claim(16, 0, WorkCounters::default()).unwrap();
        let Error::NativeWork {
            cause: NativeTreeMaterializationError::InvalidLayout,
            custody: Some(pool),
        } = AcceptedCompletionIo::new(&original, token, pool)
            .err()
            .unwrap()
        else {
            panic!("foreign ownership must be returned");
        };
        assert!(pool.work().shares_allocation_context(pool_source));
        assert_eq!(pool.bytes(), 16);
    }
    let token = original.reserve_work(maximum).unwrap();
    let pool = original.claim(16, 0, WorkCounters::default()).unwrap();
    let mut io = AcceptedCompletionIo::new(&original, token, pool).unwrap();
    let guard = foreign.claim(16, 0, WorkCounters::default()).unwrap();
    let Error::NativeWork {
        cause: NativeTreeMaterializationError::InvalidLayout,
        custody: Some(guard),
    } = io.absorb_native_custody(guard).unwrap_err()
    else {
        panic!("foreign custody must be returned");
    };
    assert!(guard.work().shares_allocation_context(&foreign));
}

#[test]
fn accepted_sdk_output_cannot_widen_the_closed_pool() {
    let cancellation = CancellationToken::new();
    let mut budget = completion_budget(WorkCounters::default(), 32);
    budget.allocation_operations += 2;
    let work = NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let token = work.reserve_work(WorkCounters::default()).unwrap();
    let pool = work.claim(8, 0, WorkCounters::default()).unwrap();
    let mut oversized_io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&work).unwrap(),
    );
    let mut oversized = Vec::<u8>::new();
    crate::store::history::reserve(&mut oversized, 16, &mut oversized_io).unwrap();
    let oversized_guard = oversized_io.into_custody().unwrap();
    let mut bounded_io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&work).unwrap(),
    );
    let mut bounded = Vec::<u8>::new();
    crate::store::history::reserve(&mut bounded, 8, &mut bounded_io).unwrap();
    let bounded_guard = bounded_io.into_custody().unwrap();
    let mut io = AcceptedCompletionIo::new(&work, token, pool).unwrap();
    cancellation.cancel();
    let Error::NativeWork {
        custody: Some(guard),
        ..
    } = io.absorb_native_custody(oversized_guard).unwrap_err()
    else {
        panic!("oversized output must keep its guard");
    };
    assert!(guard.work().shares_allocation_context(&work));
    drop(oversized);
    drop(guard);
    io.absorb_native_custody(bounded_guard).unwrap();
    assert!(io.admit_owned(1).is_err());
    drop(bounded);
    io.release_owned(8).unwrap();
    assert_eq!(io.remaining().unwrap().peak_allocation_bytes, 8);
    assert!(io.retain_owned(9).is_err());
}

#[test]
fn accepted_receiver_keeps_completed_work_when_token_or_pool_is_exceeded() {
    for (allowed_bytes, pool_bytes) in [(3, 8), (5, 4)] {
        let cancellation = CancellationToken::new();
        let maximum = WorkCounters {
            authority_records_read: 1,
            authority_bytes_read: allowed_bytes,
            items_examined: 1,
            ..Default::default()
        };
        let work =
            NativeMaterializationWork::new(completion_budget(maximum, pool_bytes), &cancellation)
                .unwrap();
        let token = work.reserve_work(maximum).unwrap();
        let pool = work.claim(pool_bytes, 0, WorkCounters::default()).unwrap();
        let mut io = AcceptedCompletionIo::new(&work, token, pool).unwrap();
        let prefix = work.receipt();
        if allowed_bytes == 3 {
            cancellation.cancel();
        }
        assert!(io.receive_record(5).is_err());
        assert_eq!(
            work.receipt(),
            prefix
                .checked_add(WorkCounters {
                    authority_records_read: 1,
                    authority_bytes_read: 5,
                    items_examined: 1,
                    ..Default::default()
                })
                .unwrap()
        );
        if pool_bytes == 4 {
            assert!(io.release_owned(5).is_err());
            io.retain_owned(4).unwrap();
            assert!(io.retain_owned(1).is_err());
            assert!(io.take_native_custody(5).is_err());
        } else {
            assert_eq!(io.remaining_work(), WorkCounters::default());
        }
    }
}

#[test]
fn virtual_release_does_not_free_a_live_buffer() {
    let cancellation = CancellationToken::new();
    let mut budget = completion_budget(WorkCounters::default(), 16);
    budget.allocation_operations += 1;
    let work = NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let token = work.reserve_work(WorkCounters::default()).unwrap();
    let mut preparation = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&work).unwrap(),
    );
    let mut dto = Vec::<u8>::new();
    crate::store::history::reserve(&mut dto, 16, &mut preparation).unwrap();
    let mut io =
        AcceptedCompletionIo::new(&work, token, preparation.into_custody().unwrap()).unwrap();
    io.adopt_preheld(16).unwrap();
    io.release_owned(16).unwrap();
    assert!(work.claim(1, 0, WorkCounters::default()).is_err());
    io.retain_owned(16).unwrap();
    let guard = io.take_native_custody(16).unwrap();
    assert!(guard.work().shares_allocation_context(&work));
    drop(io);
    assert!(work.claim(1, 0, WorkCounters::default()).is_err());
    drop(dto);
    drop(guard);
    work.claim(16, 0, WorkCounters::default()).unwrap();
}
