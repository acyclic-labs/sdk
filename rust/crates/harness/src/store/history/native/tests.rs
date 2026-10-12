use super::*;
use acyclic_fs::{CancellationToken, WorkBudget, WorkError};

fn control_budget() -> WorkBudget {
    WorkBudget {
        allocation_operations: 1,
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes(),
        ..WorkBudget::default()
    }
}

#[test]
fn native_string_copy_and_vector_growth_denials_preserve_actual_prefix() {
    let cancellation = CancellationToken::new();
    let mut budget = control_budget();
    budget.peak_allocation_bytes += 128;
    budget.bytes_copied = 32;
    let work =
        NativeMaterializationWork::new(budget, &cancellation).expect("original accounting state");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&work).expect("original observer"),
    );
    let before = work.receipt();
    assert!(matches!(
        io.clone_text("original payload"),
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "allocation_operations",
                ..
            }),
            custody: None,
        })
    ));
    assert_eq!(work.receipt().bytes_copied, before.bytes_copied);

    let mut bytes = Vec::new();
    assert!(matches!(
        crate::store::history::reserve(&mut bytes, 16, &mut io),
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "allocation_operations",
                ..
            }),
            custody: None,
        })
    ));
    assert_eq!(bytes.capacity(), 0);

    assert_eq!(
        work.receipt().allocation_operations,
        before.allocation_operations
    );
    assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
}

#[test]
fn sdk_output_adoption_rejects_foreign_custody_and_releases_original_bytes() {
    let cancellation = CancellationToken::new();
    let mut budget = control_budget();
    budget.allocation_operations += 1;
    budget.peak_allocation_bytes += 128;
    budget.bytes_copied = 8;
    let original = NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
    let foreign = NativeMaterializationWork::new(budget, &cancellation).expect("foreign ledger");
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
            let (bytes, retained) = result.unwrap();
            assert_eq!(bytes, b"original");
            assert!(producer.live_allocation_bytes() > before);
            drop(bytes);
            io.release_owned(retained).unwrap();
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

fn completion_budget(maximum: WorkCounters, pool: u64) -> WorkBudget {
    let mut budget = control_budget()
        .checked_add(maximum)
        .expect("finite test grant");
    budget.peak_allocation_bytes += pool;
    budget
}

#[test]
fn accepted_completion_token_cannot_spend_fresh_original_allowance() {
    let cancellation = CancellationToken::new();
    let maximum = WorkCounters {
        bytes_copied: 4,
        ..WorkCounters::default()
    };
    let work = NativeMaterializationWork::new(completion_budget(maximum, 16), &cancellation)
        .expect("original ledger");
    let reservation = work.reserve_work(maximum).expect("owned copy credit");
    let pool = work
        .claim(16, 0, acyclic_fs::WorkCounters::default())
        .expect("preheld pool");
    assert!(
        matches!(
            work.charge(WorkCounters {
                bytes_copied: 1,
                ..WorkCounters::default()
            }),
            Err(NativeTreeMaterializationError::Work(
                WorkError::BudgetExceeded {
                    counter: "bytes_copied",
                    ..
                }
            ))
        ),
        "reserved work is not available to ordinary callers"
    );
    let mut io = AcceptedCompletionIo::new(&work, reservation, pool).expect("accepted observer");
    io.copy_bytes(3).expect("actual copy prefix");
    let prefix = work.receipt();
    assert!(matches!(
        io.admit_copy(2),
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "bytes_copied",
                ..
            }),
            custody: None,
        })
    ));
    assert!(matches!(
        io.copy_bytes(2),
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "bytes_copied",
                ..
            }),
            custody: None,
        })
    ));
    assert_eq!(io.remaining_work().bytes_copied, 1);
    assert_eq!(
        work.receipt(),
        prefix,
        "denial cannot manufacture a completed copy"
    );
    drop(io);
    work.charge(WorkCounters {
        bytes_copied: 1,
        ..WorkCounters::default()
    })
    .expect("unspent original token credit returns on drop");
    assert_eq!(work.receipt().bytes_copied, 4);
}

#[test]
fn accepted_completion_rejects_foreign_tokens_and_preserves_foreign_guard() {
    let cancellation = CancellationToken::new();
    let maximum = WorkCounters {
        backend_write_operations: 1,
        ..WorkCounters::default()
    };
    let budget = completion_budget(maximum, 16);
    let work = NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
    let foreign = NativeMaterializationWork::new(budget, &cancellation).expect("foreign ledger");
    let foreign_token = foreign.reserve_work(maximum).expect("foreign owned credit");
    let pool = work
        .claim(16, 0, acyclic_fs::WorkCounters::default())
        .expect("original guard");
    let prefix = work.receipt();
    let rejection = AcceptedCompletionIo::new(&work, foreign_token, pool)
        .err()
        .expect("foreign token denied");
    let Error::NativeWork {
        cause: NativeTreeMaterializationError::InvalidLayout,
        custody: Some(pool),
    } = rejection
    else {
        panic!("rejection preserves the genuine typed guard");
    };
    assert!(pool.work().shares_allocation_context(&work));
    assert_eq!(pool.bytes(), 16);
    assert_eq!(work.receipt(), prefix);
    drop(pool);

    let token = work.reserve_work(maximum).expect("original token");
    let foreign_pool = foreign
        .claim(16, 0, acyclic_fs::WorkCounters::default())
        .expect("foreign guard");
    let rejection = AcceptedCompletionIo::new(&work, token, foreign_pool)
        .err()
        .expect("foreign guard denied");
    let Error::NativeWork {
        cause: NativeTreeMaterializationError::InvalidLayout,
        custody: Some(pool),
    } = rejection
    else {
        panic!("foreign guard must not be retired or adopted");
    };
    assert!(pool.work().shares_allocation_context(&foreign));
    assert_eq!(pool.bytes(), 16);
    drop(pool);

    let token = work.reserve_work(maximum).expect("refunded original token");
    let pool = work
        .claim(16, 0, acyclic_fs::WorkCounters::default())
        .expect("original pool");
    let mut io = AcceptedCompletionIo::new(&work, token, pool).expect("accepted observer");
    let foreign_guard = foreign
        .claim(16, 0, acyclic_fs::WorkCounters::default())
        .expect("foreign buffer guard");
    let rejection = io
        .absorb_native_custody(foreign_guard)
        .expect_err("closed pool denies adoption");
    let Error::NativeWork {
        cause: NativeTreeMaterializationError::InvalidLayout,
        custody: Some(guard),
    } = rejection
    else {
        panic!("foreign custody remains with its real source");
    };
    assert!(guard.work().shares_allocation_context(&foreign));

    drop(guard);
}

#[test]
fn accepted_sdk_guard_overprefix_keeps_source_guard_and_never_widens_closed_pool() {
    let cancellation = CancellationToken::new();
    let mut budget = completion_budget(WorkCounters::default(), 32);
    budget.allocation_operations += 2;
    let work = NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
    let token = work
        .reserve_work(WorkCounters::default())
        .expect("pre-action zero token");
    let pool = work
        .claim(8, 0, acyclic_fs::WorkCounters::default())
        .expect("closed eight-byte completion pool");
    let mut oversized_preparation = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&work)
            .expect("first original SDK allocator"),
    );
    let mut oversized = Vec::new();
    crate::store::history::reserve(&mut oversized, 16, &mut oversized_preparation)
        .expect("real oversized SDK backing");
    let oversized_guard = oversized_preparation
        .into_custody()
        .expect("native custody");
    let mut bounded_preparation = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&work)
            .expect("second original SDK allocator"),
    );
    let mut bounded = Vec::new();
    crate::store::history::reserve(&mut bounded, 8, &mut bounded_preparation)
        .expect("real bounded SDK backing");
    let bounded_guard = bounded_preparation.into_custody().expect("native custody");
    let prefix = work.receipt();
    let mut io = AcceptedCompletionIo::new(&work, token, pool).expect("accepted observer");
    cancellation.cancel();
    let rejection = io
        .absorb_native_custody(oversized_guard)
        .expect_err("closed prefix rejects oversized SDK result");

    assert_eq!(
        work.receipt(),
        prefix,
        "rejected transfer cannot claim or release original bytes"
    );
    drop(oversized);
    let Error::NativeWork {
        cause:
            NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes",
                observed: 16,
                maximum: 8,
            }),
        custody: Some(guard),
    } = rejection
    else {
        panic!("oversized DTO retains its real source guard and static typed bound");
    };
    assert!(guard.work().shares_allocation_context(&work));
    assert_eq!(guard.bytes(), 16);
    drop(guard);
    io.absorb_native_custody(bounded_guard)
        .expect("already-held bounded SDK transfer");

    assert!(matches!(
        io.admit_owned(1),
        Err(Error::NativeBoundary { .. })
    ));
    drop(bounded);
    io.release_owned(8)
        .expect("retire virtual SDK DTO after backing drops");
    assert_eq!(
        io.remaining()
            .expect("closed pool after release")
            .peak_allocation_bytes,
        8
    );
    assert!(
        matches!(io.retain_owned(9), Err(Error::NativeBoundary { .. })),
        "retiring a merged SDK result must not enlarge later recovery allowance"
    );
    assert_eq!(work.receipt(), prefix);
}

#[test]
fn accepted_receiver_over_token_preserves_the_actual_raw_prefix_before_denial() {
    let cancellation = CancellationToken::new();
    let maximum = WorkCounters {
        authority_records_read: 1,
        authority_bytes_read: 3,
        items_examined: 1,
        ..WorkCounters::default()
    };
    let work = NativeMaterializationWork::new(completion_budget(maximum, 8), &cancellation)
        .expect("original ledger");
    let token = work
        .reserve_work(maximum)
        .expect("pre-action receiver credit");
    let pool = work
        .claim(8, 0, acyclic_fs::WorkCounters::default())
        .expect("pool can cover the actual raw record");
    let prefix = work.receipt();
    let mut io = AcceptedCompletionIo::new(&work, token, pool).expect("accepted observer");
    cancellation.cancel();
    assert!(
        matches!(
            io.receive_record(5),
            Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                    counter: "authority_bytes_read",
                    observed: 5,
                    maximum: 3,
                }),
                custody: None,
            })
        ),
        "receiver credit denial keeps its exact original counter"
    );
    assert_eq!(
        work.receipt(),
        prefix
            .checked_add(WorkCounters {
                authority_records_read: 1,
                authority_bytes_read: 5,
                items_examined: 1,
                ..WorkCounters::default()
            })
            .expect("actual completed receiver prefix")
    );
    assert_eq!(io.remaining_work(), WorkCounters::default());
}

#[test]
fn accepted_completion_overpool_records_receiver_prefix_before_static_denial() {
    let cancellation = CancellationToken::new();
    let maximum = WorkCounters {
        authority_records_read: 1,
        authority_bytes_read: 5,
        items_examined: 1,
        ..WorkCounters::default()
    };
    let work = NativeMaterializationWork::new(completion_budget(maximum, 4), &cancellation)
        .expect("original ledger");
    let token = work
        .reserve_work(maximum)
        .expect("owned raw receiver credit");
    let pool = work
        .claim(4, 0, acyclic_fs::WorkCounters::default())
        .expect("closed four-byte pool");
    let prefix = work.receipt();
    let mut io = AcceptedCompletionIo::new(&work, token, pool).expect("accepted observer");
    assert!(matches!(
        io.receive_record(5),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "accepted completion exceeds its preheld allocation pool",
        })
    ));
    assert_eq!(
        work.receipt(),
        prefix
            .checked_add(maximum)
            .expect("actual malformed prefix")
    );

    assert!(matches!(
        io.release_owned(5),
        Err(Error::NativeBoundary { .. })
    ));
    io.retain_owned(4).expect("existing pool remains usable");
    assert!(matches!(
        io.retain_owned(1),
        Err(Error::NativeBoundary { .. })
    ));
    assert!(matches!(
        io.take_native_custody(5),
        Err(Error::NativeBoundary { .. })
    ));
}

#[test]
fn accepted_virtual_release_keeps_actual_buffer_claim_until_genuine_guard_transfer() {
    let cancellation = CancellationToken::new();
    let maximum = WorkCounters::default();
    let mut budget = completion_budget(maximum, 16);
    budget.allocation_operations += 1;
    let work = NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
    let token = work.reserve_work(maximum).expect("zero additive token");
    let mut preparation = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&work)
            .expect("original pre-action allocator"),
    );
    let mut dto = Vec::new();
    crate::store::history::reserve(&mut dto, 16, &mut preparation)
        .expect("actual original-ledger buffer allocation");
    assert_eq!(dto.capacity(), 16);
    let pool = preparation.into_custody().expect("native custody");
    let mut io = AcceptedCompletionIo::new(&work, token, pool).expect("accepted observer");
    let prefix = work.receipt();
    io.adopt_preheld(16)
        .expect("already-paid request ownership");
    io.release_owned(16)
        .expect("virtual SDK request retirement");

    assert!(
        matches!(
            work.claim(1, 0, acyclic_fs::WorkCounters::default()),
            Err(NativeTreeMaterializationError::Work(
                WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    ..
                }
            ))
        ),
        "virtual release cannot fund another live allocation"
    );
    io.retain_owned(16)
        .expect("returned DTO draws the same pool");
    let dto_guard = io
        .take_native_custody(16)
        .expect("transfer original real pool claim");

    assert!(dto_guard.work().shares_allocation_context(&work));
    assert_eq!(dto_guard.bytes(), 16);
    assert_eq!(
        work.receipt(),
        prefix,
        "transfer is not a new allocation or work event"
    );
    drop(io);
    assert!(
        work.claim(1, 0, acyclic_fs::WorkCounters::default())
            .is_err(),
        "DTO's genuine claim survives observer drop"
    );
    drop(dto);
    drop(dto_guard);
    work.claim(16, 0, acyclic_fs::WorkCounters::default())
        .expect("only real guard retirement frees the original allowance");
}
