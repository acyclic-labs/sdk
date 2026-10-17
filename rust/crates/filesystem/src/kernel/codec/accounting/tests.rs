use super::*;
use crate::kernel::codec::{CanonicalDecodeError, Encoder};
use crate::{CancellationToken, WorkBudget, WorkCounters};

#[test]
fn owned_and_borrowed_standalone_permits_keep_the_exact_header_boundary() {
    for borrowed in [false, true] {
        for limit in [9, 10] {
            let cancellation = CancellationToken::new();
            let budget = WorkBudget {
                bytes_encoded: limit,
                ..WorkBudget::UNBOUNDED
            };
            let mut allocations = allocation::AllocationLedger::default();
            let mut work = WorkCounters::UNCHARGED;
            let mut accounting = if borrowed {
                allocation::KernelAccounting::borrowed_standalone(
                    &mut allocations,
                    &mut work,
                    budget,
                    &cancellation,
                )
            } else {
                allocation::KernelAccounting::standalone(budget, &cancellation)
            };
            let mut observer = EncodeAccounting::new(&mut accounting);
            let encoded = Encoder::with_exact_capacity(
                b"ACYFSFIL",
                1,
                10,
                |step| observer.observe(&EncodeWorkStep::Allocation(step)),
                |_| crate::FsError::from(CanonicalDecodeError::AllocationFailed),
            )
            .map(Encoder::finish);
            let result = observer.finish(encoded);
            assert_eq!(result.is_ok(), limit == 10);
            if let Ok((bytes, mut owner)) = result {
                assert_eq!(bytes, b"ACYFSFIL\x01\x00");
                drop(bytes);
                accounting.release_owned(&mut owner, 10).unwrap();
            }
            assert_eq!(
                accounting.spent().bytes_encoded,
                if limit == 10 { 10 } else { 0 }
            );
            assert_eq!(accounting.spent().allocation_operations, 1);
            assert_eq!(
                accounting.remaining().unwrap().peak_allocation_bytes,
                u64::MAX
            );
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn funded_output_survives_finish_and_panic_returns_only_unperformed_credit() {
    for resource in [false, true] {
        for action in 0..3 {
            let cancellation = CancellationToken::new();
            let budget = WorkBudget {
                bytes_encoded: 15,
                ..WorkBudget::UNBOUNDED
            };
            let root = crate::NativeMaterializationWork::new(
                if resource {
                    WorkBudget::UNBOUNDED
                } else {
                    budget
                },
                &cancellation,
            )
            .unwrap();
            let work = if resource {
                root.native_volume_work(budget).unwrap()
            } else {
                root.clone()
            };
            let baseline = root.live_allocation_bytes();
            let mut accounting = allocation::KernelAccounting::native(&work, &cancellation);
            let operation = || {
                super::super::encode_owned(&mut accounting, |observe, reserve| {
                    observe(EncodeWorkStep::Admit(WorkCounters {
                        bytes_encoded: 15,
                        ..WorkCounters::UNCHARGED
                    }))?;
                    let mut encoder = Encoder::with_exact_capacity(
                        b"ACYFSFIL",
                        1,
                        15,
                        |step| observe(EncodeWorkStep::Allocation(step)),
                        reserve,
                    )?;
                    if action == 1 {
                        panic!("panic after funded header");
                    }
                    if action == 2 {
                        cancellation.cancel();
                    }
                    observe(EncodeWorkStep::Admit(WorkCounters {
                        bytes_encoded: 5,
                        ..WorkCounters::UNCHARGED
                    }))?;
                    encoder.fixed(b"bytes");
                    observe(EncodeWorkStep::Completed(WorkCounters {
                        bytes_encoded: 5,
                        ..WorkCounters::UNCHARGED
                    }))?;
                    Ok(encoder.finish())
                })
            };
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation));
            if action == 0 {
                let (bytes, owner) = result.unwrap().unwrap();
                assert_eq!(bytes, b"ACYFSFIL\x01\x00bytes");
                assert_eq!(root.live_allocation_bytes(), baseline + 15);
                drop(bytes);
                assert_eq!(root.live_allocation_bytes(), baseline + 15);
                drop(owner);
            } else if action == 1 {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            assert_eq!(root.live_allocation_bytes(), baseline);
            let completed = if action == 0 { 15 } else { 10 };
            assert_eq!(root.receipt().bytes_encoded, completed);
            let remaining = work.state(|state| state.remaining()).unwrap();
            assert_eq!(remaining.bytes_encoded, 15 - completed);
        }
    }
}
