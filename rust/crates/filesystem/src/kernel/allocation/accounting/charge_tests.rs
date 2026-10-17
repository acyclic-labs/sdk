use super::tests::{amount, counters, status};
use super::*;
use acyclic_test_support::{AllocationInfo, measure};
use proptest::prelude::*;
use std::cell::Cell;

std::thread_local! {
    static EXTRA_ALLOCATION: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn redundant_allocation() {
    if EXTRA_ALLOCATION.with(Cell::get) {
        drop(std::hint::black_box(Box::new([0_u8; 17])));
    }
}

fn measured<T>(run: impl FnOnce() -> T) -> (T, AllocationInfo) {
    let mut result = None;
    let heap = measure(|| result = Some(run()));
    (result.unwrap(), heap)
}

fn no_heap_work(heap: AllocationInfo) -> Result<(), AllocationInfo> {
    if heap == AllocationInfo::default() {
        Ok(())
    } else {
        Err(heap)
    }
}

fn scope(
    root: &crate::NativeMaterializationWork,
    limits: &[WorkBudget],
    invocation: bool,
) -> crate::NativeMaterializationWork {
    let mut work = root.clone();
    for limit in limits {
        // Linking examines one registration and reserves three cleanup items.
        // Keep the generated allowance available after setup and reservation.
        let mut limit = *limit;
        limit.items_examined = limit
            .items_examined
            .saturating_add(4 * u64::from(invocation));
        work = work.native_volume_work(limit).unwrap();
    }
    if invocation {
        work = work.invocation_view().unwrap().0;
    }
    work
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // The old check-then-charge path is the independent behavioral reference:
    // exact errors, completed prefixes, all receipt fields and live storage.
    #[test]
    fn native_charge_matches_the_old_path_at_every_completed_prefix(
        prior in prop::array::uniform24(amount(4)),
        limits in prop::array::uniform24(amount(64)),
        deltas in prop::collection::vec(prop::array::uniform24(amount(8)), 0..12),
        bounded in any::<bool>(), depth in 0_u8..3, invocation in any::<bool>(),
        recheck in any::<bool>(), stop_at in 0_usize..12, failure in 0_u8..6,
        resource_limits in prop::array::uniform2(prop::array::uniform3(amount(64))),
    ) {
        let root_cancel = [crate::CancellationToken::new(), crate::CancellationToken::new()];
        let caller_cancel = [crate::CancellationToken::new(), crate::CancellationToken::new()];
        let allowances = resource_limits.map(|limits| WorkBudget {
            bytes_copied: limits[0], items_examined: limits[1], bytes_hashed: limits[2], ..WorkBudget::UNBOUNDED
        });
        // Discover only the deterministic setup cost in independent state;
        // construct the tested roots with real finite budgets. Never overwrite
        // a budget after setup: invocation cleanup already owns reserved credit.
        let setup_root = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).unwrap();
        let setup = scope(&setup_root, &allowances[..usize::from(depth)], invocation);
        let cost = setup.state(|state| {
            let mut reserved = state.budget.remaining(state.original_budget()).unwrap();
            reserved.peak_allocation_bytes = 0;
            state.work.checked_add(reserved).unwrap()
        });
        drop(setup);
        drop(setup_root);
        let mut total = serde_json::to_value(cost).unwrap();
        let extra = serde_json::to_value(counters(limits)).unwrap();
        for (key, value) in total.as_object_mut().unwrap() {
            *value = serde_json::json!(value.as_u64().unwrap().saturating_add(extra[key].as_u64().unwrap()));
        }
        let budget = if bounded { serde_json::from_value(total).unwrap() } else { WorkBudget::UNBOUNDED };
        let roots = [0,1].map(|i| crate::NativeMaterializationWork::new(budget, &root_cancel[i]).unwrap());
        let work = roots.each_ref().map(|root| {
            let work = scope(root, &allowances[..usize::from(depth)], invocation);
            if recheck { work.recheck_admitted() } else { work }
        });
        for original in &work {
            let _ = original.record_admitted(counters(prior));
        }
        for (step, delta) in deltas.into_iter().map(counters).enumerate() {
            if step == stop_at {
                for i in [0,1] {
                    match failure {
                        1 => caller_cancel[i].cancel(),
                        2 => root_cancel[i].cancel(),
                        3 => work[i].state(|state| state.poison_native_original_work()),
                        4 => { root_cancel[i].cancel(); caller_cancel[i].cancel(); },
                        5 => { work[i].state(|state| state.poison_native_original_work()); caller_cancel[i].cancel(); },
                        _ => (),
                    }
                }
            }
            let (old, old_heap) = measured(|| KernelAccounting::native(&work[0], &caller_cancel[0]).check()
                .and_then(|()| work[0].charge(delta).map_err(Into::into)));
            let (new, new_heap) = measured(|| KernelAccounting::native(&work[1], &caller_cancel[1]).charge(delta));
            prop_assert_eq!(no_heap_work(old_heap), Ok(()));
            prop_assert_eq!(no_heap_work(new_heap), Ok(()));
            prop_assert_eq!(status(new), status(old));
            prop_assert_eq!(roots[1].receipt(), roots[0].receipt());
            prop_assert_eq!(roots[1].live_allocation_bytes(), roots[0].live_allocation_bytes());
        }
    }
}

#[test]
fn a_temporary_allocation_in_charge_fails_the_same_physical_heap_assertion() {
    let cancellation = crate::CancellationToken::new();
    let root = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    for depth in 0..3 {
        for invocation in [false, true] {
            for recheck in [false, true] {
                let work = scope(&root, &vec![WorkBudget::UNBOUNDED; depth], invocation);
                let work = if recheck {
                    work.recheck_admitted()
                } else {
                    work
                };
                let mut accounting = KernelAccounting::native(&work, &cancellation);
                // Warm the lock checker TLS outside either heap measurement.
                accounting.check().unwrap();
                let (_, clean) = measured(|| accounting.charge(WorkCounters::UNCHARGED).unwrap());
                no_heap_work(clean).unwrap();
                struct Restore(bool);
                impl Drop for Restore {
                    fn drop(&mut self) {
                        EXTRA_ALLOCATION.with(|flag| flag.set(self.0));
                    }
                }
                let restore = Restore(EXTRA_ALLOCATION.with(|flag| flag.replace(true)));
                let (_, broken) = measured(|| accounting.charge(WorkCounters::UNCHARGED).unwrap());
                drop(restore);
                assert_eq!(
                    broken,
                    AllocationInfo {
                        count_total: 1,
                        count_current: 0,
                        count_max: 1,
                        bytes_total: 17,
                        bytes_current: 0,
                        bytes_max: 17,
                    }
                );
                assert_eq!(no_heap_work(broken), Err(broken));
            }
        }
    }
}

fn exactly_one(locks: usize) -> Result<(), usize> {
    if locks == 1 { Ok(()) } else { Err(locks) }
}

#[test]
fn each_kernel_charge_visits_the_original_leaf_once() {
    let root_cancel = crate::CancellationToken::new();
    let caller_cancel = crate::CancellationToken::new();
    let root = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &root_cancel).unwrap();
    for depth in 0..3 {
        for invocation in [false, true] {
            for recheck in [false, true] {
                let work = scope(&root, &vec![WorkBudget::UNBOUNDED; depth], invocation);
                let work = if recheck {
                    work.recheck_admitted()
                } else {
                    work
                };
                let delta = WorkCounters {
                    items_examined: 1,
                    bytes_copied: 1,
                    ..WorkCounters::UNCHARGED
                };
                let mut accounting = KernelAccounting::native(&work, &caller_cancel);
                let (result, locks) = root.state.count_locks(|| accounting.charge(delta));
                result.unwrap();
                exactly_one(locks).unwrap();
                // A discarded, uncharged preflight still visits the real
                // mutex and must be detected even though its receipt is zero.
                let (result, locks) = root.state.count_locks(|| {
                    accounting.check()?;
                    accounting.charge(delta)
                });
                result.unwrap();
                assert!(exactly_one(locks).is_err());
            }
        }
    }
    caller_cancel.cancel();
    let (result, locks) = root.state.count_locks(|| {
        KernelAccounting::native(&root, &caller_cancel).charge(WorkCounters::UNCHARGED)
    });
    assert!(result.is_err());
    exactly_one(locks).unwrap();
    let limited = root
        .native_volume_work(WorkBudget {
            bytes_copied: 0,
            ..WorkBudget::UNBOUNDED
        })
        .unwrap();
    let (result, locks) = root.state.count_locks(|| {
        KernelAccounting::native(&limited, &root_cancel).charge(WorkCounters {
            bytes_copied: 1,
            ..WorkCounters::UNCHARGED
        })
    });
    assert!(result.is_err());
    exactly_one(locks).unwrap();
}
