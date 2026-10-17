use super::tests::{amount, counters, status};
use super::*;
use proptest::prelude::*;

fn scope(
    root: &crate::NativeMaterializationWork,
    limits: &[WorkBudget],
    invocation: bool,
) -> crate::NativeMaterializationWork {
    let mut work = root.clone();
    for limit in limits {
        // Linking the invocation examines one cancellation registration.
        // Keep the generated allowance available after that setup charge.
        let mut limit = *limit;
        limit.items_examined = limit.items_examined.saturating_add(u64::from(invocation));
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
        let roots = [0,1].map(|i| crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &root_cancel[i]).unwrap());
        let allowances = resource_limits.map(|limits| WorkBudget {
            bytes_copied: limits[0], items_examined: limits[1], bytes_hashed: limits[2], ..WorkBudget::UNBOUNDED
        });
        let work = roots.each_ref().map(|root| {
            let work = scope(root, &allowances[..usize::from(depth)], invocation);
            if recheck { work.recheck_admitted() } else { work }
        });
        let budget = if bounded { counters(limits) } else { WorkBudget::UNBOUNDED };
        for original in &work {
            original.state(|state| { state.work = counters(prior); state.budget = budget; });
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
            let old = KernelAccounting::native(&work[0], &caller_cancel[0]).check()
                .and_then(|()| work[0].charge(delta).map_err(Into::into));
            let new = KernelAccounting::native(&work[1], &caller_cancel[1]).charge(delta);
            prop_assert_eq!(status(new), status(old));
            prop_assert_eq!(roots[1].receipt(), roots[0].receipt());
            prop_assert_eq!(roots[1].live_allocation_bytes(), roots[0].live_allocation_bytes());
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
