use super::*;
use crate::WorkBudget;

fn measured<T>(run: impl FnOnce() -> T) -> (T, acyclic_test_support::AllocationInfo) {
    let mut result = None;
    let observed = acyclic_test_support::measure(|| result = Some(run()));
    (result.unwrap(), observed)
}

fn view(
    work: &NativeMaterializationWork,
    authority: bool,
) -> Result<NativeAllocation, NativeTreeMaterializationError> {
    if authority {
        work.authority_read_allocation()
    } else {
        let mut meter = work.allocation_work();
        meter.bind_invocation(work.cancellation().clone(), NativeResourceCredit::ReadBytes)?;
        Ok(NativeAllocation { meter, bytes: 0 })
    }
}

fn root(budget: WorkBudget) -> NativeMaterializationWork {
    NativeMaterializationWork::new(budget, &crate::CancellationToken::new()).unwrap()
}

#[test]
fn invocation_layout_matches_allocator_through_the_last_alias() {
    for authority in [false, true] {
        let work = root(crate::performance::BOUNDARY_BUDGET);
        let baseline = work.live_allocation_bytes(); // Warm the rank-checker TLS.
        let before = work.receipt();
        let (owner, observed) = measured(|| view(&work, authority).unwrap());
        assert_eq!(observed.count_total, 1);
        assert_eq!(observed.count_current, 1);
        assert_eq!(observed.bytes_total, observed.bytes_max);
        assert_eq!(observed.bytes_current as u64, observed.bytes_total);
        assert_eq!(
            work.live_allocation_bytes(),
            baseline + observed.bytes_total
        );
        assert_eq!(
            work.receipt().allocation_operations,
            before.allocation_operations + 1
        );
        assert_eq!(
            work.receipt().peak_allocation_bytes,
            baseline + observed.bytes_total
        );
        assert_eq!(
            observed.bytes_total - std::mem::size_of::<NativeInvocationView>() as u64,
            2 * std::mem::size_of::<usize>() as u64,
        );
        eprintln!("invocation authority={authority} allocator={observed:?}");
        let (alias, cloned) = measured(|| owner.work().clone());
        assert_eq!(cloned.count_total, 0);
        drop(owner);
        assert_eq!(
            work.live_allocation_bytes(),
            baseline + observed.bytes_total
        );
        let (_, released) = measured(|| drop(alias));
        assert_eq!(released.count_total, 0);
        assert_eq!(released.count_current, -1);
        assert_eq!(released.bytes_current, -(observed.bytes_total as i64));
        assert_eq!(work.live_allocation_bytes(), baseline);
        assert_eq!(
            work.receipt().allocation_operations,
            before.allocation_operations + 1
        );
    }
}

#[test]
fn every_original_and_ancestor_resource_ceiling_admits_exactly_the_full_layout() {
    let probe = root(crate::performance::BOUNDARY_BUDGET);
    probe.live_allocation_bytes();
    let (owner, observed) = measured(|| view(&probe, true).unwrap());
    let required = observed.bytes_total;
    drop(owner);
    for authority in [false, true] {
        for depth in 0..4 {
            for remaining in 0..=required {
                let work = root(WorkBudget {
                    peak_allocation_bytes: if depth == 0 {
                        NativeMaterializationWork::accounting_state_bytes() + remaining
                    } else {
                        crate::performance::BOUNDARY_BUDGET.peak_allocation_bytes
                    },
                    ..crate::performance::BOUNDARY_BUDGET
                });
                let mut scoped = work.clone();
                for _ in 0..depth {
                    scoped = scoped
                        .native_volume_work(WorkBudget {
                            peak_allocation_bytes: remaining,
                            ..crate::performance::BOUNDARY_BUDGET
                        })
                        .unwrap();
                }
                let baseline = work.live_allocation_bytes();
                let before = work.receipt();
                let (result, observed) = measured(|| view(&scoped, authority));
                assert_eq!(result.is_ok(), remaining == required);
                if let Ok(owner) = result {
                    assert_eq!(observed.count_total, 1);
                    assert_eq!(observed.bytes_current as u64, required);
                    assert_eq!(work.live_allocation_bytes(), baseline + required);
                    drop(owner);
                    assert_eq!(
                        work.receipt().allocation_operations,
                        before.allocation_operations + 1
                    );
                    assert_eq!(work.receipt().peak_allocation_bytes, baseline + required);
                } else {
                    assert_eq!(observed.count_total, 0);
                    assert_eq!(work.receipt(), before);
                }
                assert_eq!(work.live_allocation_bytes(), baseline);
                if let Some(resource) = &scoped.resource {
                    assert_eq!(
                        resource.remaining().unwrap().peak_allocation_bytes,
                        remaining
                    );
                }
            }
        }
    }
}

#[test]
fn cancelled_invocation_does_not_allocate_or_change_the_receipt() {
    let work = root(crate::performance::BOUNDARY_BUDGET);
    work.cancellation().cancel();
    let before = work.receipt();
    for authority in [false, true] {
        let (result, observed) = measured(|| view(&work, authority));
        assert!(matches!(
            result,
            Err(NativeTreeMaterializationError::Cancelled)
        ));
        assert_eq!(observed.count_total, 0);
        assert_eq!(work.receipt(), before);
        assert_eq!(
            work.live_allocation_bytes(),
            NativeMaterializationWork::accounting_state_bytes()
        );
    }
}
