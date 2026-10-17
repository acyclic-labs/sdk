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
                independent_ceiling_case(
                    required,
                    authority,
                    if depth == 0 { remaining } else { required + 1 },
                    &vec![remaining; depth],
                    false,
                );
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

// Exercise the production admission path with every resource check except the
// innermost disabled. The flag is thread-local and absent from production builds.
std::thread_local! {
    static ONLY_INNERMOST: std::cell::Cell<*const NativeResourceAllowance> = const {
        std::cell::Cell::new(std::ptr::null())
    };
}

pub(in crate::materializer) fn skip_ancestor_check(scope: &NativeResourceAllowance) -> bool {
    ONLY_INNERMOST.with(|inner| !inner.get().is_null() && !std::ptr::eq(inner.get(), scope))
}

fn independent_ceiling_case(
    required: u64,
    authority: bool,
    root_remaining: u64,
    limits: &[u64],
    only_innermost: bool,
) {
    let metadata =
        crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<NativeResourceAllowance>())
            .unwrap() as u64;
    let work = root(WorkBudget {
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes()
            + limits.len() as u64 * metadata
            + root_remaining,
        ..crate::performance::BOUNDARY_BUDGET
    });
    let mut scoped = work.clone();
    for &limit in limits {
        scoped = scoped
            .native_volume_work(WorkBudget {
                peak_allocation_bytes: limit,
                ..crate::performance::BOUNDARY_BUDGET
            })
            .unwrap();
    }
    let baseline = work.live_allocation_bytes();
    let before = work.receipt();
    let expected = root_remaining >= required && limits.iter().all(|&limit| limit >= required);
    let (result, observed) = ONLY_INNERMOST.with(|inner| {
        assert!(inner.get().is_null());
        struct Reset<'a>(&'a std::cell::Cell<*const NativeResourceAllowance>);
        impl Drop for Reset<'_> {
            fn drop(&mut self) {
                self.0.set(std::ptr::null());
            }
        }
        let _reset = Reset(inner);
        if only_innermost {
            inner.set(Arc::as_ptr(scoped.resource.as_ref().unwrap()));
        }
        measured(|| view(&scoped, authority))
    });
    let admitted = result.is_ok();
    let live_with_owner = work.live_allocation_bytes();
    drop(result);
    assert_eq!(admitted, expected, "ceiling admission");
    assert_eq!(observed.count_total, u64::from(expected));
    assert_eq!(observed.bytes_total, if expected { required } else { 0 });
    assert_eq!(
        observed.bytes_current,
        if expected { required as i64 } else { 0 }
    );
    assert_eq!(
        live_with_owner,
        baseline + if expected { required } else { 0 }
    );
    assert_eq!(work.live_allocation_bytes(), baseline);
    if expected {
        assert_eq!(
            work.receipt().allocation_operations,
            before.allocation_operations + 1
        );
        assert_eq!(work.receipt().peak_allocation_bytes, baseline + required);
    } else {
        assert_eq!(work.receipt(), before);
    }
    if let Some(resource) = &scoped.resource {
        assert_eq!(
            resource.remaining().unwrap().peak_allocation_bytes,
            *limits.iter().min().unwrap()
        );
    }
}

#[test]
fn independently_constrained_ancestors_and_root_reject_roomy_descendants() {
    let required =
        crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<NativeInvocationView>())
            .unwrap() as u64;
    for authority in [false, true] {
        for depth in 1..=3 {
            for constrained in 0..=depth {
                for remaining in 0..=required {
                    let mut limits = vec![required + 1; depth];
                    let root_remaining = if constrained == 0 {
                        remaining
                    } else {
                        limits[constrained - 1] = remaining;
                        required + 1
                    };
                    independent_ceiling_case(required, authority, root_remaining, &limits, false);
                }
            }
        }
    }
}

#[test]
fn innermost_only_admission_is_detected_for_each_ancestor() {
    let required =
        crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<NativeInvocationView>())
            .unwrap() as u64;
    for authority in [false, true] {
        for depth in 2..=3 {
            for ancestor in 0..depth - 1 {
                let mut limits = vec![required + 1; depth];
                limits[ancestor] = required - 1;
                let failure = std::panic::catch_unwind(|| {
                    independent_ceiling_case(required, authority, required + 1, &limits, true);
                })
                .expect_err("innermost-only admission must violate the ceiling assertion");
                assert!(
                    failure
                        .downcast_ref::<String>()
                        .unwrap()
                        .contains("ceiling admission")
                );
            }
        }
    }
}
