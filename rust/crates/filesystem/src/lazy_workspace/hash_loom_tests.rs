use super::*;
use crate::{CancellationToken, NativeMaterializationWork};

fn exercise(reentrant: bool, omit_invocation: bool, context: u8, shadow: bool) {
    crate::initialize_test_native_io();
    loom::model(move || {
        let cancellation = CancellationToken::new();
        let original =
            NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
                .map_err(|(error, _)| error)
                .unwrap();
        let work = match context {
            0 => original.clone(),
            1 => original
                .native_volume_work(crate::performance::BOUNDARY_BUDGET)
                .unwrap(),
            _ => {
                // Exercise the production invocation binder with independent
                // cancellation. Linked-child registration is outside hashing.
                let mut view = original.clone();
                view.bind_invocation(
                    CancellationToken::new(),
                    crate::materializer::NativeResourceCredit::ReadBytes,
                )
                .unwrap();
                view
            }
        };
        let before = original.receipt();
        if reentrant {
            let mut accounting = KernelAccounting::native(&work, &cancellation);
            work.state(|_| hash_with_charge(&LazyOverlay::Empty, |delta| accounting.charge(delta)))
                .unwrap();
            return;
        }
        let scope_cancellation = work.cancellation().clone();
        let mut hash_work = work.clone();
        if omit_invocation {
            hash_work.invocation = None;
        }
        let hash_scope_cancellation = scope_cancellation.clone();
        let hash_cancellation = cancellation.clone();
        let hash = loom::thread::spawn(move || {
            let mut accounting = KernelAccounting::native(&hash_work, &hash_cancellation);
            let cancelled_before = hash_scope_cancellation.is_cancelled();
            let result = if shadow {
                hash_lazy_value(&LazyShadow::Empty, &mut accounting)
            } else {
                hash_lazy_value(&LazyOverlay::Empty, &mut accounting)
            };
            (cancelled_before, result)
        });
        let other_work = work.clone();
        let other = loom::thread::spawn(move || {
            other_work.charge(WorkCounters {
                bytes_copied: 1,
                ..WorkCounters::UNCHARGED
            })
        });
        let cancel = loom::thread::spawn(move || scope_cancellation.cancel());
        let (cancelled_before, result) = hash.join().unwrap();
        let other_result = other.join().unwrap();
        cancel.join().unwrap();
        assert!(
            !cancelled_before || result.is_err(),
            "cancelled node hash was admitted"
        );
        if context == 2 {
            assert!(
                !cancellation.is_cancelled(),
                "invocation cancellation reached the original"
            );
        }
        let receipt = original.receipt();
        let hashed = receipt.bytes_hashed - before.bytes_hashed;
        if let Ok(digest) = result {
            assert_eq!(digest, *blake3::hash(br#""Empty""#).as_bytes());
            assert_eq!(hashed, 7);
        } else {
            // Opening quote, the complete unit-variant spelling, closing quote.
            assert!([0, 1, 6].contains(&hashed));
            assert!(matches!(
                result.unwrap_err(),
                LazyWorkspaceError::Filesystem(crate::FsError::Cancelled(_))
                    | LazyWorkspaceError::Filesystem(crate::FsError::NativeKernel(
                        crate::NativeTreeMaterializationError::Cancelled
                    ))
            ));
        }
        assert_eq!(
            receipt.bytes_copied,
            before.bytes_copied + u64::from(other_result.is_ok())
        );
        assert_eq!(receipt.allocation_operations, before.allocation_operations);
        assert!(original.live_allocation_bytes() <= before.peak_allocation_bytes);
        assert!(
            original.live_allocation_bytes() >= NativeMaterializationWork::accounting_state_bytes()
        );
        assert_eq!(receipt.peak_allocation_bytes, before.peak_allocation_bytes);
        let state = original.state.lock();
        assert!(state.native_resource.is_none());
        assert!(state.native_invocation.is_none());
        assert!(state.capture_custody.is_none());
        assert!(!state.recheck_items);
    });
}

#[test]
fn node_hash_preserves_prefix_and_other_charge_during_cancellation() {
    for context in 0..3 {
        for shadow in [false, true] {
            exercise(false, false, context, shadow);
        }
    }
}

fn counterexample(action: impl FnOnce() + std::panic::UnwindSafe, expected: &str) {
    let error = std::panic::catch_unwind(action).expect_err("broken hash must fail");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains(expected), "{message}");
}

#[test]
fn charging_through_the_outer_meter_is_rejected_before_relocking() {
    counterexample(|| exercise(true, false, 0, false), "lock order:");
}

#[test]
fn omitting_invocation_binding_loses_cancellation() {
    counterexample(
        || exercise(false, true, 2, false),
        "cancelled node hash was admitted",
    );
}
