use super::*;
use crate::{CancellationToken, NativeMaterializationWork};

fn exercise(reentrant: bool, context: u8, shadow: bool) {
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
            _ => original.invocation_view().unwrap().0,
        };
        let before = original.receipt();
        if reentrant {
            let mut accounting = KernelAccounting::native(&work, &cancellation);
            work.state(|_| hash_with_charge(&LazyOverlay::Empty, |delta| accounting.charge(delta)))
                .unwrap();
            return;
        }
        let hash_work = work.clone();
        let hash_cancellation = cancellation.clone();
        let hash = loom::thread::spawn(move || {
            let mut accounting = KernelAccounting::native(&hash_work, &hash_cancellation);
            if shadow {
                hash_lazy_value(&LazyShadow::Empty, &mut accounting)
            } else {
                hash_lazy_value(&LazyOverlay::Empty, &mut accounting)
            }
        });
        let other_work = work.clone();
        let other = loom::thread::spawn(move || {
            other_work.charge(WorkCounters {
                bytes_copied: 1,
                ..WorkCounters::UNCHARGED
            })
        });
        let cancel = loom::thread::spawn(move || cancellation.cancel());
        let result = hash.join().unwrap();
        let other_result = other.join().unwrap();
        cancel.join().unwrap();
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
            exercise(false, context, shadow);
        }
    }
}

#[test]
fn charging_through_the_outer_meter_is_rejected_before_relocking() {
    let error = std::panic::catch_unwind(|| exercise(true, 0, false))
        .expect_err("reentrant meter must fail");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains("lock order:"), "{message}");
}
