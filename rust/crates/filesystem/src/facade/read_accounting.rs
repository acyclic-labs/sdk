use super::*;

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn native_pinned_allocation(
    allocation: KernelOwnedAllocation,
) -> Result<NativeAllocation, FsError> {
    match allocation {
        KernelOwnedAllocation::NativeOriginal(allocation) => Ok(allocation),
        KernelOwnedAllocation::Standalone => Err(FsError::NativeKernel(
            NativeTreeMaterializationError::InvalidLayout,
        )),
    }
}

pub(crate) fn readonly_standalone_live(accounting: &KernelAccounting<'_>) -> Option<u64> {
    match accounting {
        KernelAccounting::Standalone { allocations, .. } => {
            Some(allocations.live_bytes() - allocations.frame_control_bytes())
        }
        KernelAccounting::BorrowedStandalone { allocations, .. } => {
            Some(allocations.live_bytes() - allocations.frame_control_bytes())
        }
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => None,
    }
}

pub(crate) fn readonly_cleanup_standalone(
    accounting: &mut KernelAccounting<'_>,
    before: Option<u64>,
    retained: u64,
) -> Result<(), FsError> {
    let Some(before) = before else {
        return Ok(());
    };
    let Some((allocations, _, _, _)) = accounting.standalone_parts() else {
        return Ok(());
    };
    let keep = before
        .checked_add(retained)
        .and_then(|bytes| bytes.checked_add(allocations.frame_control_bytes()))
        .ok_or(WorkError::Overflow)?;
    let retired = allocations
        .live_bytes()
        .checked_sub(keep)
        .ok_or(WorkError::Overflow)?;
    allocations
        .release(retired)
        .map_err(|error| error.into_work_or(FsError::HandleAllocationFailed))
}

#[cfg(test)]
mod frame_tests {
    use super::*;

    #[tokio::test]
    async fn readonly_cleanup_preserves_the_first_frames_retirement_control() {
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let before = readonly_standalone_live(&accounting);
        let ((), reservation) =
            crate::heap_future::in_heap_metered(&mut accounting, |_| std::future::ready(()))
                .unwrap()
                .await;
        crate::heap_future::FrameLedger::release_frame(&mut accounting, reservation).unwrap();
        readonly_cleanup_standalone(&mut accounting, before, 0).unwrap();
        let (allocations, _, _, _) = accounting.standalone_parts().unwrap();
        assert!(allocations.frame_control_bytes() > 0);
        assert_eq!(allocations.live_bytes(), allocations.frame_control_bytes());
        let baseline = allocations.live_bytes();
        let frame = crate::heap_future::in_heap_metered(&mut accounting, |_| async {
            let buffer = [0; 128];
            std::future::pending::<()>().await;
            std::hint::black_box(buffer);
        })
        .unwrap();
        drop(frame);
        assert_eq!(
            accounting.standalone_parts().unwrap().0.live_bytes(),
            baseline
        );
    }
}

#[cfg(test)]
#[cfg(all(
    feature = "memory",
    feature = "distributed",
    not(target_arch = "wasm32")
))]
#[path = "read_accounting/frame_cancellation.rs"]
mod frame_cancellation;
