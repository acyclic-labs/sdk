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

pub(super) fn readonly_standalone_live(accounting: &KernelAccounting<'_>) -> Option<u64> {
    match accounting {
        KernelAccounting::Standalone { allocations, .. } => Some(allocations.live_bytes()),
        KernelAccounting::BorrowedStandalone { allocations, .. } => Some(allocations.live_bytes()),
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => None,
    }
}

pub(super) fn readonly_cleanup_standalone(
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
    let keep = before.checked_add(retained).ok_or(WorkError::Overflow)?;
    let retired = allocations
        .live_bytes()
        .checked_sub(keep)
        .ok_or(WorkError::Overflow)?;
    allocations
        .release(retired)
        .map_err(crate::collection::handle_allocation_error)
}
