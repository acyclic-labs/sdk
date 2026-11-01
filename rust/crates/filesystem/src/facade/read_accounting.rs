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
    // Only temporary scratch belongs to a read's cleanup boundary. Retained
    // provider/index backing may be created or retired by independent owners.
    match accounting {
        KernelAccounting::Standalone { allocations, .. } => Some(allocations.scratch_bytes()),
        KernelAccounting::BorrowedStandalone { allocations, .. } => {
            Some(allocations.scratch_bytes())
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
    let keep = before.checked_add(retained).ok_or(WorkError::Overflow)?;
    allocations.release_scratch_to(keep)
}
