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

// Snapshot only temporary liveness: an operation may promote the ledger or
// create owned publication/index leases without making them read scratch.
pub(crate) fn readonly_standalone_live(accounting: &KernelAccounting<'_>) -> Option<u64> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::allocation::fixed_index::FixedIndex;

    #[test]
    fn read_cleanup_preserves_original_owned_index_and_caller_scratch() -> Result<(), FsError> {
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(
            crate::performance::work_budget(10_000, 8 * 1024),
            &cancellation,
        );
        let mut caller = Owned::<Vec<u8>>::allocate(19, &mut accounting)?;
        caller.value.resize(19, 1);
        let before = readonly_standalone_live(&accounting);
        let mut index = FixedIndex::default();
        for (key, value) in [([1; 16], 41u64), ([2; 16], 43u64)] {
            index.get_or_try_insert_with(key, &mut accounting, |_| Ok(value))?;
        }
        let mut output = Owned::<Vec<u8>>::allocate(17, &mut accounting)?;
        output.value.resize(17, 2);
        let mut temporary = Owned::<Vec<u8>>::allocate(107, &mut accounting)?;
        temporary.value.resize(107, 3);
        // The read scope drops temporary values before restoring its checkpoint.
        drop(temporary);
        let completed = accounting.spent();
        readonly_cleanup_standalone(&mut accounting, before, output.bytes)?;
        assert_eq!(
            accounting.spent(),
            completed,
            "cleanup changed completed work"
        );
        assert_eq!(
            readonly_standalone_live(&accounting),
            Some(caller.bytes + output.bytes)
        );
        assert_eq!(index.get([1; 16], &mut accounting)?, Some(&41));
        assert_eq!(index.get([2; 16], &mut accounting)?, Some(&43));
        drop(index);
        output.retire(&mut accounting)?;
        assert_eq!(caller.value.as_slice(), &[1; 19]);
        assert_eq!(readonly_standalone_live(&accounting), before);

        // A real scratch underflow remains a refusal and changes no liveness.
        assert!(matches!(
            readonly_cleanup_standalone(&mut accounting, Some(caller.bytes + 1), 0),
            Err(FsError::Work(WorkError::Overflow))
        ));
        assert_eq!(readonly_standalone_live(&accounting), before);
        caller.retire(&mut accounting)?;
        readonly_cleanup_standalone(&mut accounting, Some(0), 0)?;
        Ok(())
    }
}
