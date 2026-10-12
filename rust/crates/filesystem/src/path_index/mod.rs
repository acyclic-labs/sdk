//! Bounded, disposable generation/path query acceleration.

use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::kernel::{FileRecord, LogicalName, NameEncoding, NamespacePath, Owned};
use crate::model::VolumeLimits;
use crate::{FileId, FsError, GenerationId, WorkCounters, WorkError};
use std::mem::size_of;

mod codec;
mod memory;
pub(crate) use codec::{cache_key, decode, encode_records};
pub(crate) use memory::MemoryGenerationPathIndex;

const MAXIMUM_ENTRIES: usize = 256;
const MAXIMUM_RETAINED_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_VALUE_BYTES: usize = 1024 * 1024;

fn snapshot(accounting: &mut KernelAccounting<'_>) -> Option<u64> {
    accounting.standalone_parts().map(|(ledger, _, _, _)| ledger.live_bytes())
}

// Retire only this operation's scratch, after its physical buffers have dropped.
fn restore(accounting: &mut KernelAccounting<'_>, before: Option<u64>, retained: u64) -> Result<(), FsError> {
    if let Some(before) = before {
        let (ledger, _, _, _) = accounting.standalone_parts().ok_or(FsError::HandleAllocationFailed)?;
        let keep = before.checked_add(retained).ok_or(WorkError::Overflow)?;
        let retired = ledger.live_bytes().checked_sub(keep).ok_or(WorkError::Overflow)?;
        ledger.release(retired).map_err(|error| error.into_work_or(FsError::HandleAllocationFailed))?;
    }
    Ok(())
}

fn copy(bytes: &[u8], accounting: &mut KernelAccounting<'_>) -> Result<Owned<Vec<u8>>, FsError> {
    let delta = WorkCounters { bytes_copied: bytes.len() as u64, ..WorkCounters::UNCHARGED };
    accounting.admit(delta)?;
    let mut owned = Owned::<Vec<u8>>::allocate(bytes.len(), accounting)?;
    owned.value.extend_from_slice(bytes);
    accounting.record_completed(delta)?;
    Ok(owned)
}

#[cfg(test)]
mod tests;
