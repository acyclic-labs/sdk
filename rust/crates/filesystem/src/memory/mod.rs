//! Deterministic in-memory backends.

use crate::foundation::{
    AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES, AuthorityId, DurableCommit, Epoch, Head, OperationId,
    ProposedCommit, Sequence, authority_commit_digest,
};
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    AppendOutcome, AuthorityFailure, AuthorityStoreError, CreateAuthorityOutcome,
    OBJECT_DIGEST_ENVELOPE_BYTES, ObjectFailure, ObjectId, ObjectRead, ObjectReadRetention,
    ObjectStoreError, ObjectWrite, ReplayLimit, object_digest,
};
use crate::sync::Mutex;
use crate::{AuthorityWork, FsError, WorkError};
use acyclic_stream::OwnedBytes;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::RwLock;

mod authority;
mod objects;
pub use authority::MemoryAuthorityStore;
pub use objects::MemoryObjectStore;

/// Default maximum immutable object size.
pub const DEFAULT_MAX_OBJECT_BYTES: u64 = 64 * 1024 * 1024;

fn allocate<T>(
    capacity: usize,
    accounting: &mut KernelAccounting<'_>,
    failed: fn(std::collections::TryReserveError) -> FsError,
) -> Result<(Vec<T>, KernelOwnedAllocation), FsError> {
    let requested = capacity
        .checked_mul(size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(WorkError::Overflow)?;
    let mut custody = accounting.claim_owned(requested, u64::from(requested != 0))?;
    // On failure, free the vector before its guard.
    let mut values = Vec::new();
    values.try_reserve_exact(capacity).map_err(failed)?;
    let actual = values
        .capacity()
        .checked_mul(size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(WorkError::Overflow)?;
    accounting.reconcile_owned(&mut custody, requested, actual)?;
    Ok((values, custody))
}

fn allocation(custody: KernelOwnedAllocation) -> crate::AuthorityAllocation {
    match custody {
        KernelOwnedAllocation::Standalone => None,
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(custody) => Some(custody),
    }
}

#[cfg(test)]
#[path = "../tests/memory/mod.rs"]
mod tests;
