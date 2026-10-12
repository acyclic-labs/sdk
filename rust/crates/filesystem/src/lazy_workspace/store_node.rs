use super::*;
use crate::kernel::allocation::{AllocationLedger, KernelAccounting, KernelOwnedAllocation};

pub(crate) trait MemoryLazyNode: Serialize + Eq {
    fn owned_bytes(&self) -> u64;
    fn clone_accounted(
        &self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, KernelOwnedAllocation), LazyWorkspaceError>
    where
        Self: Sized;
}

pub(super) fn copy_bytes(
    bytes: &[u8],
    accounting: &mut KernelAccounting<'_>,
) -> Result<crate::kernel::Owned<Vec<u8>>, LazyWorkspaceError> {
    let mut copy = crate::kernel::Owned::allocate(bytes.len(), accounting)?;
    let work = WorkCounters {
        bytes_copied: bytes.len() as u64,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(work)?;
    copy.value.extend_from_slice(bytes);
    accounting.record_completed(work)?;
    Ok(copy)
}

impl MemoryLazyNode for LazyOverlay {
    fn owned_bytes(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Node { path, .. } => path.capacity() as u64,
        }
    }

    fn clone_accounted(
        &self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, KernelOwnedAllocation), LazyWorkspaceError> {
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        let Self::Node {
            path,
            priority,
            change,
            left,
            right,
        } = self
        else {
            return Ok((Self::Empty, accounting.claim_owned(0, 0)?));
        };
        let path = copy_bytes(path.as_bytes(), accounting)?;
        // Input is a String, so this conversion cannot reject or allocate.
        let text = String::from_utf8(path.value).expect("copied UTF-8 path");
        Ok((
            Self::Node {
                path: text,
                priority: *priority,
                change: change.clone(),
                left: *left,
                right: *right,
            },
            path.allocation,
        ))
    }
}

impl MemoryLazyNode for LazyShadow {
    fn owned_bytes(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Node { record, .. } => {
                record.capacity() as u64 + std::mem::size_of::<crate::WorkspaceMetadata>() as u64
            }
        }
    }

    fn clone_accounted(
        &self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, KernelOwnedAllocation), LazyWorkspaceError> {
        cancellation
            .check()
            .map_err(|_| LazyWorkspaceError::Cancelled)?;
        let Self::Node {
            file_id,
            priority,
            record,
            metadata,
            left,
            right,
        } = self
        else {
            return Ok((Self::Empty, accounting.claim_owned(0, 0)?));
        };
        let mut metadata_allocation =
            accounting.claim_owned(std::mem::size_of::<crate::WorkspaceMetadata>() as u64, 1)?;
        let mut record = copy_bytes(record, accounting)?;
        let copied = WorkCounters {
            bytes_copied: std::mem::size_of::<crate::WorkspaceMetadata>() as u64,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        let metadata = Box::new(**metadata);
        accounting.record_completed(copied)?;
        record.allocation.absorb(&mut metadata_allocation)?;
        Ok((
            Self::Node {
                file_id: *file_id,
                priority: *priority,
                record: record.value,
                metadata,
                left: *left,
                right: *right,
            },
            record.allocation,
        ))
    }
}

pub(crate) fn clone_lazy_node<T: MemoryLazyNode>(
    value: &T,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<T, LazyWorkspaceError> {
    let before = allocations.live_bytes();
    let mut accounting =
        KernelAccounting::borrowed_standalone(allocations, work, budget, cancellation);
    let result = value
        .clone_accounted(&mut accounting, cancellation)
        .map(|(value, _)| value);
    if result.is_err() {
        allocations
            .release(allocations.live_bytes() - before)
            .map_err(|error| {
                LazyWorkspaceError::Filesystem(
                    error.into_work_or(crate::FsError::PendingMutationAllocationFailed),
                )
            })?;
    }
    result
}
