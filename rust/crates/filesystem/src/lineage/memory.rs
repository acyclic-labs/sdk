use super::*;
use crate::record_store::MemoryRecords;

/// Process-local lineage adapter for tests and embedded callers.
#[derive(Default)]
pub struct MemoryWorkspaceLineageStore {
    records: MemoryRecords<WorkspaceId, StoredRecord>,
}

struct StoredRecord {
    record: WorkspaceLineageRecord,
    #[cfg(not(target_arch = "wasm32"))]
    _allocation: Option<crate::NativeAllocation>,
    #[cfg(not(target_arch = "wasm32"))]
    entry_allocation: Option<crate::NativeAllocation>,
}

impl MemoryWorkspaceLineageStore {
    /// Creates an empty adapter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Process-local lineage synchronization failure.
#[derive(Debug, Error)]
#[error("workspace-lineage memory store is unavailable")]
pub struct MemoryWorkspaceLineageStoreError;

impl WorkspaceLineageStore for MemoryWorkspaceLineageStore {
    type Error = MemoryWorkspaceLineageStoreError;

    async fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<WorkspaceLineageRecord>, Self::Error> {
        let records = self
            .records
            .0
            .lock()
            .map_err(|_| MemoryWorkspaceLineageStoreError)?;
        Ok(records
            .get(&workspace_id)
            .map(|stored| stored.record.clone()))
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        let mut records = self
            .records
            .0
            .lock()
            .map_err(|_| MemoryWorkspaceLineageStoreError)?;
        if records
            .get(&workspace_id)
            .map_or(0, |stored| stored.record.revision)
            != expected_revision
        {
            return Ok(false);
        }
        #[cfg(not(target_arch = "wasm32"))]
        let entry_allocation = records
            .get_mut(&workspace_id)
            .and_then(|stored| stored.entry_allocation.take());
        records.insert(
            workspace_id,
            StoredRecord {
                record: replacement,
                #[cfg(not(target_arch = "wasm32"))]
                _allocation: None,
                #[cfg(not(target_arch = "wasm32"))]
                entry_allocation,
            },
        );
        Ok(true)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn load_with_native_work(
        &self,
        workspace_id: WorkspaceId,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<
        Option<(WorkspaceLineageRecord, crate::NativeAllocation)>,
        (
            WorkspaceLineageError<Self::Error>,
            Option<crate::NativeAllocation>,
        ),
    > {
        let mut accounting =
            crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        accounting
            .charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })
            .map_err(lineage_filesystem_failure)?;
        let records = self.records.0.lock().map_err(|_| {
            (
                WorkspaceLineageError::Store(MemoryWorkspaceLineageStoreError),
                None,
            )
        })?;
        records
            .get(&workspace_id)
            .map(|stored| clone_record(&stored.record, &mut accounting))
            .transpose()
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_swap_with_native_work(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: (WorkspaceLineageRecord, crate::NativeAllocation),
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<
        bool,
        (
            WorkspaceLineageError<Self::Error>,
            Option<crate::NativeAllocation>,
        ),
    > {
        let actual = replacement
            .0
            .workspace_name
            .capacity()
            .checked_add(
                replacement
                    .0
                    .parent_workspace_name
                    .as_ref()
                    .map_or(0, String::capacity),
            )
            .ok_or_else(|| lineage_filesystem_failure(crate::WorkError::Overflow.into()))?
            as u64;
        if !replacement.1.work().shares_ledger(original) || replacement.1.bytes() < actual {
            return Err(lineage_filesystem_failure(
                crate::NativeTreeMaterializationError::InvalidLayout.into(),
            ));
        }
        let mut accounting =
            crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        accounting
            .charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })
            .map_err(lineage_filesystem_failure)?;
        let mut records = self.records.0.lock().map_err(|_| {
            (
                WorkspaceLineageError::Store(MemoryWorkspaceLineageStoreError),
                None,
            )
        })?;
        if records
            .get(&workspace_id)
            .map_or(0, |stored| stored.record.revision)
            != expected_revision
        {
            return Ok(false);
        }
        let entry_allocation = if let Some(stored) = records.get_mut(&workspace_id) {
            stored.entry_allocation.take()
        } else {
            let allocation = accounting
                .claim_owned(std::mem::size_of::<(WorkspaceId, StoredRecord)>() as u64, 0)
                .map_err(lineage_filesystem_failure)?;
            let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =
                allocation
            else {
                unreachable!("native entry retains original custody")
            };
            Some(allocation)
        };
        records.insert(
            workspace_id,
            StoredRecord {
                record: replacement.0,
                _allocation: Some(replacement.1),
                entry_allocation,
            },
        );
        Ok(true)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn copy_name(
    name: &str,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<
    (String, crate::NativeAllocation),
    (
        WorkspaceLineageError<MemoryWorkspaceLineageStoreError>,
        Option<crate::NativeAllocation>,
    ),
> {
    let copied = crate::WorkCounters {
        bytes_copied: name.len() as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    accounting
        .admit(copied)
        .map_err(lineage_filesystem_failure)?;
    let allocation = accounting
        .claim_owned(name.len() as u64, u64::from(!name.is_empty()))
        .map_err(lineage_filesystem_failure)?;
    let mut owner = (String::new(), allocation);
    owner.0.try_reserve_exact(name.len()).map_err(|error| {
        lineage_filesystem_failure(
            crate::NativeTreeMaterializationError::AllocationFailed(error).into(),
        )
    })?;
    accounting
        .reconcile_owned(&mut owner.1, name.len() as u64, owner.0.capacity() as u64)
        .map_err(lineage_filesystem_failure)?;
    owner.0.push_str(name);
    accounting
        .record_completed(copied)
        .map_err(lineage_filesystem_failure)?;
    let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = owner.1
    else {
        unreachable!("native name retains original custody")
    };
    Ok((owner.0, allocation))
}

#[cfg(not(target_arch = "wasm32"))]
fn clone_record(
    source: &WorkspaceLineageRecord,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<
    (WorkspaceLineageRecord, crate::NativeAllocation),
    (
        WorkspaceLineageError<MemoryWorkspaceLineageStoreError>,
        Option<crate::NativeAllocation>,
    ),
> {
    let name = copy_name(&source.workspace_name, accounting)?;
    let copied = crate::WorkCounters {
        bytes_copied: std::mem::size_of::<WorkspaceLineageRecord>() as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    accounting
        .admit(copied)
        .map_err(lineage_filesystem_failure)?;
    let mut owned = (
        WorkspaceLineageRecord {
            version: source.version,
            revision: source.revision,
            ready: source.ready,
            workspace_id: source.workspace_id,
            workspace_name: name.0,
            parent_workspace_id: source.parent_workspace_id,
            parent_workspace_name: None,
            fork_generation: source.fork_generation,
            initial_generation: source.initial_generation,
        },
        name.1,
    );
    accounting
        .record_completed(copied)
        .map_err(lineage_filesystem_failure)?;
    if let Some(parent_name) = &source.parent_workspace_name {
        let mut parent = copy_name(parent_name, accounting)?;
        owned
            .1
            .absorb(&mut parent.1)
            .map_err(|error| lineage_filesystem_failure(error.into()))?;
        owned.0.parent_workspace_name = Some(parent.0);
    }
    Ok(owned)
}
