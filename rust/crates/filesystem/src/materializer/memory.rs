use super::sdk;
use sdk::{MaterializationJournal as Journal, MaterializationJournalStore, OperationId};
use std::collections::BTreeMap;

#[derive(Default)]
pub struct Store(parking_lot::Mutex<BTreeMap<OperationId, Journal>>);
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("journal changed")]
    Conflict(Journal),
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    NativeWork(#[from] sdk::NativeTreeMaterializationError),
}
impl MaterializationJournalStore for Store {
    type Error = StoreError;
    #[cfg(not(target_arch = "wasm32"))]
    type ActionCompletion<'a> = futures::future::BoxFuture<'a, Result<Journal, StoreError>>;
    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_action(
        &self,
        mut journal: Journal,
    ) -> Result<Self::ActionCompletion<'_>, StoreError> {
        let operation = journal.plan.operation_id;
        if self.0.lock().get(&operation) != Some(&journal) {
            return Err(StoreError::Conflict(journal));
        }
        let mut replacement = copy(&journal)?;
        journal.revision = journal
            .revision
            .checked_add(1)
            .ok_or(sdk::NativeTreeMaterializationError::InvalidLayout)?;
        replacement.revision = journal.revision;
        Ok(Box::pin(async move {
            if let Some(allocation) = &journal.custody {
                journal.work = allocation.work().journal_work()?;
                replacement.work = journal.work;
            }
            let mut records = self.0.lock();
            if records.get(&operation).map(|current| current.revision) != Some(journal.revision - 1)
            {
                return Err(StoreError::Conflict(journal));
            }
            records.insert(operation, replacement);
            Ok(journal)
        }))
    }
    async fn load(&self, operation: OperationId) -> Result<Option<Journal>, StoreError> {
        self.0.lock().get(&operation).map(copy).transpose()
    }
    async fn compare_and_swap(
        &self,
        operation: OperationId,
        expected: u64,
        replacement: Journal,
    ) -> Result<(bool, Journal), StoreError> {
        let mut records = self.0.lock();
        let current = records.get(&operation);
        let stored = current.map_or(0, |journal| journal.revision) == expected;
        if stored {
            records.insert(operation, copy(&replacement)?);
        }
        Ok((stored, replacement))
    }
}
fn copy(journal: &Journal) -> Result<Journal, StoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    let copied = sdk::WorkCounters {
        bytes_copied: copy_bytes(journal)?,
        ..sdk::WorkCounters::UNCHARGED
    };
    #[cfg(not(target_arch = "wasm32"))]
    let copying = journal
        .custody
        .as_ref()
        .map(|allocation| allocation.work().reserve_work(copied))
        .transpose()?;
    #[cfg(not(target_arch = "wasm32"))]
    let custody = journal
        .custody
        .as_ref()
        .map(|allocation| {
            let bytes = sdk::materializer::journal_owned_bytes(journal)?;
            allocation
                .work()
                .claim(bytes, 0, sdk::WorkCounters::UNCHARGED)
        })
        .transpose()?;
    let copy = Journal {
        plan: journal.plan.clone(),
        preimages: journal
            .preimages
            .iter()
            .map(|image| sdk::MaterializationPreimage {
                image: image.image.clone(),
                #[cfg(not(target_arch = "wasm32"))]
                custody: None,
            })
            .collect(),
        #[cfg(not(target_arch = "wasm32"))]
        custody,
        ..*journal
    };
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(copying) = copying {
        copying.record_completed(copied)?;
    }
    Ok(copy)
}

#[cfg(not(target_arch = "wasm32"))]
fn copy_bytes(journal: &Journal) -> Result<u64, sdk::NativeTreeMaterializationError> {
    use sdk::materializer::{MaterializationEdit as Edit, MaterializationIntent as Intent};
    let mut bytes = 0_u64;
    let mut add = |count: usize, width: usize| -> Result<(), sdk::NativeTreeMaterializationError> {
        bytes = bytes
            .checked_add(count.checked_mul(width).ok_or(sdk::WorkError::Overflow)? as u64)
            .ok_or(sdk::WorkError::Overflow)?;
        Ok(())
    };
    add(journal.plan.edits.len(), std::mem::size_of::<Edit>())?;
    for edit in &journal.plan.edits {
        if let Edit::Install { path, image } | Edit::SetMetadata { path, image } = edit {
            add(path.len(), 1)?;
            add(image.len(), 1)?;
        } else if let Edit::Remove { path } = edit {
            add(path.len(), 1)?;
        } else if let Edit::Rename { from, to } = edit {
            add(from.len(), 1)?;
            add(to.len(), 1)?;
        } else {
            return Err(sdk::NativeTreeMaterializationError::InvalidLayout);
        }
    }
    if let Intent::NativePreparation {
        selection,
        original,
    } = &journal.plan.intent
    {
        add(
            selection.len(),
            std::mem::size_of::<sdk::materializer::MaterializationPreparationSelection>(),
        )?;
        add(original.len(), 1)?;
        for item in selection {
            add(item.provider_identity.len(), 1)?;
        }
    }
    add(
        journal.preimages.len(),
        std::mem::size_of::<sdk::MaterializationPreimage>(),
    )?;
    for image in &journal.preimages {
        add(image.image.len(), 1)?;
    }
    Ok(bytes)
}
