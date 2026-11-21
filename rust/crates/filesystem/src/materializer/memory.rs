use super::sdk;
use acyclic_fs_format::identity::OperationId;
use sdk::{MaterializationJournal as Journal, MaterializationJournalStore};
use std::collections::BTreeMap;

#[derive(Default)]
pub struct Store(parking_lot::Mutex<BTreeMap<OperationId, Journal>>);
pub struct StoreError {
    pub journal: Option<Journal>,
    pub cause: Option<sdk::MaterializationJournalCopyError>,
}

impl StoreError {
    #[cfg(not(target_arch = "wasm32"))]
    fn conflict(journal: Journal) -> Self {
        Self {
            journal: Some(journal),
            cause: None,
        }
    }
}

impl From<sdk::MaterializationJournalCopyError> for StoreError {
    fn from(cause: sdk::MaterializationJournalCopyError) -> Self {
        Self {
            journal: None,
            cause: Some(cause),
        }
    }
}

impl std::fmt::Debug for StoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.cause {
            Some(cause) => formatter.debug_tuple("NativeWork").field(cause).finish(),
            None => {
                let mut debug = formatter.debug_tuple("Conflict");
                if let Some(journal) = &self.journal {
                    debug.field(journal);
                }
                debug.finish()
            }
        }
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.cause {
            Some(cause) => std::fmt::Display::fmt(cause, formatter),
            None => formatter.write_str("journal changed"),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.as_ref().and_then(std::error::Error::source)
    }
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
        let operation = journal.plan().operation_id;
        if !self.matches(&journal).await? {
            return Err(StoreError::conflict(journal));
        }
        let mut replacement = journal.try_clone()?;
        journal.revision = journal
            .revision
            .checked_add(1)
            .ok_or(sdk::NativeTreeMaterializationError::InvalidLayout)?;
        replacement.revision = journal.revision;
        Ok(Box::pin(async move {
            if let Some(allocation) = journal.custody() {
                journal.work = allocation.work().journal_work()?;
                replacement.work = journal.work;
            }
            let mut records = self.0.lock();
            if records.get(&operation).map(|current| current.revision) != Some(journal.revision - 1)
            {
                return Err(StoreError::conflict(journal));
            }
            records.insert(operation, replacement);
            Ok(journal)
        }))
    }
    async fn load(&self, operation: OperationId) -> Result<Option<Journal>, StoreError> {
        self.0
            .lock()
            .get(&operation)
            .map(Journal::try_clone)
            .transpose()
            .map_err(StoreError::from)
    }
    async fn matches(&self, journal: &Journal) -> Result<bool, StoreError> {
        Ok(self
            .0
            .lock()
            .get(&journal.plan().operation_id)
            .is_some_and(|current| current.shares_history_with(journal) && current == journal))
    }

    async fn compare_and_swap(
        &self,
        operation: OperationId,
        expected: u64,
        replacement: Journal,
    ) -> Result<(bool, Journal), StoreError> {
        let mut records = self.0.lock();
        let current = records.get(&operation);
        let stored = current.map_or(0, |journal| journal.revision) == expected
            && current.is_none_or(|journal| journal.shares_history_with(&replacement));
        if stored {
            records.insert(operation, replacement.try_clone()?);
        }
        Ok((stored, replacement))
    }
}
