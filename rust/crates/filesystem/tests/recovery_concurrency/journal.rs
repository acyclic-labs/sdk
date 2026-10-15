use super::*;

#[derive(Clone, Default)]
pub(super) struct CrashJournalStore {
    pub(super) inner: Arc<Store>,
    state: Arc<Mutex<CrashJournalState>>,
    write_gate: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Default)]
struct CrashJournalState {
    writes: usize,
    failure: Option<(usize, bool)>,
}

impl CrashJournalStore {
    pub(super) fn fail_at_relative_write(&self, offset: usize, after_write: bool) {
        let mut state = self.state.lock().expect("journal state");
        state.failure = Some((state.writes + offset, after_write));
    }

    pub(super) fn clear_failure(&self) {
        self.state.lock().expect("journal state").failure = None;
    }

    fn fails_before_next_write(&self) -> bool {
        let state = self.state.lock().expect("journal state");
        state.failure == Some((state.writes + 1, false))
    }

    fn did_write(&self) -> bool {
        let mut state = self.state.lock().expect("journal state");
        state.writes += 1;
        state.failure == Some((state.writes, true))
    }
}

#[derive(Debug, Error)]
pub(super) enum CrashStoreError {
    #[error(transparent)]
    Memory(#[from] StoreError),
    #[error("simulated process crash at durable journal write")]
    InjectedCrash(MaterializationJournal),
}

impl MaterializationJournalStore for CrashJournalStore {
    type Error = CrashStoreError;
    #[cfg(not(target_arch = "wasm32"))]
    type ActionCompletion<'a> =
        futures::future::BoxFuture<'a, Result<MaterializationJournal, CrashStoreError>>;

    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_action(
        &self,
        journal: MaterializationJournal,
    ) -> Result<Self::ActionCompletion<'_>, Self::Error> {
        let write_gate = self.write_gate.lock().await;
        // Retain the failure packet before the physical action, on its original ledger.
        let before = if self.fails_before_next_write() {
            self.inner.load(journal.plan.operation_id).await?
        } else {
            None
        };
        let actual = self.inner.prepare_action(journal).await?;
        Ok(Box::pin(async move {
            let _write_gate = write_gate;
            if let Some(journal) = before {
                drop(actual);
                return Err(CrashStoreError::InjectedCrash(journal));
            }
            let journal = actual.await?;
            if self.did_write() {
                Err(CrashStoreError::InjectedCrash(journal))
            } else {
                Ok(journal)
            }
        }))
    }

    async fn load(
        &self,
        operation: OperationId,
    ) -> Result<Option<MaterializationJournal>, Self::Error> {
        self.inner
            .load(operation)
            .await
            .map_err(CrashStoreError::from)
    }

    async fn compare_and_swap(
        &self,
        operation: OperationId,
        expected: u64,
        replacement: MaterializationJournal,
    ) -> Result<(bool, MaterializationJournal), Self::Error> {
        let _write_gate = self.write_gate.lock().await;
        if self.fails_before_next_write() {
            let current = self.inner.load(operation).await?;
            if current.as_ref().map_or(0, |journal| journal.revision) == expected {
                return Err(CrashStoreError::InjectedCrash(replacement));
            }
        }
        let (stored, replacement) = self
            .inner
            .compare_and_swap(operation, expected, replacement)
            .await?;
        if stored && self.did_write() {
            return Err(CrashStoreError::InjectedCrash(replacement));
        }
        Ok((stored, replacement))
    }
}
