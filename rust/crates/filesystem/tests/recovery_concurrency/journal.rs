use super::*;

// The fixture controls the journal write seam.
#[derive(Clone, Default)]
pub(super) struct CrashJournalStore {
    pub(super) inner: Arc<Store>,
    pub(super) state: Arc<Mutex<CrashJournalState>>,
    write_gate: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Default)]
pub(super) struct CrashJournalState {
    pub(super) writes: usize,
    failure: Option<(usize, bool)>,
}

impl CrashJournalStore {
    pub(super) fn fail_at_relative_write(&self, offset: usize, after_write: bool) {
        let mut state = self.state.lock().expect("journal fault state");
        state.failure = Some((state.writes + offset, after_write));
    }

    pub(super) fn clear_failure(&self) {
        let mut state = self.state.lock().expect("journal fault state");
        state.failure = None;
    }

    fn fails_before_next_write(&self) -> bool {
        let state = self.state.lock().expect("journal fault state");
        state.failure == Some((state.writes + 1, false))
    }

    fn did_write(&self) -> bool {
        let mut state = self.state.lock().expect("journal fault state");
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

#[cfg(not(target_arch = "wasm32"))]
pub(super) struct CrashActionCompletion<'a> {
    actual: Option<<Store as MaterializationJournalStore>::ActionCompletion<'a>>,
    before_write: Option<MaterializationJournal>,
    store: &'a CrashJournalStore,
    // Freeze the relative write ordinal from admission through the real action.
    _write_gate: tokio::sync::MutexGuard<'a, ()>,
}

#[cfg(not(target_arch = "wasm32"))]
impl std::future::Future for CrashActionCompletion<'_> {
    type Output = Result<MaterializationJournal, CrashStoreError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(journal) = this.before_write.take() {
            drop(this.actual.take());
            return std::task::Poll::Ready(Err(CrashStoreError::InjectedCrash(journal)));
        }
        let actual = this
            .actual
            .as_mut()
            .expect("completion is polled only until ready");
        let std::task::Poll::Ready(result) =
            std::future::Future::poll(std::pin::Pin::new(actual), context)
        else {
            return std::task::Poll::Pending;
        };
        drop(this.actual.take());
        std::task::Poll::Ready(result.map_err(CrashStoreError::from).and_then(|journal| {
            if this.store.did_write() {
                Err(CrashStoreError::InjectedCrash(journal))
            } else {
                Ok(journal)
            }
        }))
    }
}

impl MaterializationJournalStore for CrashJournalStore {
    type Error = CrashStoreError;

    #[cfg(not(target_arch = "wasm32"))]
    type ActionCompletion<'a>
        = CrashActionCompletion<'a>
    where
        Self: 'a;

    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_action<'a>(
        &'a self,
        journal: MaterializationJournal,
    ) -> Result<Self::ActionCompletion<'a>, Self::Error> {
        let write_gate = self.write_gate.lock().await;
        // The fault is armed before admission. Its retained error packet is
        // a real Memory load funded by the original native ledger, never a
        // plain journal clone or an allocation after the host action.
        let before_write = if self.fails_before_next_write() {
            self.inner.load(journal.plan.operation_id).await?
        } else {
            None
        };
        let actual = self.inner.prepare_action(journal).await?;
        Ok(CrashActionCompletion {
            actual: Some(actual),
            before_write,
            store: self,
            _write_gate: write_gate,
        })
    }

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationJournal>, Self::Error> {
        self.inner
            .load(operation_id)
            .await
            .map_err(CrashStoreError::from)
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MaterializationJournal,
    ) -> Result<(bool, MaterializationJournal), Self::Error> {
        let _write_gate = self.write_gate.lock().await;
        if self.fails_before_next_write() {
            // Rejected CAS attempts neither consume a write ordinal nor crash.
            // Query only at an armed pre-write seam, through the actual store.
            let current = self.inner.load(operation_id).await?;
            if current.as_ref().map_or(0, |journal| journal.revision) == expected_revision {
                return Err(CrashStoreError::InjectedCrash(replacement));
            }
        }
        let (stored, replacement) = self
            .inner
            .compare_and_swap(operation_id, expected_revision, replacement)
            .await?;
        if stored && self.did_write() {
            return Err(CrashStoreError::InjectedCrash(replacement));
        }
        Ok((stored, replacement))
    }
}
