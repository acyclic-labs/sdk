/// Test seam at the journal append: every frame `LocalStream` persists, and
/// only those, passes through it. That includes conflicting commits retained
/// with their idempotency receipts; an elided replay persists nothing.
///
/// While a cut is installed, every frame is flushed before it is
/// acknowledged, and a crash models power loss: a frame written but not yet
/// flushed does not survive.
#[cfg(feature = "test-support")]
pub trait PersistenceCut: Send + Sync {
    /// Called before a frame is written. `false` crashes the provider instead:
    /// the frame is not written and every later call is unavailable.
    fn admit(&self) -> bool;
    /// Called after a frame is written, before it is flushed. `false` crashes
    /// the provider and the unflushed frame is lost.
    fn written(&self) -> bool {
        true
    }
    /// Called after the frame is flushed. `false` crashes the provider before
    /// the caller observes the durable write.
    fn acknowledge(&self) -> bool;
    /// Called once per crash with whether the power loss was simulated
    /// faithfully. An `Err` means the reopened journal may retain bytes the
    /// crash should have lost, so the cut proves nothing.
    fn crashed(&self, simulation: Result<(), CutSimulationError>);
}

/// Why a crash could not be simulated faithfully.
#[cfg(feature = "test-support")]
#[derive(Debug, Error)]
pub enum CutSimulationError {
    /// The journal lock was poisoned, so nothing was discarded.
    #[error("the journal lock is poisoned")]
    Lock,
    /// The unflushed journal bytes could not be discarded.
    #[error("discarding unflushed journal bytes failed: {0}")]
    Truncate(#[source] std::io::Error),
    /// The blocking task simulating the crash did not finish.
    #[error("the crash simulation task failed")]
    Task,
}

#[cfg(feature = "test-support")]
thread_local! {
    static FAIL_CUT_TRUNCATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Fault switch for negative controls: crashes simulated by this thread fail
/// to discard their unflushed journal bytes.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn fail_cut_truncation(fail: bool) {
    FAIL_CUT_TRUNCATION.with(|flag| flag.set(fail));
}

#[cfg(feature = "test-support")]
impl LocalStream {
    /// Routes every later journal append of this provider through `cut`.
    /// Every frame written so far must already be flushed.
    pub fn cut_persistence(&self, cut: Arc<dyn PersistenceCut>) {
        {
            let mut journal = self.inner.journal.lock().expect("journal lock");
            assert!(
                journal
                    .flushed_sequence
                    .is_some_and(|cut| cut >= journal.appended),
                "a frame written before the cut is not flushed"
            );
            journal.flushed = journal.bytes;
        }
        assert!(
            self.inner.persistence_cut.set(cut).is_ok(),
            "persistence is already cut"
        );
    }

    /// Power loss: discard every journal byte of this epoch not yet flushed,
    /// report whether that succeeded, then poison.
    async fn cut_crash(&self, cut: &dyn PersistenceCut) -> StreamError {
        let fail = FAIL_CUT_TRUNCATION.with(std::cell::Cell::get);
        let journal = self.inner.journal.clone();
        let simulated = spawn_blocking(move || {
            let journal = journal.lock().map_err(|_| CutSimulationError::Lock)?;
            if fail {
                return Err(CutSimulationError::Truncate(std::io::Error::other(
                    "injected truncation failure",
                )));
            }
            journal
                .file
                .set_len(journal.flushed)
                .map_err(CutSimulationError::Truncate)
        })
        .await
        .map_err(|_| CutSimulationError::Task)
        .and_then(|simulated| simulated);
        cut.crashed(simulated);
        self.inner.poison()
    }

    /// Finishes one frame written while a cut is installed: flush, then
    /// acknowledge.
    async fn cut_after_write(&self, cut: Arc<dyn PersistenceCut>) -> Result<(), StreamError> {
        if !cut.written() {
            return Err(self.cut_crash(cut.as_ref()).await);
        }
        let stream = self.clone();
        spawn_blocking(move || stream.flush_all(&tracing::Span::none()))
            .await
            .map_err(|_| self.inner.poison())??;
        if !cut.acknowledge() {
            return Err(self.cut_crash(cut.as_ref()).await);
        }
        self.inner.published();
        Ok(())
    }
}
