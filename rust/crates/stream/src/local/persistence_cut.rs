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
    /// The provider could not establish the original durable prefix.
    #[error("installing the persistence cut failed: {0}")]
    Install(#[source] StreamError),
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
    /// Seals the accepted prefix before installing the cut under the original
    /// mutation fence, so a concurrent writer cannot escape either boundary.
    ///
    /// Returns [`StreamError::AlreadyExists`] if a cut is already installed,
    /// or the provider's failure when the prefix cannot be flushed. Losing the
    /// provider ends a pending fence wait; dropping this future releases it.
    pub async fn cut_persistence(&self, cut: Arc<dyn PersistenceCut>) -> Result<(), StreamError> {
        self.check_available()?;
        let mut failed = self.inner.failed.subscribe();
        let installation = std::pin::pin!(async {
            let _visibility = self.inner.visibility.write().await;
            self.check_available()?;
            if self.inner.persistence_cut.get().is_some() {
                return Err(StreamError::AlreadyExists);
            }
            self.seal_accepted().await?;
            {
                let mut journal = self.inner.journal.lock().map_err(|_| self.inner.poison())?;
                journal.flushed = journal.bytes;
            }
            self.inner
                .persistence_cut
                .set(cut)
                .map_err(|_| StreamError::AlreadyExists)
        });
        let unavailable = std::pin::pin!(LocalInner::unavailable(&mut failed));
        match futures::future::select(unavailable, installation).await {
            futures::future::Either::Left((error, _)) => Err(error),
            futures::future::Either::Right((result, _)) => result,
        }
    }

    /// Power loss: discard every journal byte of this epoch not yet flushed,
    /// report whether that succeeded, then poison.
    // Runs on the persistence worker, outside its journal write section.
    fn cut_crash(&self, cut: &dyn PersistenceCut, fail: bool) -> StreamError {
        let simulated = (|| {
            let journal = self
                .inner
                .journal
                .lock()
                .map_err(|_| CutSimulationError::Lock)?;
            if fail {
                return Err(CutSimulationError::Truncate(std::io::Error::other(
                    "injected truncation failure",
                )));
            }
            let flushed = journal.flushed;
            journal
                .file
                .lease()
                .and_then(|file| file.write(|file| file.set_len(flushed)))
                .map_err(CutSimulationError::Truncate)
        })();
        cut.crashed(simulated);
        self.inner.poison()
    }

    /// Finishes one frame written while a cut is installed: flush, then
    /// acknowledge.
    fn cut_after_write(&self, cut: &dyn PersistenceCut, fail: bool) -> Result<(), StreamError> {
        if !cut.written() {
            return Err(self.cut_crash(cut, fail));
        }
        self.flush_all(&tracing::Span::none())?;
        if !cut.acknowledge() {
            return Err(self.cut_crash(cut, fail));
        }
        Ok(())
    }
}
