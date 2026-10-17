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
}

#[cfg(feature = "test-support")]
impl LocalStream {
    /// Routes every later journal append of this provider through `cut`.
    /// Every frame written so far must already be durable.
    pub fn cut_persistence(&self, cut: Arc<dyn PersistenceCut>) {
        let length = self
            .inner
            .journal
            .lock()
            .ok()
            .and_then(|journal| (&journal.file).stream_position().ok())
            .expect("journal position");
        self.inner
            .durable_len
            .store(length, std::sync::atomic::Ordering::SeqCst);
        assert!(
            self.inner.persistence_cut.set(cut).is_ok(),
            "persistence is already cut"
        );
    }

    /// Power loss: discard every journal byte not yet flushed, then poison.
    async fn cut_crash(&self) -> StreamError {
        let durable = self
            .inner
            .durable_len
            .load(std::sync::atomic::Ordering::SeqCst);
        let journal = self.inner.journal.clone();
        let _ = spawn_blocking(move || {
            if let Ok(journal) = journal.lock() {
                let _ = journal.file.set_len(durable);
            }
        })
        .await;
        self.inner.poison()
    }

    /// Finishes one frame written while a cut is installed: flush, then
    /// acknowledge.
    async fn cut_after_write(&self, cut: Arc<dyn PersistenceCut>) -> Result<(), StreamError> {
        if !cut.written() {
            return Err(self.cut_crash().await);
        }
        let stream = self.clone();
        spawn_blocking(move || stream.flush_all(&tracing::Span::none()))
            .await
            .map_err(|_| self.inner.poison())??;
        if !cut.acknowledge() {
            return Err(self.cut_crash().await);
        }
        self.inner.published();
        Ok(())
    }
}
