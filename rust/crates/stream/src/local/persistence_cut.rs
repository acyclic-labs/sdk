/// Test seam at the journal append: every frame `LocalStream` persists, and
/// only those, passes through it. That includes conflicting commits retained
/// with their idempotency receipts; an elided replay persists nothing.
#[cfg(feature = "test-support")]
pub trait PersistenceCut: Send + Sync {
    /// Called before a frame is written. `false` crashes the provider instead:
    /// the frame is not written and every later call is unavailable.
    fn admit(&self) -> bool;
    /// Called after a frame is written. `false` crashes the provider before
    /// the caller observes the write.
    fn acknowledge(&self) -> bool;
}

#[cfg(feature = "test-support")]
impl LocalStream {
    /// Routes every later journal append of this provider through `cut`.
    pub fn cut_persistence(&self, cut: Arc<dyn PersistenceCut>) {
        assert!(
            self.inner.persistence_cut.set(cut).is_ok(),
            "persistence is already cut"
        );
    }
}
