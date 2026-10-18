thread_local! {
    static INLINE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) async fn scheduled<F>(work: F) -> super::TestResult<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    if INLINE.get() {
        return Ok(work.await);
    }
    // The shared dispatcher admits non-Send drivers on one event-loop thread.
    // Goal drivers are Send: use the existing runtime's workers, preserving the
    // native schedule and aborting the owned task if its caller is cancelled.
    let task = acyclic_native_runtime::testing::spawn(work);
    Ok(tokio_util::task::AbortOnDropHandle::new(task).await?)
}

#[cfg(test)]
pub(super) fn with_inline<T>(inline: bool, work: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            INLINE.set(self.0);
        }
    }
    let _restore = Restore(INLINE.replace(inline));
    work()
}
