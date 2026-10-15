/// How the journal is made durable before a mutation becomes observable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LocalDurability {
    /// Every publication waits for the storage device to flush its cache (`F_FULLFSYNC` on
    #[default]
    FullFlush,
    /// Every publication is ordered behind earlier writes without waiting for the device
    Barrier,
}

tokio::task_local! {
    /// Whether mutations made on this task leave their frames unflushed; see
    static DEFERRED: bool;
}

/// Defers synchronization of local-stream mutations made by `future`.
/// A subsequent mutation still synchronizes its predecessor before writing another
/// frame. Call [`LocalStream::flush`] to apply the configured durability policy
/// to the final deferred frame.
pub async fn deferring_durability<F: Future>(future: F) -> F::Output {
    DEFERRED.scope(true, future).await
}

fn deferred() -> bool {
    DEFERRED.try_with(|deferred| *deferred).unwrap_or(false)
}
