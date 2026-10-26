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
    static DEFERRED: Deferral;
}

#[derive(Clone, Copy)]
enum Deferral {
    Eager,
    All,
    Provider([u8; 16]),
}

fn current_deferral() -> Deferral {
    DEFERRED.try_with(|value| *value).unwrap_or(Deferral::Eager)
}

// Every publication worker inherits the caller's exact provider scope. Native
// guarded and prepared commits must use the same boundary as ordinary writes.
fn publication_scope<F>(future: F) -> impl Future<Output = F::Output> + Send
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    DEFERRED.scope(current_deferral(), future)
}

/// Leaves local-stream mutations accepted until an explicit durability barrier.
pub async fn deferring_durability<F: Future>(future: F) -> F::Output {
    DEFERRED.scope(Deferral::All, future).await
}

/// Requires each mutation in `future` to admit and flush its own durable
/// publication, including when its caller is deferring unrelated writes.
pub async fn requiring_durability<F: Future>(future: F) -> F::Output {
    DEFERRED.scope(Deferral::Eager, future).await
}

/// Groups only mutations of this provider owner. Unrelated local journals keep
/// their normal durability, including filesystem authorities used by `future`.
/// The caller must obtain an explicit receipt before certifying publication.
pub async fn deferring_provider_durability<P: StreamProvider + ?Sized, F: Future>(
    provider: &P,
    future: F,
) -> F::Output {
    DEFERRED
        .scope(
            Deferral::Provider(provider.operation_claims().owner_identity()),
            future,
        )
        .await
}

fn deferred(provider: &LocalStream) -> bool {
    match current_deferral() {
        Deferral::Eager => false,
        Deferral::All => true,
        Deferral::Provider(owner) => provider.operation_claims().owner_identity() == owner,
    }
}
