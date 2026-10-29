use std::{future::Future, sync::Arc};

type Owner = Arc<dyn Send + Sync>;

tokio::task_local! {
    static OWNER: Owner;
}

/// Retains an application's original lifetime owner through its accepted actors.
///
/// Pass the existing completion sender or custody guard. Every explicit
/// [`super::ActorExecutor::spawn`] within this scope retains that same owner
/// until its original future is destroyed, including after observer loss or
/// cancellation. Ordinary background tasks do not inherit this lifetime, so a
/// transport cannot retain its own application-close producer.
///
/// One shared owner allocation is made per scope. Actor submission clones that
/// owner without another allocation, channel, registration or completion scan.
pub fn with_actor_lifetime<F: Future>(
    owner: impl Send + Sync + 'static,
    future: F,
) -> impl Future<Output = F::Output> {
    OWNER.scope(Arc::new(owner), future)
}

pub(super) fn owner() -> Option<Owner> {
    OWNER.try_with(Arc::clone).ok()
}

pub(super) fn spawn_on<F>(
    runtime: &tokio::runtime::Handle,
    future: F,
) -> crate::TaskHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // Choose before task storage is allocated. A delivery with no scoped owner
    // needs only its original future, without space for task-local bookkeeping.
    match owner() {
        Some(owner) => crate::task::spawn_on(runtime, OWNER.scope(owner, future)),
        None => crate::task::spawn_on(runtime, future),
    }
}
