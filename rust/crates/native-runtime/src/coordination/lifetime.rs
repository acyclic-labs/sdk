use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

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

pin_project_lite::pin_project! {
    #[project = InheritedProjection]
    enum Inherited<F> {
        Scoped { #[pin] future: tokio::task::futures::TaskLocalFuture<Owner, F> },
        Plain { #[pin] future: F },
    }
}

impl<F: Future> Future for Inherited<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            InheritedProjection::Scoped { future } => future.poll(context),
            InheritedProjection::Plain { future } => future.poll(context),
        }
    }
}

pub(super) fn inherit<F: Future>(future: F) -> impl Future<Output = F::Output> {
    match OWNER.try_with(Arc::clone) {
        Ok(owner) => Inherited::Scoped {
            future: OWNER.scope(owner, future),
        },
        Err(_) => Inherited::Plain { future },
    }
}
