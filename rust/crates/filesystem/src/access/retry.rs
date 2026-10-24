use crate::CancellationToken;
use std::future::Future;

#[derive(Debug)]
pub(crate) enum RetryError<E> {
    CallerCancelled,
    MountCancelled,
    Observation(E),
}

pub(crate) fn observe_policy<T, E, F: Future<Output = Result<Option<T>, E>>>(
    caller: &CancellationToken,
    mount: &CancellationToken,
    observe: impl FnMut() -> F,
) -> impl Future<Output = Result<T, RetryError<E>>> {
    retry::<true, _, _, _>(caller, mount, observe)
}

#[cfg(test)]
pub(crate) fn observe_policy_unfenced<T, E, F: Future<Output = Result<Option<T>, E>>>(
    caller: &CancellationToken,
    mount: &CancellationToken,
    observe: impl FnMut() -> F,
) -> impl Future<Output = Result<T, RetryError<E>>> {
    retry::<false, _, _, _>(caller, mount, observe)
}

async fn retry<const CHECK_MOUNT: bool, T, E, F: Future<Output = Result<Option<T>, E>>>(
    caller: &CancellationToken,
    mount: &CancellationToken,
    mut observe: impl FnMut() -> F,
) -> Result<T, RetryError<E>> {
    loop {
        caller.check().map_err(|_| RetryError::CallerCancelled)?;
        if CHECK_MOUNT {
            mount.check().map_err(|_| RetryError::MountCancelled)?;
        }
        if let Some(observed) = observe().await.map_err(RetryError::Observation)? {
            return Ok(observed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mount_revocation_fences_policy_observation() {
        #[cfg(acyclic_fs_loom)]
        loom::model(check_mount_revocation);
        #[cfg(not(acyclic_fs_loom))]
        check_mount_revocation();
    }

    fn check_mount_revocation() {
        use std::task::{Context, Poll, Waker};
        let caller = CancellationToken::new();
        let mount = CancellationToken::new();
        mount.cancel();
        let mut context = Context::from_waker(Waker::noop());
        let current = std::pin::pin!(observe_policy(&caller, &mount, || {
            std::future::ready(Ok::<_, ()>(Some(1)))
        }))
        .poll(&mut context);
        assert!(matches!(current, Poll::Ready(Err(RetryError::MountCancelled))));
        // Negative control: omitting the mount fence admits the same observation.
        let unfenced = std::pin::pin!(observe_policy_unfenced(&caller, &mount, || {
            std::future::ready(Ok::<_, ()>(Some(1)))
        }))
        .poll(&mut context);
        assert!(matches!(unfenced, Poll::Ready(Ok(1))));
    }
}
