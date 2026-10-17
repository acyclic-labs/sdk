use std::future::Future;

/// Race only this registration; dropping the loser withdraws its queue wait.
pub(super) async fn until<T>(
    registration: impl Future<Output = T>,
    deadline: impl Future<Output = ()>,
) -> Option<T> {
    futures::pin_mut!(registration, deadline);
    match futures::future::select(registration, deadline).await {
        futures::future::Either::Left((result, _)) => Some(result),
        futures::future::Either::Right(_) => None,
    }
}
