//! Shared fork ordering and definite-refusal handling. Unknown dispatch is never retried.
use crate::{IdempotencyKey, ProviderError};
use std::{future::Future, time::Duration};

/// Every existing node forks at most once per round. Node zero is the parent;
/// node n+1 is child n. All starts in a round are joined before reporting failure.
pub async fn fork_tree<T, F, O>(parent: T, count: u32, fork: F) -> Result<Vec<T>, ProviderError>
where
    T: Clone + Send,
    F: Fn(u32, T) -> O + Send + Sync,
    O: Future<Output = Result<T, ProviderError>> + Send,
{
    let mut nodes = vec![parent];
    for round in fork_rounds(count) {
        let starts = round.into_iter().map(|(index, source)| {
            nodes.get(source).cloned().map(|source| fork(index, source)).ok_or_else(||
                ProviderError::Rejected("fork tree source is absent; recovery: report the invariant failure before retrying".into()))
        }).collect::<Result<Vec<_>, _>>()?;
        let mut failure = None;
        let mut children = Vec::new();
        for result in futures::future::join_all(starts).await {
            match result {
                Ok(child) => children.push(child),
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        nodes.extend(children);
    }
    Ok(nodes.into_iter().skip(1).collect())
}

#[must_use]
pub fn fork_rounds(count: u32) -> Vec<Vec<(u32, usize)>> {
    let mut rounds = Vec::new();
    let mut produced: u32 = 0;
    while produced < count {
        let batch = produced.saturating_add(1).min(count - produced);
        rounds.push(
            (0..batch)
                .map(|source| (produced + source, source as usize))
                .collect(),
        );
        produced += batch;
    }
    rounds
}

/// Refused means the native service definitively did not start a mutation.
/// Transport failure, conflict and a lost reply must remain errors instead.
pub enum Dispatch<T> {
    Accepted(T),
    Refused,
}

pub async fn retry_refused<T, F, O>(
    key: IdempotencyKey,
    interval: Duration,
    timeout: Duration,
    mut dispatch: F,
) -> Result<T, ProviderError>
where
    T: Send,
    F: FnMut() -> O + Send,
    O: Future<Output = Result<Dispatch<T>, ProviderError>> + Send,
{
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| {
            ProviderError::Invalid(
                "fork deadline overflow; recovery: choose a finite readiness timeout".into(),
            )
        })?;
    if interval.is_zero() || timeout.is_zero() {
        return Err(ProviderError::Invalid(
            "fork timing admission failed; recovery: set nonzero readiness interval and timeout"
                .into(),
        ));
    }
    let mut backoff = interval;
    loop {
        match tokio::time::timeout_at(deadline, dispatch()).await {
            Ok(Ok(Dispatch::Accepted(value))) => return Ok(value),
            Ok(Ok(Dispatch::Refused)) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(ProviderError::Indeterminate(key)),
        }
        if tokio::time::timeout_at(deadline, tokio::time::sleep(backoff))
            .await
            .is_err()
        {
            return Err(ProviderError::Indeterminate(key));
        }
        backoff = backoff.saturating_mul(2).min(Duration::from_secs(2));
    }
}
