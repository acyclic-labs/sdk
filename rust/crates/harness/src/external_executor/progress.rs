//! Passive requests recheck the state after establishing their push follow.
use crate::{Error, Result};
use futures::{Stream, StreamExt};

pub(crate) async fn follow<
    C,
    T,
    F: Future<Output = Result<(C, Option<T>)>>,
    S: Stream<Item = Result<()>> + Unpin,
>(
    mut cursor: C,
    mut step: impl FnMut(C) -> F,
    subscribe: impl Future<Output = Result<S>>,
    stopped: impl Future<Output = Result<()>>,
) -> Result<Option<T>> {
    let (next, result) = step(cursor).await?;
    cursor = next;
    if let Some(result) = result {
        return Ok(Some(result));
    }
    let mut changes = subscribe.await?;
    tokio::pin!(stopped);
    loop {
        // The task can become ready between the first snapshot and this cut.
        let (next, result) = step(cursor).await?;
        cursor = next;
        if let Some(result) = result {
            return Ok(Some(result));
        }
        tokio::select! {
            biased;
            stopped = &mut stopped => {stopped?; return Ok(None);}
            change = changes.next() => {
                change.ok_or_else(|| Error::Storage("workflow follow ended".into()))??;
            }
        }
    }
}

// This cursor records the cut observed before work. A concurrent change during
// the await must remain unprocessed, even if the work reads some newer state.
pub(crate) async fn at_revision<T, F: Future<Output = Result<Option<T>>>>(
    previous: Option<u64>,
    observed: Option<u64>,
    advance: impl FnOnce() -> F,
) -> Result<(Option<u64>, Option<T>)> {
    if observed.is_some() && previous == observed {
        return Ok((previous, None));
    }
    let result = advance().await?;
    Ok((observed, result))
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
