//! Retry only an observed coordinator advance, rechecking the original fence.
use crate::Result;
use acyclic_stream::{CommitConflict, StreamPath};

pub(super) async fn retry<const RETRY: bool, S, F, Fut>(
    mut state: S,
    mut attempt: F,
) -> Result<bool>
where
    F: FnMut(S) -> Fut,
    Fut: Future<Output = Result<(Option<bool>, S)>>,
{
    loop {
        let (result, next) = attempt(state).await?;
        state = next;
        match result {
            Some(result) => return Ok(result),
            None if RETRY => {}
            None => return Ok(false),
        }
    }
}

pub(super) fn advanced(conflicts: &[CommitConflict], path: &StreamPath, tail: u64) -> bool {
    !conflicts.is_empty()
        && conflicts.iter().all(|conflict| {
            matches!(conflict,
        CommitConflict::Tail {path: actual_path, expected, actual: Some(actual)}
            if actual_path == path && *expected == tail && *actual > tail)
        })
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
