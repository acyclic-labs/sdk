//! Task request/result bindings on a retained workspace Streams connection.
use super::storage;
use crate::{Error, Result, TaskId, task_volume::TaskVolume};
use acyclic_stream::{StreamPath, StreamProvider};
use std::sync::Arc;

/// Opens a task's local hook volumes through an authenticated workspace client.
/// Only the owner creates results; cold discovery uses replay plus follow.
pub async fn local_hook_volumes<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    task: TaskId,
) -> Result<(TaskVolume<P>, TaskVolume<P>)> {
    let requests = TaskVolume::open(
        provider.clone(),
        super::super::local_paths::path(super::super::local_paths::REQUESTS, task),
    )
    .await
    .map_err(storage)?;
    let result_path = super::super::local_paths::path(super::super::local_paths::RESULTS, task);
    match TaskVolume::open_existing(provider.clone(), result_path.clone()).await {
        Ok(results) => return Ok((requests, results)),
        Err(crate::task_volume::Error::ResetRequired) => {}
        Err(error) => return Err(storage(error)),
    }
    // Only the owner creates result volumes. Discovery is a live replay plus
    // follow, so cold task binding has no sleep or check/subscription gap.
    use futures::StreamExt as _;
    let mut children = provider
        .children(acyclic_stream::ChildrenRequest {
            parent: Some(StreamPath::new(super::super::local_paths::RESULTS).map_err(storage)?),
            limit: 64,
        })
        .await
        .map_err(storage)?;
    loop {
        let child = children
            .next()
            .await
            .ok_or_else(|| Error::Storage("result discovery ended".into()))?
            .map_err(storage)?;
        if child.path == result_path {
            break;
        }
    }
    let results = TaskVolume::open_existing(provider, result_path)
        .await
        .map_err(storage)?;
    Ok((requests, results))
}
