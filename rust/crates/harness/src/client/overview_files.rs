//! Task status files are one ordinary authorized snapshot-plus-follow query,
//! folded into the shared ClientViews like every other file query.
use super::ClientViews;
use crate::{
    Error, Result,
    task_volume::{ChangeStream, FileQuerySource, Query},
};

/// Install every task's status file (lifecycle, parent and attention) at one
/// atomic cut, then follow that exact cursor; task archives and transcripts
/// are never loaded. Checkpoints are applied too, so the fold advances by
/// exactly one publication; its revision moves only when status files change.
pub async fn task_overview_files(
    files: &(impl FileQuerySource + ?Sized),
    views: &mut ClientViews,
    maximum_files: usize,
    maximum_bytes: usize,
) -> Result<ChangeStream> {
    let observation = files
        .observe_files(Query::TaskOverviews, None, maximum_files, maximum_bytes)
        .await?;
    let snapshot = observation
        .snapshot
        .ok_or_else(|| Error::Conflict("task overview snapshot missing".into()))?;
    views.query_snapshot(Query::TaskOverviews, snapshot, maximum_files, maximum_bytes)?;
    Ok(observation.changes)
}

#[cfg(all(
    test,
    feature = "test-support",
    not(acyclic_harness_loom),
    not(target_arch = "wasm32")
))]
mod tests;
