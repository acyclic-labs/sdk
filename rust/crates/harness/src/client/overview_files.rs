//! Task status files are one ordinary authorized snapshot-plus-follow query,
//! folded into the shared ClientViews like every other file query.
use super::ViewUpdate;
use crate::{
    Error, Result,
    task_volume::{FileQuerySource, Query},
};
use acyclic_stream::BoxProviderStream;
use futures::{StreamExt, stream};

/// Follow every task's status file (lifecycle, parent and attention); task
/// archives and transcripts are never loaded.
pub async fn task_overview_files(
    files: &(impl FileQuerySource + ?Sized),
    maximum_files: usize,
    maximum_bytes: usize,
) -> Result<BoxProviderStream<'static, Result<ViewUpdate>>> {
    let observation = files
        .observe_files(Query::TaskOverviews, None, maximum_files, maximum_bytes)
        .await?;
    let snapshot = ViewUpdate::QuerySnapshot {
        query: Query::TaskOverviews,
        snapshot: observation
            .snapshot
            .ok_or_else(|| Error::Conflict("task overview snapshot missing".into()))?,
        maximum_files,
        maximum_bytes,
    };
    // Checkpoints are applied too: the fold's cursor must advance by exactly
    // one publication. Its revision only moves when status files change.
    let changes = observation
        .changes
        .map(|change| change.map(ViewUpdate::File).map_err(Into::into));
    Ok(Box::pin(
        stream::once(async { Ok(snapshot) }).chain(changes),
    ))
}

#[cfg(all(
    test,
    feature = "test-support",
    not(acyclic_harness_loom),
    not(target_arch = "wasm32")
))]
mod tests;
