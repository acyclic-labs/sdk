//! Disposable private-file views, opened at atomic snapshot/follow cuts.
use super::*;
use crate::{
    client::FileQueryView,
    task_volume::{Change, ChangeStream, FileQuerySource, Query},
};

/// Bounded folds used by command view consumers. Authority stays in the files.
#[derive(Default)]
pub struct CommandViewFiles(
    /// Query folds, each retaining its own exact cursor.
    pub Vec<FileQueryView>,
);
impl CommandViewFiles {
    /// Observe every selected query before exposing its snapshot to rendering.
    pub async fn observe(
        source: &(impl FileQuerySource + ?Sized),
        queries: &[Query],
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> Result<(Self, ChangeStream)> {
        let mut views = Vec::new();
        let mut follows = Vec::new();
        for query in queries {
            let observed = source
                .observe_files(query.clone(), None, maximum_files, maximum_bytes)
                .await
                .map_err(storage)?;
            let snapshot = observed
                .snapshot
                .ok_or_else(|| Error::Conflict("view snapshot missing".into()))?;
            views.push(FileQueryView::from_snapshot(
                query.clone(),
                snapshot,
                maximum_files,
                maximum_bytes,
            )?);
            follows.push(observed.changes);
        }
        Ok((Self(views), Box::pin(futures::stream::select_all(follows))))
    }
    /// Fold only into the query whose cursor owns this publication.
    pub fn apply(&mut self, change: Change) -> Result<bool> {
        self.0
            .iter_mut()
            .find(|view| view.cursor().query == change.cursor.query)
            .ok_or_else(|| Error::Conflict("view follow has no snapshot".into()))?
            .apply(change)
    }
    /// True when no selection changed; the existing follows remain live.
    pub fn selects(&self, queries: &[Query]) -> bool {
        self.0.len() == queries.len()
            && queries
                .iter()
                .all(|query| self.0.iter().any(|view| view.query() == query))
    }
}
fn storage(error: crate::task_volume::Error) -> Error {
    Error::Storage(error.to_string())
}
