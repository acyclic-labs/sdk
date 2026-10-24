//! Ordinary document queries feed the one shared file fold.
use super::{ClientViews, FileUpdate, file_follow::FileFollow};
use crate::{
    Result,
    task_volume::{FileKey, FileQuerySource, Query},
};
use futures::FutureExt as _;
use std::sync::Arc;

/// Authorized goal and plan readers retained at their last applied cuts.
pub struct DocumentFiles {
    owner: Arc<()>,
    follows: Vec<FileFollow>,
}
/// One selected document delivery. Apply it before waiting again.
pub struct DocumentUpdate {
    owner: Arc<()>,
    index: usize,
    update: FileUpdate,
}
impl DocumentFiles {
    /// Observe arbitrary document selections through the same resumable reader.
    pub async fn observe(
        files: &(impl FileQuerySource + ?Sized),
        views: &mut ClientViews,
        queries: &[(Query, usize)],
        maximum_bytes: usize,
    ) -> Result<Self> {
        let mut follows = Vec::with_capacity(queries.len());
        // Validate all initial snapshots before publishing any selection.
        let mut replacement = views.clone();
        for (query, maximum) in queries {
            let (follow, snapshot) =
                FileFollow::open(files, query.clone(), None, *maximum, maximum_bytes).await?;
            let snapshot = snapshot
                .ok_or_else(|| crate::Error::Conflict("document snapshot missing".into()))?;
            replacement.query_snapshot(query.clone(), snapshot, *maximum, maximum_bytes)?;
            follows.push(follow);
        }
        *views = replacement;
        Ok(Self {
            owner: Arc::new(()),
            follows,
        })
    }
    /// Read without advancing a committed fold; cancellation retains delivery.
    pub async fn read_change(&mut self) -> Result<DocumentUpdate> {
        let reads: Vec<_> = self
            .follows
            .iter_mut()
            .enumerate()
            .map(|(index, follow)| async move { (index, follow.read().await) }.boxed_local())
            .collect();
        if reads.is_empty() {
            return std::future::pending().await;
        }
        let ((index, update), _, _) = futures::future::select_all(reads).await;
        Ok(DocumentUpdate {
            owner: Arc::clone(&self.owner),
            index,
            update: update?,
        })
    }
    /// Atomically apply the selected delivery, then commit its resume cursor.
    pub fn apply_change(
        &mut self,
        update: DocumentUpdate,
        views: &mut ClientViews,
    ) -> Result<bool> {
        if !Arc::ptr_eq(&self.owner, &update.owner) {
            return Err(crate::Error::Invalid(
                "document reader does not own this delivery".into(),
            ));
        }
        self.follows
            .get_mut(update.index)
            .ok_or_else(|| {
                crate::Error::Invalid("document reader does not own this delivery".into())
            })?
            .apply(update.update, views)
    }
    /// Wait and fold outside a competing event-loop wait.
    pub async fn next_change(&mut self, views: &mut ClientViews) -> Result<bool> {
        let update = self.read_change().await?;
        self.apply_change(update, views)
    }
}
/// Install atomic goal/plan snapshots and retain their authorized readers.
pub async fn document_files(
    files: &(impl FileQuerySource + ?Sized),
    views: &mut ClientViews,
    maximum_plans: usize,
    maximum_bytes: usize,
) -> Result<DocumentFiles> {
    DocumentFiles::observe(
        files,
        views,
        &[
            (Query::File(FileKey::Goal), 1),
            (Query::Plans, maximum_plans),
        ],
        maximum_bytes,
    )
    .await
}
