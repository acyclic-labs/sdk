//! One disposable file-query fold with publish-before-cursor delivery.

use super::{FileViews, PublishError, QuerySubscription, QueryViewLimits, query_error};
use crate::{
    Result,
    task_volume::{Change, Cursor, Generation, HeadFile, HeadSnapshot, Query, TaskVolume},
};
use acyclic_stream::StreamProvider;

/// A single task-volume subscription shared by native and WASM renderers.
/// Source files stay authoritative; this cache is reconstructed at its cursor.
pub struct ClientQuery {
    subscription: QuerySubscription,
    views: FileViews,
    limits: QueryViewLimits,
}

impl ClientQuery {
    /// Loads a snapshot at the retained cursor, or an explicit replacement cut
    /// when history cannot be resumed, then follows from that same cut.
    pub async fn open<P: StreamProvider + ?Sized>(
        volume: &TaskVolume<P>,
        query: Query,
        cursor: Option<Cursor>,
        limits: QueryViewLimits,
    ) -> Result<Self> {
        let window = matches!(query, Query::TaskSession { .. });
        let (subscription, snapshot) =
            QuerySubscription::open(volume, query, cursor, limits.segments).await?;
        let mut views = FileViews::snapshot(snapshot, limits).map_err(query_error)?;
        views.window = window;
        Ok(Self {
            subscription,
            views,
            limits,
        })
    }

    /// Captures the accepted view cut. Segment slices retain their original
    /// backing allocation; snapshotting clones only bounded file/segment metadata.
    pub fn snapshot(&self) -> HeadSnapshot {
        let cursor = self.cursor().clone();
        HeadSnapshot {
            files: self.files().cloned().collect(),
            generation: Generation {
                volume: cursor.volume,
                epoch: cursor.epoch,
                position: cursor.position,
                commit: cursor.commit,
            },
            cursor,
        }
    }

    /// Last completely published view cut. A failed publisher leaves it unchanged.
    pub fn cursor(&self) -> &Cursor {
        self.subscription.cursor()
    }

    /// Whether opening/resuming replaced an incompatible history rather than resuming it.
    pub fn reset(&self) -> bool {
        self.subscription.reset()
    }

    /// Borrow one logical file, including ordered text/reasoning segments.
    pub fn file(&self, key: &crate::task_volume::FileKey) -> Option<&HeadFile> {
        self.views.file(key)
    }

    /// Visit retained files without copying their bytes or serializing the transcript.
    pub fn files(&self) -> impl Iterator<Item = &HeadFile> {
        self.views.files()
    }

    /// Validates and publishes one exact change before advancing the views/cursor.
    /// Failed publication retains the same frame for retry, even when later records
    /// have arrived. The shared reducer remains the only file-fold implementation.
    pub async fn publish_next<F>(&mut self, mut publish: F) -> Result<()>
    where
        F: for<'a> std::ops::AsyncFnMut(&'a Change) -> Result<()>,
    {
        let views = &mut self.views;
        self.subscription
            .publish_next(async |change| {
                let apply = views
                    .prepare(change)
                    .map_err(query_error)
                    .map_err(PublishError::Rejected)?;
                publish(change).await.map_err(PublishError::Failed)?;
                apply(views)
                    .map_err(query_error)
                    .map_err(PublishError::Failed)
            })
            .await
    }

    /// Resumes matching views directly, or atomically installs a validated reset
    /// snapshot. Pending output must be published before reconnecting.
    pub async fn reconnect<P: StreamProvider + ?Sized>(
        &mut self,
        volume: &TaskVolume<P>,
    ) -> Result<bool> {
        let views = &mut self.views;
        self.subscription
            .reconnect(volume, |snapshot| {
                let window = views.window;
                let mut replacement =
                    FileViews::snapshot(snapshot, self.limits).map_err(query_error)?;
                replacement.window = window;
                *views = replacement;
                Ok(())
            })
            .await
    }
}
