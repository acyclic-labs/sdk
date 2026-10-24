//! Retained authorized file readers; delivery is committed only after its fold.
use super::{ClientViews, query_error};
use crate::{
    Error, Result,
    task_volume::{Change, ChangeStream, Cursor, FileQuerySource, Query, Snapshot},
};
use futures::StreamExt as _;
use std::sync::Arc;

/// A source delivers a change, or an explicit atomic replacement after a reset.
#[derive(Clone)]
pub enum FileUpdate {
    /// One publication at its exact accepted cursor.
    Change(Change),
    /// A reset replaces all files in this selection at one checked cut.
    Snapshot(Snapshot),
}
impl FileUpdate {
    /// Whether this delivery only advances the query cursor.
    pub fn is_checkpoint(&self) -> bool {
        matches!(self, Self::Change(change) if change.is_checkpoint())
    }
    pub(crate) fn cursor(&self) -> &Cursor {
        match self {
            Self::Change(change) => &change.cursor,
            Self::Snapshot(snapshot) => &snapshot.cursor,
        }
    }
}

pub(crate) struct FileFollow {
    source: Arc<dyn FileQuerySource>,
    query: Query,
    pub(crate) cursor: Cursor,
    maximum_files: usize,
    maximum_bytes: usize,
    changes: ChangeStream,
    pending: Option<FileUpdate>,
    closed_at: Option<Cursor>,
}
impl FileFollow {
    pub(crate) async fn open(
        source: &(impl FileQuerySource + ?Sized),
        query: Query,
        cursor: Option<Cursor>,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> Result<(Self, Option<Snapshot>)> {
        let observed = source
            .observe_files(query.clone(), cursor.clone(), maximum_files, maximum_bytes)
            .await?;
        if observed.reset && observed.snapshot.is_none() {
            return Err(Error::Conflict("file reset snapshot missing".into()));
        }
        if let Some(snapshot) = &observed.snapshot {
            super::FileQueryView::from_snapshot(
                query.clone(),
                snapshot.clone(),
                maximum_files,
                maximum_bytes,
            )?;
        }
        let cursor = observed
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.cursor.clone())
            .or(cursor)
            .ok_or_else(|| Error::Conflict("file follow has no initial cut".into()))?;
        Ok((
            Self {
                source: source.retain(),
                query,
                cursor,
                maximum_files,
                maximum_bytes,
                changes: observed.changes,
                pending: None,
                closed_at: None,
            },
            observed.snapshot,
        ))
    }
    pub(crate) async fn read(&mut self) -> Result<FileUpdate> {
        if self.pending.is_none() {
            if let Some(change) = self.changes.next().await {
                self.pending = Some(FileUpdate::Change(change.map_err(query_error)?));
            } else {
                if self.closed_at.as_ref() == Some(&self.cursor) {
                    return Err(Error::Conflict(
                        "resumed file follow ended without progress".into(),
                    ));
                }
                let observed = self
                    .source
                    .observe_files(
                        self.query.clone(),
                        Some(self.cursor.clone()),
                        self.maximum_files,
                        self.maximum_bytes,
                    )
                    .await?;
                if observed.reset && observed.snapshot.is_none() {
                    return Err(Error::Conflict("file reset snapshot missing".into()));
                }
                if let Some(snapshot) = observed.snapshot {
                    self.validate_snapshot(&snapshot)?;
                    self.pending = Some(FileUpdate::Snapshot(snapshot));
                }
                self.closed_at = Some(self.cursor.clone());
                self.changes = observed.changes;
                if self.pending.is_none() {
                    let change = self
                        .changes
                        .next()
                        .await
                        .ok_or_else(|| {
                            Error::Conflict("resumed file follow ended without progress".into())
                        })?
                        .map_err(query_error)?;
                    self.pending = Some(FileUpdate::Change(change));
                }
            }
        }
        self.pending
            .clone()
            .ok_or_else(|| Error::Conflict("file delivery missing".into()))
    }
    pub(crate) fn validate_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        super::FileQueryView::from_snapshot(
            self.query.clone(),
            snapshot.clone(),
            self.maximum_files,
            self.maximum_bytes,
        )?;
        Ok(())
    }
    pub(crate) fn accept(&mut self, cursor: Cursor) {
        self.cursor = cursor;
        self.pending = None;
    }
    pub(crate) fn apply(&mut self, update: FileUpdate, views: &mut ClientViews) -> Result<bool> {
        let visible = !update.is_checkpoint();
        let cursor = update.cursor().clone();
        let changed = match update {
            FileUpdate::Change(change) => views.query_change(change)?,
            FileUpdate::Snapshot(snapshot) => {
                views.query_snapshot(
                    self.query.clone(),
                    snapshot,
                    self.maximum_files,
                    self.maximum_bytes,
                )?;
                true
            }
        };
        self.accept(cursor);
        Ok(visible && changed)
    }
}

#[cfg(all(test, not(acyclic_harness_loom), feature = "tui"))]
mod tests;
