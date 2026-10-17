//! Query I/O and delivery ownership, independent of the renderer's view handle.

use crate::{
    Error, Result,
    task_volume::{Change, ChangeStream, Cursor, HeadSnapshot, Query, TaskVolume},
};
use acyclic_stream::{StreamPath, StreamProvider};
use futures::StreamExt as _;

pub(crate) fn query_error(error: crate::task_volume::Error) -> Error {
    Error::Storage(error.to_string())
}

pub(crate) enum PublishError {
    Rejected(Error),
    Failed(Error),
}

pub(crate) struct QuerySubscription {
    target: StreamPath,
    query: Query,
    maximum_files: usize,
    cursor: Cursor,
    changes: ChangeStream,
    pending: Option<Change>,
    reset: bool,
}

impl QuerySubscription {
    pub async fn open<P: StreamProvider + ?Sized>(
        volume: &TaskVolume<P>,
        query: Query,
        cursor: Option<Cursor>,
        maximum_files: usize,
    ) -> Result<(Self, HeadSnapshot)> {
        let observed = volume
            .observe(query.clone(), cursor.clone(), maximum_files)
            .await
            .map_err(query_error)?;
        let snapshot = match observed.snapshot {
            Some(snapshot) => snapshot,
            None => volume
                .snapshot_at(
                    &query,
                    cursor.ok_or_else(|| {
                        Error::Storage("query returned no snapshot or cursor".into())
                    })?,
                    maximum_files,
                )
                .await
                .map_err(query_error)?,
        }
        .into_heads()
        .map_err(query_error)?;
        Ok((
            Self {
                target: volume.path().clone(),
                query,
                maximum_files,
                cursor: snapshot.cursor.clone(),
                changes: observed.changes,
                pending: None,
                reset: observed.reset,
            },
            snapshot,
        ))
    }

    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }
    pub fn reset(&self) -> bool {
        self.reset
    }

    pub async fn wait_next(&mut self) -> Result<()> {
        if self.pending.is_none() {
            self.pending = Some(
                self.changes
                    .next()
                    .await
                    .ok_or_else(|| Error::Storage("query follow ended; reconnect required".into()))?
                    .map_err(query_error)?,
            );
        }
        Ok(())
    }

    pub async fn publish_next<F>(&mut self, mut publish: F) -> Result<()>
    where
        F: for<'a> std::ops::AsyncFnMut(&'a Change) -> std::result::Result<(), PublishError>,
    {
        self.wait_next().await?;
        let change = self
            .pending
            .as_ref()
            .ok_or_else(|| Error::Storage("query delivery missing".into()))?;
        match publish(change).await {
            Ok(()) => {}
            Err(PublishError::Rejected(error)) => {
                self.pending = None;
                return Err(error);
            }
            Err(PublishError::Failed(error)) => return Err(error),
        }
        self.cursor = change.cursor.clone();
        self.pending = None;
        Ok(())
    }

    pub async fn reconnect<P: StreamProvider + ?Sized>(
        &mut self,
        volume: &TaskVolume<P>,
        mut replace: impl FnMut(HeadSnapshot) -> Result<()>,
    ) -> Result<bool> {
        if volume.path() != &self.target {
            return Err(Error::Invalid("query reconnect target differs".into()));
        }
        if self.pending.is_some() {
            return Err(Error::Conflict(
                "publish the buffered query delivery before reconnecting".into(),
            ));
        }
        let observed = volume
            .observe(
                self.query.clone(),
                Some(self.cursor.clone()),
                self.maximum_files,
            )
            .await
            .map_err(query_error)?;
        let cursor = if let Some(snapshot) = observed.snapshot {
            let cursor = snapshot.cursor.clone();
            replace(snapshot.into_heads().map_err(query_error)?)?;
            cursor
        } else {
            self.cursor.clone()
        };
        self.cursor = cursor;
        self.changes = observed.changes;
        self.reset = observed.reset;
        Ok(observed.reset)
    }
}
