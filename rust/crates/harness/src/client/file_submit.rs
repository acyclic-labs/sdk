//! Ordered request files, with the private volume as the sole outbox.

use super::FileClientError;
use crate::{
    Error,
    task_volume::{
        Change, ChangeStream, Cursor, FileKey, Generation, Query, TaskFile, TaskVolume, Write,
    },
};
use acyclic_stream::StreamProvider;
use futures::StreamExt as _;
use pending::PendingRequests;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

mod pending;

type Result<T> = std::result::Result<T, FileClientError>;

pub(crate) fn validate_request_key(key: &FileKey) -> Result<()> {
    if !matches!(
        key,
        FileKey::Task {
            file: TaskFile::Request(_),
            ..
        }
    ) {
        return Err(Error::Invalid("submit requires an immutable task request file".into()).into());
    }
    Ok(())
}

/// The publication prefix required before an outbox entry can be acknowledged.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum FileClientDurability {
    /// Process-local memory sessions; no crash recovery guarantee.
    Accepted,
    /// Persistent sessions retain intent until the provider confirms durability.
    Durable,
}

/// Bounds retained pending requests, independent of already acknowledged history.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct FileClientLimits {
    /// Maximum queued commands.
    pub commands: usize,
    /// Maximum queued payload bytes.
    pub bytes: u64,
    /// Required provider publication prefix.
    pub durability: FileClientDurability,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    target: [u8; 32],
    target_epoch: uuid::Uuid,
    durability: FileClientDurability,
    cursor: Cursor,
}

/// Single-writer client over private outbox and task volumes. Both are ordinary
/// platform files; no harness journal, transport, socket or timer is involved.
pub struct FileClient<P: ?Sized, T: ?Sized> {
    outbox: Arc<TaskVolume<P>>,
    target: Arc<TaskVolume<T>>,
    target_id: [u8; 32],
    target_epoch: uuid::Uuid,
    checkpoint: FileKey,
    cursor: Cursor,
    scanned: Cursor,
    changes: ChangeStream,
    buffered: Option<Change>,
    pending: PendingRequests,
    limits: FileClientLimits,
}

impl<P: StreamProvider + ?Sized, T: StreamProvider + ?Sized> FileClient<P, T> {
    /// Restores only the cursor and its pending tail. Target rebinding and history
    /// loss are errors; they cannot silently discard offline requests.
    pub async fn open(
        outbox: Arc<TaskVolume<P>>,
        target: Arc<TaskVolume<T>>,
        limits: FileClientLimits,
    ) -> Result<Self> {
        if limits.commands == 0 || limits.bytes == 0 {
            return Err(Error::Invalid("invalid file client bounds".into()).into());
        }
        let checkpoint =
            FileKey::parse("workspace/client/outbox.json").map_err(FileClientError::publication)?;
        let target_head = target.head().await.map_err(FileClientError::publication)?;
        let target_id = target_head.volume;
        let target_epoch = target_head.epoch;
        let state = outbox
            .snapshot(&Query::File(checkpoint.clone()), 1)
            .await
            .map_err(FileClientError::publication)?;
        if state.generation.volume == target_id {
            return Err(Error::Invalid("outbox and target are the same volume".into()).into());
        }
        let cursor = if let Some(file) = state.files.first() {
            if limits.durability == FileClientDurability::Durable
                && !outbox
                    .file_is_durable(file)
                    .await
                    .map_err(FileClientError::publication)?
            {
                return Err(Error::Conflict("outbox checkpoint awaits durability".into()).into());
            }
            let state: Checkpoint =
                serde_json::from_slice(&file.bytes).map_err(|e| Error::Storage(e.to_string()))?;
            if state.target != target_id
                || state.target_epoch != target_epoch
                || state.durability != limits.durability
            {
                return Err(Error::Conflict("outbox target differs".into()).into());
            }
            state.cursor
        } else {
            if !outbox
                .snapshot(&Query::Tasks, 1)
                .await
                .map_err(FileClientError::publication)?
                .files
                .is_empty()
            {
                return Err(Error::Conflict("outbox requests have no checkpoint".into()).into());
            }
            let cursor = outbox
                .cut(&Query::Tasks)
                .await
                .map_err(FileClientError::publication)?;
            let bytes = serde_json::to_vec(&Checkpoint {
                target: target_id,
                target_epoch,
                durability: limits.durability,
                cursor: cursor.clone(),
            })
            .map_err(|e| Error::Storage(e.to_string()))?;
            outbox
                .append(state.generation, &Write::new(checkpoint.clone(), bytes))
                .await
                .map_err(FileClientError::publication)?;
            cursor
        };
        let changes = outbox
            .follow(Query::Tasks, cursor.clone())
            .await
            .map_err(FileClientError::publication)?;
        let mut client = Self {
            outbox,
            target,
            target_id,
            target_epoch,
            checkpoint,
            scanned: cursor.clone(),
            cursor,
            changes,
            buffered: None,
            pending: PendingRequests::new(limits),
            limits,
        };
        client.catch_up().await?;
        Ok(client)
    }

    /// Number of queued requests still awaiting successful target publication.
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Last acknowledged outbox publication, suitable for diagnostics and recovery.
    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }

    async fn catch_up(&mut self) -> Result<()> {
        let cut = self
            .outbox
            .cut(&Query::Tasks)
            .await
            .map_err(FileClientError::publication)?;
        while self.scanned.position < cut.position {
            if self.buffered.is_none() {
                self.buffered = Some(
                    self.changes
                        .next()
                        .await
                        .ok_or_else(|| Error::Storage("outbox follow ended".into()))?
                        .map_err(FileClientError::publication)?,
                );
            }
            let change = self
                .buffered
                .as_ref()
                .expect("buffer filled before validation");
            if let Some(write) = &change.file {
                if !matches!(
                    write.key,
                    FileKey::Task {
                        file: TaskFile::Request(_),
                        ..
                    }
                ) || write.channel.is_some()
                {
                    return Err(Error::Invalid("outbox contains a non-request file".into()).into());
                }
                self.pending.admit(change.cursor.clone(), write.clone())?;
            }
            self.scanned = change.cursor.clone();
            self.buffered = None;
        }
        Ok(())
    }

    /// Publishes one immutable request before attempting any target operation.
    /// Resubmitting the same key/content is idempotent, including after reconnect.
    pub async fn enqueue(&mut self, write: Write) -> Result<()> {
        if write.channel.is_some() {
            return Err(Error::Invalid("request cannot have a streaming channel".into()).into());
        }
        let length = write.bytes.len() as u64;
        self.enqueue_with(write.key, length, || Ok(write.bytes))
            .await
    }

    /// Reconciles and admits capacity before the caller copies a platform buffer.
    /// The synchronous producer runs once, with exclusive client ownership and
    /// no intervening await before the immutable publication is fenced.
    pub async fn enqueue_with(
        &mut self,
        key: FileKey,
        length: u64,
        produce: impl FnOnce() -> crate::Result<acyclic_stream::OwnedBytes>,
    ) -> Result<()> {
        self.catch_up().await?;
        validate_request_key(&key)?;
        let existing = self
            .outbox
            .snapshot(&Query::File(key.clone()), 1)
            .await
            .map_err(FileClientError::publication)?;
        if let Some(file) = existing.files.first() {
            if file.bytes.len() as u64 != length {
                return Err(Error::Conflict("outbox request identity conflict".into()).into());
            }
            return if file.bytes.as_ref() == produce()?.as_ref() {
                Ok(())
            } else {
                Err(Error::Conflict("outbox request identity conflict".into()).into())
            };
        }
        self.pending.check(length)?;
        let bytes = produce()?;
        if bytes.len() as u64 != length {
            return Err(
                Error::Invalid("request buffer length changed during admission".into()).into(),
            );
        }
        let write = Write::new(key, bytes);
        // Admission is fenced at the exact reconciled cut. An earlier cancelled
        // append that lands after catch-up must conflict, never bypass quotas.
        let head = Generation {
            volume: self.scanned.volume,
            epoch: self.scanned.epoch,
            position: self.scanned.position,
            commit: self.scanned.commit,
        };
        let publication = self
            .outbox
            .append(head, &write)
            .await
            .map_err(FileClientError::publication);
        // A failed acknowledgement may follow successful publication. Retain
        // accepted intent even when returning that indeterminate failure.
        let reconciled = self.catch_up().await;
        publication?;
        reconciled
    }

    /// Sends in enqueue order and stops at the first indeterminate publication.
    /// A checkpoint failure retains the request; retrying its immutable file write
    /// is safe even when the target already accepted it.
    /// A definite rejection returns `Rejected` after retiring that entry. The next
    /// flush continues with the following request; uncertain failures retain it.
    pub async fn flush(&mut self) -> Result<usize> {
        self.catch_up().await?;
        let mut submitted = 0;
        while let Some(pending) = self.pending.front() {
            let checkpoint = self
                .outbox
                .snapshot(&Query::File(self.checkpoint.clone()), 1)
                .await
                .map_err(FileClientError::publication)?;
            let current: Checkpoint = serde_json::from_slice(
                &checkpoint
                    .files
                    .first()
                    .ok_or_else(|| Error::Conflict("outbox checkpoint disappeared".into()))?
                    .bytes,
            )
            .map_err(|e| Error::Storage(e.to_string()))?;
            if current.target != self.target_id
                || current.target_epoch != self.target_epoch
                || current.durability != self.limits.durability
                || (current.cursor != self.cursor && current.cursor != pending.cursor)
            {
                return Err(Error::Conflict(
                    "outbox checkpoint changed; reopen before publication".into(),
                )
                .into());
            }
            if self.limits.durability == FileClientDurability::Durable
                && !self
                    .outbox
                    .file_is_durable(&pending.write)
                    .await
                    .map_err(FileClientError::publication)?
            {
                return Err(Error::Conflict("outbox publication awaits durability".into()).into());
            }
            let head = self
                .target
                .head()
                .await
                .map_err(FileClientError::publication)?;
            if head.volume != self.target_id || head.epoch != self.target_epoch {
                return Err(Error::Conflict("outbox target history differs".into()).into());
            }
            let rejected =
                match crate::task_volume::publish(&self.target, head, &pending.write, || {
                    std::future::ready(Ok::<_, crate::task_volume::Error>((Vec::new(), None)))
                })
                .await
                {
                    Ok(_) => None,
                    Err(error) if terminal_rejection(&error) => Some(error.to_string()),
                    Err(error) => return Err(FileClientError::publication(error)),
                };
            if rejected.is_none()
                && self.limits.durability == FileClientDurability::Durable
                && !self
                    .target
                    .file_is_durable(&pending.write)
                    .await
                    .map_err(FileClientError::publication)?
            {
                return Err(Error::Conflict("target publication awaits durability".into()).into());
            }
            let state = Checkpoint {
                target: self.target_id,
                target_epoch: self.target_epoch,
                durability: self.limits.durability,
                cursor: pending.cursor.clone(),
            };
            let bytes = serde_json::to_vec(&state).map_err(|e| Error::Storage(e.to_string()))?;
            let checkpoint_write = Write::new(self.checkpoint.clone(), bytes);
            self.outbox
                .append(checkpoint.generation, &checkpoint_write)
                .await
                .map_err(FileClientError::publication)?;
            if self.limits.durability == FileClientDurability::Durable
                && !self
                    .outbox
                    .file_is_durable(&checkpoint_write)
                    .await
                    .map_err(FileClientError::publication)?
            {
                return Err(Error::Conflict("outbox checkpoint awaits durability".into()).into());
            }
            let done = self
                .pending
                .retire_front()
                .expect("front retained across publication");
            self.cursor = done.cursor;
            if let Some(reason) = rejected {
                return Err(FileClientError::Rejected {
                    request: done.write.key,
                    reason,
                });
            }
            submitted += 1;
        }
        Ok(submitted)
    }
}

fn terminal_rejection(error: &crate::task_volume::Error) -> bool {
    use crate::task_volume::Error as E;
    matches!(
        error,
        E::Unauthorized
            | E::InvalidPath
            | E::InvalidSegment
            | E::LimitExceeded
            | E::ImmutableConflict(_)
            | E::SequenceConflict(_)
            | E::Storage(
                acyclic_stream::StreamError::AccessDenied
                    | acyclic_stream::StreamError::InvalidArgument
                    | acyclic_stream::StreamError::IdempotencyMismatch
            )
    )
}
