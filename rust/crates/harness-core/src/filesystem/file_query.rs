//! Read-only query projection of ordinary files from their existing authority log.

use super::{FilesystemContentVerifier, PreparationIo, workspace_ref};
use crate::{
    conversation::{ContentGrant, VolumeClass, VolumeRef},
    task_volume::{self, Cursor, FileQuerySource, Observation, Query, SCHEMA, Snapshot},
};
use acyclic_fs::{AsyncObjectStore, StreamAuthorityStore, WorkBudget};
use acyclic_stream::{BoxProviderFuture, ReadRequest, Record, StreamPath, StreamProvider};
use futures::StreamExt as _;
use std::sync::Arc;

mod policy;
mod projection;
use projection::{Selection, generation};

/// An authorized query of ordinary agent or project files and their original history.
pub struct FilesystemFileQuery<P, O> {
    reader: Arc<FilesystemContentVerifier<StreamAuthorityStore<P>, O>>,
    volume: VolumeRef,
    budget: WorkBudget,
    policy: Arc<acyclic_fs::access::AccessView>,
}

impl<P, O> Clone for FilesystemFileQuery<P, O> {
    fn clone(&self) -> Self {
        Self {
            reader: self.reader.clone(),
            volume: self.volume.clone(),
            budget: self.budget,
            policy: Arc::clone(&self.policy),
        }
    }
}

impl<P: StreamProvider + 'static, O: AsyncObjectStore + 'static>
    FilesystemContentVerifier<StreamAuthorityStore<P>, O>
{
    /// Binds the existing file reader and a finite per-revision query allowance.
    pub async fn file_query(
        self: &Arc<Self>,
        volume: &VolumeRef,
        budget: WorkBudget,
    ) -> crate::Result<FilesystemFileQuery<P, O>> {
        volume.validate(&mut None)?;
        if !matches!(
            volume.class(),
            VolumeClass::AgentPrivate | VolumeClass::Project
        ) || volume.provider() != &self.host.provider
            || !budget.has_finite_allowances()
        {
            return Err(crate::Error::Invalid(
                "file query requires an agent or project volume and finite work".into(),
            ));
        }
        let io = PreparationIo::from_budget(budget, &acyclic_fs::CancellationToken::new())?;
        let reference =
            workspace_ref(self.host.provider.clone(), &volume.storage_name(&mut None)?)?;
        let workspace = self.host.open(&reference, Some(&io)).await?;
        let view = policy::compile(self, &workspace, &io).await?;
        let bytes = (std::mem::size_of::<acyclic_fs::access::AccessView>()
            + 2 * std::mem::size_of::<usize>()) as u64;
        io.retain_owned(bytes)?;
        io.allocation_attempt(bytes)?;
        Ok(FilesystemFileQuery {
            reader: self.clone(),
            volume: volume.clone(),
            budget,
            policy: Arc::new(acyclic_fs::access::AccessView::new(view)),
        })
    }
}

async fn record<P: StreamProvider>(
    provider: &P,
    path: &StreamPath,
    position: u64,
) -> task_volume::Result<Record> {
    let mut read = provider
        .read(ReadRequest {
            path: path.clone(),
            from: position,
            limit: 1,
        })
        .await?;
    let record = read
        .next()
        .await
        .ok_or(task_volume::Error::ResetRequired)??;
    if record.sequence != position {
        return Err(task_volume::Error::ResetRequired);
    }
    Ok(record)
}

async fn cut<P: StreamProvider>(
    provider: &P,
    path: &StreamPath,
    supplied: Option<Cursor>,
    mut head: Cursor,
) -> task_volume::Result<(Cursor, Record, bool)> {
    if let Some(cursor) = supplied
        && cursor.volume == head.volume
        && cursor.epoch == head.epoch
        && cursor.query == head.query
        && cursor.schema == head.schema
        && cursor.position >= 2
        && cursor.position <= head.accepted
        && cursor.accepted >= cursor.position
        && cursor.accepted <= head.accepted
        && cursor.durable <= cursor.accepted
        && cursor.durable <= head.durable
    {
        match record(provider, path, cursor.position - 1).await {
            Ok(anchor) if anchor.commit_id.as_bytes() == &cursor.commit => {
                return Ok((cursor, anchor, true));
            }
            Ok(_) | Err(task_volume::Error::ResetRequired) => {}
            Err(error) => return Err(error),
        }
    }
    let anchor = record(provider, path, head.position - 1).await?;
    head.commit = *anchor.commit_id.as_bytes();
    Ok((head, anchor, false))
}

impl<P: StreamProvider + 'static, O: AsyncObjectStore + 'static> FileQuerySource
    for FilesystemFileQuery<P, O>
{
    fn observe_files(
        &self,
        query: Query,
        cursor: Option<Cursor>,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> BoxProviderFuture<'_, task_volume::Result<Observation>> {
        Box::pin(async move {
            if maximum_files == 0
                || maximum_files as u64 > crate::conversation::MAX_PORTABLE_COUNT as u64
                || maximum_bytes == 0
                || maximum_bytes as u64 > self.reader.maximum_bytes
            {
                return Err(task_volume::Error::LimitExceeded);
            }
            let io =
                PreparationIo::from_budget(self.budget, &acyclic_fs::CancellationToken::new())?;
            self.authorize(&query, &io).await?;
            let reference = workspace_ref(
                self.reader.host.provider.clone(),
                &self.volume.storage_name(&mut None)?,
            )?;
            let workspace = self.reader.host.open(&reference, Some(&io)).await?;
            let permissions = self.authorize_policy(&workspace, &io).await?;
            let (provider, path) = self
                .reader
                .host
                .filesystem
                .authority_records(workspace.id().volume_id())
                .map_err(|error| crate::Error::Storage(error.to_string()))?;
            let genesis = record(provider.as_ref(), &path, 0).await?;
            let watermarks = provider.watermarks(path.clone()).await?;
            if watermarks.accepted < 2 || watermarks.durable > watermarks.accepted {
                return Err(task_volume::Error::ResetRequired);
            }
            let supplied = cursor.is_some();
            let head = query_head(&path, &genesis, &self.volume, &query, watermarks)?;
            let (cursor, anchor, valid_resume) =
                cut(provider.as_ref(), &path, cursor, head).await?;
            let current = generation(&workspace, &anchor, &io).await?;
            self.require_snapshot(&permissions, &current, &io).await?;
            let selection = Selection::read(
                &current,
                &self.volume,
                &query,
                maximum_files,
                maximum_bytes,
                &io,
            )
            .await?;
            let snapshot = (!valid_resume).then(|| Snapshot {
                files: selection.files.values().cloned().collect(),
                generation: task_volume::Generation {
                    volume: cursor.volume,
                    epoch: cursor.epoch,
                    position: cursor.position,
                    commit: cursor.commit,
                },
                cursor: cursor.clone(),
            });
            let records = provider
                .follow_checked(
                    path,
                    cursor.position,
                    acyclic_stream::CommitId::from_bytes(cursor.commit),
                    acyclic_stream::FollowFilter::default(),
                )
                .await?;
            super::access::require_current(&workspace, &permissions, &io).await?;
            let changes = self.changes(
                query,
                current,
                cursor,
                selection,
                records,
                (maximum_files, maximum_bytes),
            );
            Ok(Observation {
                snapshot,
                reset: supplied && !valid_resume,
                changes,
            })
        })
    }
}

impl<P: StreamProvider + 'static, O: AsyncObjectStore + 'static> FilesystemFileQuery<P, O> {
    fn changes(
        &self,
        query: Query,
        current: acyclic_fs::Generation<StreamAuthorityStore<P>, O>,
        cursor: Cursor,
        selection: Selection,
        records: acyclic_stream::RecordStream,
        bounds: (usize, usize),
    ) -> task_volume::ChangeStream {
        let source = self.clone();
        let stream = futures::stream::try_unfold(
            (records, source, current, cursor, selection),
            move |(mut records, source, current, mut cursor, mut selection)| {
                let query = query.clone();
                async move {
                    let record = records
                        .next()
                        .await
                        .ok_or(task_volume::Error::ResetRequired)??;
                    if record.sequence != cursor.position {
                        return Err(task_volume::Error::ResetRequired);
                    }
                    let io = PreparationIo::from_budget(
                        source.budget,
                        &acyclic_fs::CancellationToken::new(),
                    )?;
                    source.authorize(&query, &io).await?;
                    let permissions = source.authorize_policy(&current.workspace(), &io).await?;
                    let next = generation(&current.workspace(), &record, &io).await?;
                    source.require_snapshot(&permissions, &next, &io).await?;
                    let maximum = u32::try_from(source.budget.items_returned)
                        .map_err(|_| task_volume::Error::LimitExceeded)?;
                    let (diff, _allocation) = io.sdk(
                        current
                            .diff_to(&next, maximum, &io, io.cancellation())
                            .await,
                    )?;
                    if diff.changes().truncated {
                        return Err(task_volume::Error::LimitExceeded);
                    }
                    let (files, removed) = selection
                        .apply(&next, &source.volume, diff.changes(), &query, bounds, &io)
                        .await?;
                    cursor.position = cursor
                        .position
                        .checked_add(1)
                        .ok_or(task_volume::Error::LimitExceeded)?;
                    cursor.accepted = cursor.accepted.max(cursor.position);
                    cursor.commit = *record.commit_id.as_bytes();
                    let change = task_volume::Change {
                        file: None,
                        files,
                        removed,
                        cursor: cursor.clone(),
                    };
                    super::access::require_current(&current.workspace(), &permissions, &io).await?;
                    Ok(Some((change, (records, source, next, cursor, selection))))
                }
            },
        );
        Box::pin(stream)
    }

    async fn authorize(&self, query: &Query, io: &PreparationIo) -> crate::Result<()> {
        let prefix = match query {
            Query::Plans => "plans",
            Query::File(task_volume::FileKey::Goal) => "",
            Query::File(task_volume::FileKey::RequestHead) => "inputs",
            _ => {
                return Err(crate::Error::Unsupported(
                    "ordinary file query selection is unsupported".into(),
                ));
            }
        };
        self.reader
            .verify_creation(&self.volume, None, Some(io))
            .await?;
        ContentGrant::verify_directory_read(
            &self.reader.verifier,
            &self.reader.scope,
            &self.volume,
            prefix,
            &mut Some(&mut io.history_io()),
        )?;
        Ok(())
    }
}

fn query_head(
    path: &StreamPath,
    genesis: &Record,
    volume_ref: &VolumeRef,
    query: &Query,
    watermarks: acyclic_stream::StreamWatermarks,
) -> task_volume::Result<Cursor> {
    let mut identity = blake3::Hasher::new();
    identity.update(path.as_str().as_bytes());
    identity.update(genesis.commit_id.as_bytes());
    identity
        .update(&serde_json::to_vec(volume_ref).map_err(|_| task_volume::Error::InvalidSegment)?);
    let volume = *identity.finalize().as_bytes();
    let epoch = uuid::Uuid::from_bytes(
        genesis.commit_id.as_bytes()[..16]
            .try_into()
            .map_err(|_| task_volume::Error::InvalidSegment)?,
    );
    let query_id =
        *blake3::hash(&serde_json::to_vec(query).map_err(|_| task_volume::Error::InvalidSegment)?)
            .as_bytes();
    Ok(Cursor {
        volume,
        epoch,
        schema: SCHEMA,
        query: query_id,
        position: watermarks.accepted,
        accepted: watermarks.accepted,
        durable: watermarks.durable,
        commit: [0; 32],
    })
}
