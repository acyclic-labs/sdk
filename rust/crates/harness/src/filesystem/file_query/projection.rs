use super::*;
use crate::task_volume::{FileKey, FileValue};
use acyclic_fs::{FileId, Generation, Workspace, WorkspaceError, kernel::FileKind};
use std::collections::{BTreeMap, BTreeSet};

type Authority<P> = StreamAuthorityStore<P>;

pub(super) async fn generation<P: StreamProvider + 'static, O: AsyncObjectStore + 'static>(
    workspace: &Workspace<Authority<P>, O>,
    record: &Record,
    io: &PreparationIo,
) -> task_volume::Result<Generation<Authority<P>, O>> {
    let work = acyclic_fs::WorkCounters {
        authority_records_read: 1,
        authority_bytes_read: record.value.len() as u64,
        bytes_copied: record.value.len() as u64,
        bytes_hashed: record.value.len() as u64,
        items_examined: 1,
        ..Default::default()
    };
    io.preflight(work).map_err(crate::Error::from)?;
    let hold = io.reserve_decode(record.value.len() as u64)?;
    io.allocation_attempt(record.value.len() as u64)?;
    io.charge(work).map_err(crate::Error::from)?;
    let decoded = (
        acyclic_fs::StreamsDurableRecord::decode(&record.value, record.value.len() as u64)
            .map_err(|_| task_volume::Error::InvalidSegment)?,
        hold,
    );
    let durable = &decoded.0.0;
    let digest = acyclic_fs::foundation::authority_commit_digest(
        acyclic_fs::kernel::volume_authority_id(workspace.id().volume_id()),
        durable.epoch,
        durable.sequence,
        durable.operation_id,
        durable.fingerprint,
        durable.previous_digest,
        &durable.payload,
    );
    if durable.sequence.get() != record.sequence || durable.digest != digest {
        return Err(task_volume::Error::InvalidSegment);
    }
    let key = acyclic_fs::IdempotencyKey::from_bytes(durable.operation_id.into_bytes());
    let generation = io
        .sdk(
            workspace
                .operation_generation(key, io, io.cancellation())
                .await,
        )?
        .ok_or(task_volume::Error::ResetRequired)?;
    drop(decoded);
    Ok(generation)
}

/// Disposable selection metadata; payloads are the original immutable FS slices.
pub(super) struct Selection {
    pub(super) files: BTreeMap<FileKey, FileValue>,
    watched: BTreeSet<FileId>,
}

impl Selection {
    pub(super) fn affected(&self, diff: &acyclic_fs::GenerationDiff) -> bool {
        diff.files
            .iter()
            .any(|change| self.watched.contains(&change.file_id))
    }

    pub(super) async fn read<P: StreamProvider + 'static, O: AsyncObjectStore + 'static>(
        generation: &Generation<Authority<P>, O>,
        query: &Query,
        maximum_files: usize,
        maximum_bytes: usize,
        io: &PreparationIo,
    ) -> task_volume::Result<Self> {
        let mut selection = Self {
            files: BTreeMap::new(),
            watched: BTreeSet::new(),
        };
        let root = io.sdk(generation.stat("/", io, io.cancellation()).await)?;
        selection.watched.insert(root.file_id);
        let directory = match query {
            Query::Plans => "/plans",
            Query::File(FileKey::Goal) => "/GOAL.md",
            _ => return Err(task_volume::Error::InvalidPath),
        };
        let (stat, recorded) = generation.stat(directory, io, io.cancellation()).await;
        recorded.map_err(crate::Error::from)?;
        let stat = match stat {
            Ok(stat) => stat,
            Err(WorkspaceError::NotFound) => return Ok(selection),
            Err(error) => return Err(crate::Error::from(error).into()),
        };
        selection.watched.insert(stat.file_id);
        let mut entries = Vec::new();
        if query == &Query::File(FileKey::Goal) {
            entries.push((FileKey::Goal, stat.file_id, stat.kind));
        } else {
            if stat.kind != FileKind::Directory {
                return Err(task_volume::Error::InvalidPath);
            }
            entries = plan_entries(generation, directory, maximum_files, io).await?;
        }
        let mut remaining = maximum_bytes;
        for (key, id, kind) in entries {
            if kind != FileKind::Regular {
                return Err(task_volume::Error::InvalidPath);
            }
            selection.watched.insert(id);
            let path = format!("/{}", key.path());
            let bytes = io.sdk(
                generation
                    .read(&path, remaining as u64, io, io.cancellation())
                    .await,
            )?;
            remaining = remaining
                .checked_sub(bytes.len())
                .ok_or(task_volume::Error::LimitExceeded)?;
            selection
                .files
                .insert(key.clone(), FileValue::new(key, bytes));
        }
        Ok(selection)
    }
}

async fn plan_entries<P: StreamProvider + 'static, O: AsyncObjectStore + 'static>(
    generation: &Generation<Authority<P>, O>,
    directory: &str,
    maximum_files: usize,
    io: &PreparationIo,
) -> task_volume::Result<Vec<(FileKey, FileId, FileKind)>> {
    let mut entries = Vec::new();
    let mut previous: Option<(
        acyclic_fs::kernel::DirectoryPage,
        acyclic_fs::AuthorityAllocation,
    )> = None;
    loop {
        let after = previous
            .as_ref()
            .and_then(|(page, _)| page.entries.last())
            .map(|entry| &entry.name);
        let current = io.sdk(
            generation
                .list_directory(
                    directory,
                    after,
                    u32::try_from(maximum_files).map_err(|_| task_volume::Error::LimitExceeded)?,
                    io,
                    io.cancellation(),
                )
                .await,
        )?;
        drop(previous.take());
        let page = &current.0;
        for entry in &page.entries {
            let decoded = entry
                .name
                .unicode_text(
                    io.remaining().map_err(crate::Error::from)?,
                    io.cancellation(),
                )
                .map_err(|failure| crate::Error::Invalid(failure.error.to_string()))?;
            io.charge(decoded.work).map_err(crate::Error::from)?;
            let path = format!("plans/{}", decoded.value);
            if decoded.value.ends_with(".md") {
                if entries.len() == maximum_files {
                    return Err(task_volume::Error::LimitExceeded);
                }
                let key = FileKey::parse(&path)?;
                entries.push((key, entry.file_id, entry.kind));
            }
        }
        if !page.has_more {
            break;
        }
        if page.entries.is_empty() {
            return Err(task_volume::Error::InvalidSegment);
        }
        previous = Some(current);
    }
    Ok(entries)
}
