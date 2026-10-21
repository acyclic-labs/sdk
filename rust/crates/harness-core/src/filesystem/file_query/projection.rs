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
    entries: BTreeMap<FileKey, FileId>,
    root: FileId,
    directory: Option<FileId>,
    parent: Option<FileId>,
    goal_metadata_path: Option<String>,
    goal_metadata_entry: Option<FileId>,
}

impl Selection {
    pub(super) async fn read<P: StreamProvider + 'static, O: AsyncObjectStore + 'static>(
        generation: &Generation<Authority<P>, O>,
        volume: &crate::conversation::VolumeRef,
        query: &Query,
        maximum_files: usize,
        maximum_bytes: usize,
        io: &PreparationIo,
    ) -> task_volume::Result<Self> {
        let root = io.sdk(generation.stat("/", io, io.cancellation()).await)?;
        let mut selection = Self {
            files: BTreeMap::new(),
            entries: BTreeMap::new(),
            root: root.file_id,
            directory: None,
            parent: None,
            goal_metadata_path: if query == &Query::File(FileKey::Goal) {
                Some(crate::filesystem::content_metadata_path_io(
                    "GOAL.md",
                    true,
                    Some(io),
                )?)
            } else {
                None
            },
            goal_metadata_entry: None,
        };
        let entries = selection.list(generation, query, maximum_files, io).await?;
        selection
            .replace(
                generation,
                volume,
                entries,
                &BTreeSet::new(),
                maximum_bytes,
                io,
            )
            .await?;
        Ok(selection)
    }

    async fn list<P: StreamProvider + 'static, O: AsyncObjectStore + 'static>(
        &mut self,
        generation: &Generation<Authority<P>, O>,
        query: &Query,
        maximum_files: usize,
        io: &PreparationIo,
    ) -> task_volume::Result<BTreeMap<FileKey, FileId>> {
        if query == &Query::File(FileKey::RequestHead) {
            let (parent, recorded) = generation.stat("/inputs", io, io.cancellation()).await;
            recorded.map_err(crate::Error::from)?;
            self.parent = match parent {
                Ok(parent) if parent.kind == FileKind::Directory => Some(parent.file_id),
                Err(WorkspaceError::NotFound) => None,
                Ok(_) => return Err(task_volume::Error::InvalidPath),
                Err(error) => return Err(crate::Error::from(error).into()),
            };
        }
        let directory = match query {
            Query::Plans => "/plans",
            Query::File(FileKey::Goal) => "/GOAL.md",
            Query::File(FileKey::RequestHead) => "/inputs/current.json",
            _ => return Err(task_volume::Error::InvalidPath),
        };
        let (stat, recorded) = generation.stat(directory, io, io.cancellation()).await;
        recorded.map_err(crate::Error::from)?;
        let stat = match stat {
            Ok(stat) => stat,
            Err(WorkspaceError::NotFound) => {
                self.directory = None;
                return Ok(BTreeMap::new());
            }
            Err(error) => return Err(crate::Error::from(error).into()),
        };
        self.directory = Some(stat.file_id);
        if let Query::File(key) = query {
            Ok(if stat.kind == FileKind::Regular {
                BTreeMap::from([(key.clone(), stat.file_id)])
            } else {
                BTreeMap::new()
            })
        } else {
            if stat.kind != FileKind::Directory {
                return Err(task_volume::Error::InvalidPath);
            }
            Ok(plan_entries(generation, directory, maximum_files, io)
                .await?
                .into_iter()
                .map(|(key, id, _)| (key, id))
                .collect())
        }
    }

    pub(super) async fn apply<P: StreamProvider + 'static, O: AsyncObjectStore + 'static>(
        &mut self,
        generation: &Generation<Authority<P>, O>,
        volume: &crate::conversation::VolumeRef,
        diff: &acyclic_fs::GenerationDiff,
        query: &Query,
        bounds: (usize, usize),
        io: &PreparationIo,
    ) -> task_volume::Result<(Vec<FileValue>, Vec<FileKey>)> {
        let root_name = match query {
            Query::Plans => "plans",
            Query::File(FileKey::Goal) => "GOAL.md",
            Query::File(FileKey::RequestHead) => "inputs",
            _ => return Err(task_volume::Error::InvalidPath),
        };
        let mut root_changed = false;
        let mut metadata_changed = false;
        for binding in &diff.bindings {
            if let Some(path) = &self.goal_metadata_path {
                let decoded = name(&binding.name, io)?;
                metadata_changed |= path.rsplit('/').next() == Some(decoded.as_str())
                    || decoded == "harness-file-metadata"
                    || (binding.directory_id == self.root && decoded == ".system");
            }
            if query == &Query::File(FileKey::RequestHead)
                && Some(binding.directory_id) == self.parent
                && name(&binding.name, io)? == "current.json"
            {
                root_changed = true;
            }
            if binding.directory_id == self.root && name(&binding.name, io)? == root_name {
                root_changed = true;
            }
        }
        let changed: BTreeSet<_> = diff.files.iter().map(|file| file.file_id).collect();
        let mut dirty: BTreeSet<_> = self
            .entries
            .iter()
            .filter(|(_, id)| changed.contains(*id))
            .map(|(key, _)| key.clone())
            .collect();
        if metadata_changed
            || self
                .goal_metadata_entry
                .is_some_and(|id| changed.contains(&id))
        {
            dirty.insert(FileKey::Goal);
        }
        let mut entries = if root_changed {
            let entries = self.list(generation, query, bounds.0, io).await?;
            dirty.extend(
                entries
                    .iter()
                    .filter(|(key, id)| self.entries.get(*key) != Some(*id))
                    .map(|(key, _)| key.clone()),
            );
            entries
        } else {
            std::mem::take(&mut self.entries)
        };
        if !root_changed && query == &Query::Plans {
            for binding in &diff.bindings {
                if Some(binding.directory_id) != self.directory {
                    continue;
                }
                if !binding
                    .before
                    .as_ref()
                    .is_some_and(|entry| entry.kind == FileKind::Regular)
                    && !binding
                        .after
                        .as_ref()
                        .is_some_and(|entry| entry.kind == FileKind::Regular)
                {
                    continue;
                }
                let decoded = name(&binding.name, io)?;
                if decoded == ".md" || !decoded.ends_with(".md") {
                    continue;
                }
                let key = FileKey::parse(&format!("plans/{decoded}"))?;
                dirty.insert(key.clone());
                entries.remove(&key);
                if let Some(after) = &binding.after
                    && after.kind == FileKind::Regular
                {
                    entries.insert(key, after.file_id);
                }
            }
        }
        if entries.len() > bounds.0 {
            return Err(task_volume::Error::LimitExceeded);
        }
        self.replace(generation, volume, entries, &dirty, bounds.1, io)
            .await
    }

    async fn replace<P: StreamProvider + 'static, O: AsyncObjectStore + 'static>(
        &mut self,
        generation: &Generation<Authority<P>, O>,
        volume: &crate::conversation::VolumeRef,
        entries: BTreeMap<FileKey, FileId>,
        changed: &BTreeSet<FileKey>,
        maximum_bytes: usize,
        io: &PreparationIo,
    ) -> task_volume::Result<(Vec<FileValue>, Vec<FileKey>)> {
        let mut removed = Vec::new();
        self.files.retain(|key, _| {
            if !entries.contains_key(key) {
                removed.push(key.clone());
                false
            } else {
                !changed.contains(key)
            }
        });
        let mut remaining = maximum_bytes;
        for value in self.files.values() {
            remaining = remaining
                .checked_sub(value.bytes.len())
                .ok_or(task_volume::Error::LimitExceeded)?;
        }
        let mut files = Vec::new();
        for key in entries
            .keys()
            .filter(|key| !self.files.contains_key(*key))
            .cloned()
            .collect::<Vec<_>>()
        {
            let path = format!("/{}", key.path());
            let bytes = io.sdk(
                generation
                    .read(&path, remaining as u64, io, io.cancellation())
                    .await,
            )?;
            remaining = remaining
                .checked_sub(bytes.len())
                .ok_or(task_volume::Error::LimitExceeded)?;
            let mut value = FileValue::new(key.clone(), bytes);
            if let Some(path) = &self.goal_metadata_path {
                let (publication, entry) =
                    goal_publication(generation, volume, path, &value.bytes, io).await?;
                value.publication = publication;
                self.goal_metadata_entry = entry;
            }
            files.push(value.clone());
            self.files.insert(key, value);
        }
        self.entries = entries;
        Ok((files, removed))
    }
}

async fn goal_publication<P: StreamProvider + 'static, O: AsyncObjectStore + 'static>(
    generation: &Generation<Authority<P>, O>,
    volume: &crate::conversation::VolumeRef,
    path: &str,
    bytes: &[u8],
    io: &PreparationIo,
) -> task_volume::Result<(Option<crate::OperationId>, Option<FileId>)> {
    let (stat, recorded) = generation.stat(path, io, io.cancellation()).await;
    recorded.map_err(crate::Error::from)?;
    let stat = match stat {
        Ok(stat) => stat,
        Err(WorkspaceError::NotFound) => return Ok((None, None)),
        Err(error) => return Err(crate::Error::from(error).into()),
    };
    let metadata = io.sdk(
        generation
            .read(
                path,
                io.remaining()
                    .map_err(crate::Error::from)?
                    .source_bytes_read,
                io,
                io.cancellation(),
            )
            .await,
    )?;
    let reference: crate::conversation::FileRef = io.decode_json(&metadata)?;
    if reference.volume() != volume || reference.path() != "GOAL.md" {
        return Err(task_volume::Error::InvalidSegment);
    }
    // An unindexed writer may leave an old descriptor. Only its provenance is
    // unknown; the readable goal remains observable. IO failures still propagate.
    if !crate::executor::with_history_io(Some(io), |work| {
        reference.descriptor().matches_bytes(bytes, work)
    })? {
        return Ok((None, Some(stat.file_id)));
    }
    Ok((
        Some(crate::filesystem::file_publication(&reference, Some(io))?),
        Some(stat.file_id),
    ))
}

fn name(
    value: &acyclic_fs::kernel::LogicalName,
    io: &PreparationIo,
) -> task_volume::Result<String> {
    let decoded = value
        .unicode_text(
            io.remaining().map_err(crate::Error::from)?,
            io.cancellation(),
        )
        .map_err(|failure| crate::Error::Invalid(failure.error.to_string()))?;
    io.charge(decoded.work).map_err(crate::Error::from)?;
    Ok(decoded.value)
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
            if entry.kind != FileKind::Regular {
                continue;
            }
            let decoded = entry
                .name
                .unicode_text(
                    io.remaining().map_err(crate::Error::from)?,
                    io.cancellation(),
                )
                .map_err(|failure| crate::Error::Invalid(failure.error.to_string()))?;
            io.charge(decoded.work).map_err(crate::Error::from)?;
            if decoded.value != ".md" && decoded.value.ends_with(".md") {
                if entries.len() == maximum_files {
                    return Err(task_volume::Error::LimitExceeded);
                }
                let key = FileKey::parse(&format!("plans/{}", decoded.value))?;
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
