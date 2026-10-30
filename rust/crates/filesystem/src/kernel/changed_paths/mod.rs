//! One retained changed-path producer over two immutable file-table frontiers.

use super::bindings::{Binding, BindingFormat};
use super::direct_paths::{DirectPaths, PathState};
use super::file_table_mutation::FileTableFormat;
use super::persistent_diff::{Cursor as FileCursor, Values};
use super::tree_mutation::TreeFormat;
use super::{
    DecodeLimits, FileKind, FilePayload, FileRecord, GenerationRoot, KernelAccounting,
    NamespacePath, Owned, TreeEntry,
};
use crate::async_storage::AsyncObjectStore;
use crate::{CancellationToken, FileId, FsError, ObjectId, VolumeConfig};

mod descendants;
use descendants::Descendants;

/// One changed exact path between two immutable generations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangedPath {
    /// Canonical exact path in either endpoint.
    pub path: NamespacePath,
    /// Complete path-independent record in the earlier generation, when present.
    pub before: Option<FileRecord>,
    /// Complete path-independent record in the later generation, when present.
    pub after: Option<FileRecord>,
}

pub(crate) fn record_changed(before: &FileRecord, after: &FileRecord) -> bool {
    before.file_id != after.file_id
        || before.kind != after.kind
        || before.metadata != after.metadata
        || before.kind != FileKind::Directory && before.payload != after.payload
}

struct Active {
    records: [Option<FileRecord>; 2],
    side: usize,
    paths: Option<DirectPaths>,
    edges: Option<FileCursor<BindingFormat>>,
    edge_path: Option<(usize, PathState)>,
}

impl Active {
    fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let paths = self.paths.map_or(Ok(()), |paths| paths.retire(accounting));
        let edges = self.edges.map_or(Ok(()), |edges| edges.retire(accounting));
        let path = self
            .edge_path
            .map_or(Ok(()), |(_, path)| path.retire(accounting));
        paths.and(edges).and(path)
    }
}

struct Candidate {
    path: Owned<NamespacePath>,
    record: FileRecord,
    side: usize,
}

/// Retains the pair frontier, current aliases, and any moved-directory walk.
/// Each file frontier entry emits endpoint aliases in direct-binding order.
/// A moved directory expands its retained subtree before the next frontier entry.
pub(crate) struct Cursor<'a> {
    files: Source<'a>,
    endpoints: [Option<(FileId, ObjectId)>; 2],
    config: VolumeConfig,
    active: Option<Active>,
    descendants: Option<Descendants>,
    candidate: Option<Candidate>,
    failed: bool,
    complete: bool,
}

enum Source<'a> {
    Frontier(FileCursor<FileTableFormat>),
    Recorded(std::slice::Iter<'a, super::FileRecordChange>),
}

impl Source<'_> {
    async fn next<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<[Option<FileRecord>; 2]>, FsError> {
        match self {
            Self::Frontier(cursor) => {
                let Some(change) = cursor.next(store, accounting, cancellation).await? else {
                    return Ok(None);
                };
                let records = [change.value.before, change.value.after];
                change.retire(accounting)?;
                Ok(Some(records))
            }
            Self::Recorded(records) => {
                let Some(change) = records.as_slice().first() else {
                    return Ok(None);
                };
                accounting.charge(crate::WorkCounters {
                    items_examined: 1,
                    bytes_copied: std::mem::size_of::<super::FileRecordChange>() as u64,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                let result = [change.before, change.after];
                records.next();
                Ok(Some(result))
            }
        }
    }

    fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        match self {
            Self::Frontier(cursor) => cursor.retire(accounting),
            Self::Recorded(_) => Ok(()),
        }
    }
}

fn validate_endpoints(
    roots: [Option<&GenerationRoot>; 2],
    config: VolumeConfig,
) -> Result<[Option<(FileId, ObjectId)>; 2], FsError> {
    config.validate()?;
    if let [Some(before), Some(after)] = roots
        && before.root_file_id != after.root_file_id
    {
        return Err(FsError::InvalidDiff);
    }
    Ok(roots.map(|root| root.map(|root| (root.root_file_id, root.file_table))))
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(
        roots: [Option<&GenerationRoot>; 2],
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        let endpoints = validate_endpoints(roots, config)?;
        Ok(Self {
            files: Source::Frontier(FileCursor::new(
                roots[0].map(|root| root.file_table),
                roots[1].map(|root| root.file_table),
                DecodeLimits::for_volume(config),
                accounting,
            )?),
            endpoints,
            config,
            active: None,
            descendants: None,
            candidate: None,
            failed: false,
            complete: false,
        })
    }

    /// A previously collected generation delta supplies the same producer's
    /// initial records, without reopening its already authenticated frontier.
    pub(crate) fn from_records(
        roots: [Option<&GenerationRoot>; 2],
        config: VolumeConfig,
        records: &'a [super::FileRecordChange],
    ) -> Result<Self, FsError> {
        Ok(Self {
            endpoints: validate_endpoints(roots, config)?,
            files: Source::Recorded(records.iter()),
            config,
            active: None,
            descendants: None,
            candidate: None,
            failed: false,
            complete: false,
        })
    }

    pub(crate) async fn next<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        roots: [Option<&GenerationRoot>; 2],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Owned<ChangedPath>>, FsError> {
        if self.failed
            || roots.map(|root| root.map(|root| (root.root_file_id, root.file_table)))
                != self.endpoints
        {
            return Err(FsError::InvalidDiff);
        }
        // Withdrawal after advancing an underlying page is terminal. The
        // original state remains reachable here until explicit retirement.
        self.failed = true;
        let result = self
            .next_inner(store, roots, accounting, cancellation)
            .await;
        self.failed = result.is_err();
        result
    }

    async fn next_inner<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        roots: [Option<&GenerationRoot>; 2],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Owned<ChangedPath>>, FsError> {
        loop {
            accounting.check()?;
            cancellation.check()?;
            if self.complete {
                return Ok(None);
            }
            if self.candidate.is_some() {
                if let Some(row) = self
                    .resolve_candidate(store, roots, accounting, cancellation)
                    .await?
                {
                    return Ok(Some(row));
                }
                continue;
            }
            if let Some(descendants) = &mut self.descendants {
                let [Some(before), Some(after)] = roots else {
                    return Err(FsError::InvalidDiff);
                };
                if let Some(candidate) = descendants
                    .next(
                        store,
                        [before, after],
                        self.config,
                        accounting,
                        cancellation,
                    )
                    .await?
                {
                    self.candidate = Some(candidate);
                } else {
                    self.descendants
                        .take()
                        .ok_or(FsError::InvalidDiff)?
                        .retire(accounting)?;
                }
                continue;
            }
            if let Some(active) = &mut self.active {
                if let Some(edges) = &mut active.edges {
                    if active.edge_path.is_none() {
                        if let Some((side, edge)) = edges
                            .next_set_change(store, accounting, cancellation)
                            .await?
                        {
                            let record = active.records[side].ok_or(FsError::InvalidDiff)?;
                            active.edge_path =
                                Some((side, PathState::new(record.file_id, edge, accounting)?));
                        } else {
                            active
                                .edges
                                .take()
                                .ok_or(FsError::InvalidDiff)?
                                .retire(accounting)?;
                            active.side = 2;
                        }
                    }
                    if let Some((side, path)) = &mut active.edge_path {
                        let root = roots[*side].ok_or(FsError::InvalidDiff)?;
                        path.advance(
                            store,
                            root.root_file_id,
                            root.file_table,
                            self.config,
                            accounting,
                            cancellation,
                        )
                        .await?;
                        let (side, path) = active.edge_path.take().ok_or(FsError::InvalidDiff)?;
                        let path = path.finish(self.config, accounting)?;
                        let record = active.records[side].ok_or(FsError::InvalidDiff)?;
                        self.candidate = Some(Candidate { path, record, side });
                        continue;
                    }
                }
                if active.side < 2 {
                    let side = active.side;
                    if let Some(record) = active.records[side] {
                        if active.paths.is_none() {
                            active.paths = Some(DirectPaths::new(
                                roots[side].ok_or(FsError::InvalidDiff)?,
                                record,
                                self.config,
                                accounting,
                            )?);
                        }
                        if let Some(path) = active
                            .paths
                            .as_mut()
                            .ok_or(FsError::InvalidDiff)?
                            .next(store, accounting, cancellation)
                            .await?
                        {
                            self.candidate = Some(Candidate { path, record, side });
                            continue;
                        }
                        active
                            .paths
                            .take()
                            .ok_or(FsError::InvalidDiff)?
                            .retire(accounting)?;
                    }
                    active.side += 1;
                    continue;
                }
                let active = self.active.take().ok_or(FsError::InvalidDiff)?;
                if let [Some(before), Some(after)] = active.records
                    && before.kind == FileKind::Directory
                    && after.kind == FileKind::Directory
                    && before.bindings != after.bindings
                {
                    self.descendants = Some(Descendants::new(before, self.config, accounting)?);
                }
                continue;
            }
            let Some(records) = self.files.next(store, accounting, cancellation).await? else {
                self.complete = true;
                return Ok(None);
            };
            for (side, record) in records.iter().enumerate() {
                if let Some(record) = record {
                    super::direct_paths::validate_record(
                        roots[side].ok_or(FsError::InvalidDiff)?.root_file_id,
                        record,
                    )?;
                }
            }
            if let [Some(before), Some(after)] = records
                && before.bindings == after.bindings
                && !record_changed(&before, &after)
            {
                // Changed directory child roots do not change that directory's
                // path facts; its changed children remain on the same frontier.
                continue;
            }
            let edges = if let [Some(before), Some(after)] = records
                && !record_changed(&before, &after)
            {
                Some(FileCursor::new(
                    before.bindings,
                    after.bindings,
                    DecodeLimits::for_volume(self.config),
                    accounting,
                )?)
            } else {
                None
            };
            self.active = Some(Active {
                records,
                side: 0,
                paths: None,
                edges,
                edge_path: None,
            });
        }
    }

    async fn resolve_candidate<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        roots: [Option<&GenerationRoot>; 2],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Owned<ChangedPath>>, FsError> {
        let candidate = self.candidate.as_ref().ok_or(FsError::InvalidDiff)?;
        let other = if let Some(root) = roots[1 - candidate.side] {
            match super::lookup_path_async_with_accounting(
                store,
                root,
                &candidate.path.value,
                self.config,
                accounting,
                cancellation,
                None,
            )
            .await
            {
                Ok(found) => found.record,
                // A proven non-directory prefix proves absence of this descendant.
                Err(FsError::Path(super::PathLookupError::NotDirectory)) => None,
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        let candidate = self.candidate.take().ok_or(FsError::InvalidDiff)?;
        let records = if candidate.side == 0 {
            [Some(candidate.record), other]
        } else {
            [other, Some(candidate.record)]
        };
        // One identity owns a replaced path, without an output-history set.
        // A retained before alias owns same-identity facts on both endpoints.
        let owns = other.is_none_or(|other| {
            candidate.record.file_id < other.file_id
                || candidate.side == 0 && candidate.record.file_id == other.file_id
        });
        let changed = match records {
            [Some(before), Some(after)] => record_changed(&before, &after),
            _ => true,
        };
        if !owns || !changed {
            candidate.path.retire(accounting)?;
            return Ok(None);
        }
        Ok(Some(Owned::new(
            ChangedPath {
                path: candidate.path.value,
                before: records[0],
                after: records[1],
            },
            candidate.path.allocation,
            candidate.path.bytes,
        )))
    }

    pub(crate) fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let row = self
            .candidate
            .map_or(Ok(()), |candidate| candidate.path.retire(accounting));
        let active = self
            .active
            .map_or(Ok(()), |active| active.retire(accounting));
        let descendants = self
            .descendants
            .map_or(Ok(()), |walk| walk.retire(accounting));
        let files = self.files.retire(accounting);
        row.and(active).and(descendants).and(files)
    }
}

async fn record<S: AsyncObjectStore>(
    store: &S,
    root: &GenerationRoot,
    file_id: FileId,
    config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<FileRecord>, FsError> {
    let (record, allocation) = super::lookup_file_record_async_with_accounting(
        store,
        root.file_table,
        file_id,
        DecodeLimits::for_volume(config),
        accounting,
        cancellation,
        None,
    )
    .await?;
    // FileRecord is inline; no record backing survives this original guard.
    Owned::new((), allocation, 0).retire(accounting)?;
    Ok(record)
}
