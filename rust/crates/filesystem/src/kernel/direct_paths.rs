//! Demand-driven paths from authenticated direct namespace bindings.

use super::bindings::{Binding, BindingFormat};
use super::persistent_diff::Values;
use super::{
    DecodeLimits, FileKind, FileRecord, GenerationRoot, KernelAccounting, LogicalName,
    NamespacePath, Owned,
};
use crate::async_storage::AsyncObjectStore;
use crate::{CancellationToken, FileId, FsError, VolumeConfig, WorkCounters, WorkError};

pub(super) fn validate_record(root_file_id: FileId, record: &FileRecord) -> Result<(), FsError> {
    let is_root = record.file_id == root_file_id;
    if record.link_count == 0
        || is_root
            && (record.bindings.is_some()
                || record.link_count != 1
                || record.kind != FileKind::Directory)
        || !is_root && record.bindings.is_none()
    {
        return Err(FsError::InvalidDiff);
    }
    Ok(())
}

/// Retains the exact alias traversal position for one immutable file record.
/// Parent paths are reconstructed only when their alias is requested.
pub(crate) struct DirectPaths {
    edges: Option<Values<BindingFormat>>,
    record: FileRecord,
    root_file_id: FileId,
    file_table: crate::ObjectId,
    config: VolumeConfig,
    progress: Option<PathState>,
    emitted: u64,
    failed: bool,
}

impl DirectPaths {
    pub(crate) fn new(
        root: &GenerationRoot,
        record: FileRecord,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        accounting.check()?;
        validate_record(root.root_file_id, &record)?;
        let edges = record
            .bindings
            .map(|id| Values::new(id, DecodeLimits::for_volume(config), accounting))
            .transpose()?;
        Ok(Self {
            edges,
            record,
            root_file_id: root.root_file_id,
            file_table: root.file_table,
            config,
            progress: None,
            emitted: 0,
            failed: false,
        })
    }

    pub(crate) async fn next<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Owned<NamespacePath>>, FsError> {
        if self.failed {
            return Err(FsError::InvalidDiff);
        }
        // A withdrawn page future cannot resume after consuming an alias. Its
        // retained path/frontier still retires through this original cursor.
        self.failed = true;
        let result = self.next_inner(store, accounting, cancellation).await;
        self.failed = result.is_err();
        result
    }

    async fn next_inner<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Owned<NamespacePath>>, FsError> {
        accounting.check()?;
        cancellation.check()?;
        let Some(edges) = &mut self.edges else {
            if self.emitted != 0 {
                return Ok(None);
            }
            let names = Owned::allocate(0, accounting)?;
            let path = NamespacePath::from_reversed_owned(names, self.config.limits, accounting)?;
            self.emitted = 1;
            return Ok(Some(path));
        };
        let Some(edge) = edges.next(store, accounting, cancellation).await? else {
            if self.emitted != self.record.link_count {
                return Err(FsError::InvalidDiff);
            }
            return Ok(None);
        };
        if self.emitted >= self.record.link_count {
            edge.retire(accounting)?;
            return Err(FsError::InvalidDiff);
        }
        self.progress = Some(PathState::new(self.record.file_id, edge, accounting)?);
        self.progress
            .as_mut()
            .ok_or(FsError::InvalidDiff)?
            .advance(
                store,
                self.root_file_id,
                self.file_table,
                self.config,
                accounting,
                cancellation,
            )
            .await?;
        let progress = self.progress.take().ok_or(FsError::InvalidDiff)?;
        let path = progress.finish(self.config, accounting)?;
        self.emitted += 1;
        Ok(Some(path))
    }

    pub(crate) fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let edges = self.edges.map_or(Ok(()), |edges| edges.retire(accounting));
        let path = self.progress.map_or(Ok(()), |path| path.retire(accounting));
        edges.and(path)
    }
}

/// Owned by DirectPaths, including while a page future is suspended or dropped.
pub(super) struct PathState {
    names: Owned<Vec<LogicalName>>,
    next: Option<Owned<Binding>>,
    parent: Option<FileId>,
    parent_edges: Option<Values<BindingFormat>>,
    checkpoint: FileId,
    power: u64,
    distance: u64,
}

impl PathState {
    pub(super) fn new(
        file_id: FileId,
        edge: Owned<Binding>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        let names = match Owned::allocate(0, accounting) {
            Ok(names) => names,
            Err(error) => {
                edge.retire(accounting)?;
                return Err(error);
            }
        };
        Ok(Self {
            names,
            next: Some(edge),
            parent: None,
            parent_edges: None,
            checkpoint: file_id,
            power: 1,
            distance: 0,
        })
    }

    pub(super) async fn advance<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        root_file_id: FileId,
        file_table: crate::ObjectId,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        loop {
            accounting.check()?;
            cancellation.check()?;
            if let Some(edge) = self.next.take() {
                if let Err(error) = self.names.ensure_for_push(usize::MAX, accounting) {
                    edge.retire(accounting)?;
                    return Err(error);
                }
                let Binding { parent, name } = edge.transfer_to(
                    &mut self.names.allocation,
                    &mut self.names.bytes,
                    accounting,
                )?;
                self.names.value.push(name);
                accounting.charge(WorkCounters {
                    source_path_components: 1,
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                if parent == root_file_id {
                    return Ok(());
                }
                // Brent's online cycle detector adds no allocation or second
                // parent lookup: it observes the same edges used by the path.
                self.distance = self.distance.checked_add(1).ok_or(WorkError::Overflow)?;
                if parent == self.checkpoint {
                    return Err(FsError::InvalidDiff);
                }
                if self.distance == self.power {
                    self.checkpoint = parent;
                    self.power = self.power.checked_mul(2).ok_or(WorkError::Overflow)?;
                    self.distance = 0;
                }
                self.parent = Some(parent);
            }
            let parent = self.parent.ok_or(FsError::InvalidDiff)?;
            let (record, allocation) = super::lookup_file_record_async_with_accounting(
                store,
                file_table,
                parent,
                DecodeLimits::for_volume(config),
                accounting,
                cancellation,
                None,
            )
            .await?;
            // FileRecord owns no heap backing; retire its original guard.
            Owned::new((), allocation, 0).retire(accounting)?;
            let record = record.ok_or(FsError::InvalidDiff)?;
            if record.kind != FileKind::Directory || record.link_count != 1 {
                return Err(FsError::InvalidDiff);
            }
            self.parent_edges = Some(Values::<BindingFormat>::new(
                record.bindings.ok_or(FsError::InvalidDiff)?,
                DecodeLimits::for_volume(config),
                accounting,
            )?);
            let edges = self.parent_edges.as_mut().ok_or(FsError::InvalidDiff)?;
            self.next = edges.next(store, accounting, cancellation).await?;
            if self.next.is_none() {
                return Err(FsError::InvalidDiff);
            }
            if let Some(extra) = edges.next(store, accounting, cancellation).await? {
                extra.retire(accounting)?;
                return Err(FsError::InvalidDiff);
            }
            self.parent_edges
                .take()
                .ok_or(FsError::InvalidDiff)?
                .retire(accounting)?;
            self.parent = None;
        }
    }

    pub(super) fn finish(
        self,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Owned<NamespacePath>, FsError> {
        if self.next.is_some() || self.parent_edges.is_some() {
            self.retire(accounting)?;
            return Err(FsError::InvalidDiff);
        }
        NamespacePath::from_reversed_owned(self.names, config.limits, accounting)
    }

    pub(super) fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let edge = self.next.map_or(Ok(()), |edge| edge.retire(accounting));
        let parents = self
            .parent_edges
            .map_or(Ok(()), |edges| edges.retire(accounting));
        let names = self.names.retire(accounting);
        edge.and(parents).and(names)
    }
}
