use super::*;

#[derive(Clone, Copy, Default)]
pub(super) struct ShadowRecords {
    pub(super) authored: Option<(FileRecord, crate::WorkspaceMetadata)>,
    pub(super) observed: Option<(
        SourceReference,
        SourceVersion,
        FileRecord,
        crate::WorkspaceMetadata,
    )>,
}

impl ShadowRecords {
    pub(super) fn at_source(
        self,
        source: SourceReference,
        version: SourceVersion,
    ) -> Option<(FileRecord, crate::WorkspaceMetadata)> {
        self.observed
            .filter(|(original, original_version, _, _)| {
                *original == source && *original_version == version
            })
            .map(|(_, _, record, metadata)| (record, metadata))
    }
}

fn decode_shadow_record(
    value: &LazyStoredRecord,
    id: FileId,
    ctx: &mut KernelAccounting<'_>,
) -> Result<(FileRecord, crate::WorkspaceMetadata), LazyWorkspaceError> {
    ctx.charge(WorkCounters {
        items_examined: 1,
        bytes_copied: value.record.len() as u64,
        ..WorkCounters::UNCHARGED
    })?;
    let record = crate::kernel::decode_file_record(&value.record).map_err(crate::FsError::from)?;
    if record.file_id != id {
        return Err(LazyWorkspaceError::StaleIdentity);
    }
    Ok((record, value.metadata))
}

fn encode_shadow_record(
    (record, metadata): (FileRecord, crate::WorkspaceMetadata),
    ctx: &mut KernelAccounting<'_>,
) -> Result<Owned<LazyStoredRecord>, LazyWorkspaceError> {
    let (record, allocation) = crate::kernel::codec::encode_owned(ctx, |observe, error| {
        crate::kernel::encode_file_record(record, observe, error)
    })?;
    let bytes = record.capacity() as u64;
    Ok(Owned::new(
        LazyStoredRecord { record, metadata },
        allocation,
        bytes,
    ))
}

impl<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
> LazyWorkspace<A, O, D, S>
{
    pub(super) async fn append_overlay(
        &self,
        path: &str,
        change: LazyOverlayChange,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let state = self.state(ctx).await?;
        self.append_overlay_at(state, path, change, ctx).await
    }

    pub(super) async fn append_overlay_at(
        &self,
        state: LazyWorkspaceState,
        path: &str,
        change: LazyOverlayChange,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let overlay = self
            .insert_overlay(state.overlay, path, change, ctx)
            .await?;
        self.publish_overlay(state, overlay, ctx).await
    }

    pub(super) async fn publish_overlay(
        &self,
        state: LazyWorkspaceState,
        overlay: LazyOverlayId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let revision = state.revision;
        let next = LazyWorkspaceState {
            revision: revision.checked_add(1).ok_or(crate::WorkError::Overflow)?,
            overlay,
            ..state
        };
        if self
            .store
            .compare_and_swap_lazy_workspace(self.workspace.id(), revision, next, ctx)
            .await?
        {
            return Ok(());
        }
        Err(LazyWorkspaceError::Concurrent)
    }

    pub(super) async fn insert_overlay(
        &self,
        root: LazyOverlayId,
        path: &str,
        change: LazyOverlayChange,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<LazyOverlayId, LazyWorkspaceError> {
        let path = self.canonical_path(path, ctx)?;
        if let Err(error) = ctx.charge(WorkCounters {
            bytes_hashed: path.value.len() as u64,
            ..WorkCounters::UNCHARGED
        }) {
            path.retire(ctx)?;
            return Err(error.into());
        }
        let priority = path_priority(&path.value);
        treap::insert_overlay(
            &self.store,
            root,
            (
                LazyOverlay::Node {
                    path: path.value,
                    priority,
                    change,
                    left: root,
                    right: root,
                },
                path.allocation,
            ),
            ctx,
        )
        .await
    }

    pub(super) async fn overlay_fact(
        &self,
        root: LazyOverlayId,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<LazyOverlayChange>, LazyWorkspaceError> {
        self.overlay_entry(root, path, ctx)
            .await
            .map(|entry| entry.map(|(_, change, _)| change))
    }

    pub(super) async fn overlay_entry(
        &self,
        mut root: LazyOverlayId,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyOverlayId, LazyOverlayChange, [LazyOverlayId; 2])>, LazyWorkspaceError>
    {
        loop {
            let Some(node) = self.store.load_lazy_overlay(root, ctx).await? else {
                return Ok(None);
            };
            let result = match &node.0 {
                LazyOverlay::Empty => Some(None),
                LazyOverlay::Node {
                    path: key,
                    change,
                    left,
                    right,
                    ..
                } => match path.cmp(key) {
                    std::cmp::Ordering::Equal => {
                        Some(Some((root, change.clone(), [*left, *right])))
                    }
                    order => {
                        root = if order.is_lt() { *left } else { *right };
                        None
                    }
                },
            };
            retire_node(node, ctx)?;
            if let Some(value) = result {
                return Ok(value);
            }
        }
    }

    pub(super) async fn shadow_records(
        &self,
        mut root: LazyShadowId,
        id: FileId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<ShadowRecords>, LazyWorkspaceError> {
        loop {
            let Some(node) = self.store.load_lazy_shadow(root, ctx).await? else {
                return Ok(None);
            };
            let result: Result<_, LazyWorkspaceError> = (|| match &node.0 {
                LazyShadow::Empty => Ok(Some(None)),
                LazyShadow::Node {
                    file_id,
                    authored,
                    observed,
                    left,
                    right,
                    ..
                } => match id.cmp(file_id) {
                    std::cmp::Ordering::Equal => {
                        let authored = authored
                            .as_ref()
                            .map(|record| decode_shadow_record(record, id, ctx))
                            .transpose()?;
                        let observed = observed
                            .as_ref()
                            .map(|record| {
                                decode_shadow_record(&record.content, id, ctx).map(
                                    |(content, metadata)| {
                                        (record.source, record.version, content, metadata)
                                    },
                                )
                            })
                            .transpose()?;
                        if authored.is_none() && observed.is_none() {
                            return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
                        }
                        Ok(Some(Some(ShadowRecords { authored, observed })))
                    }
                    order => {
                        root = if order.is_lt() { *left } else { *right };
                        Ok(None)
                    }
                },
            })();
            retire_node(node, ctx)?;
            if let Some(value) = result? {
                return Ok(value);
            }
        }
    }

    pub(super) async fn insert_shadow(
        &self,
        root: LazyShadowId,
        id: FileId,
        record: FileRecord,
        metadata: crate::WorkspaceMetadata,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<LazyShadowId, LazyWorkspaceError> {
        self.insert_shadow_records(
            root,
            id,
            ShadowRecords {
                authored: Some((record, metadata)),
                observed: None,
            },
            ctx,
        )
        .await
    }

    async fn insert_shadow_records(
        &self,
        root: LazyShadowId,
        id: FileId,
        records: ShadowRecords,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<LazyShadowId, LazyWorkspaceError> {
        let (durable, count) = match (records.authored, records.observed) {
            (Some((authored, _)), Some((_, _, observed, _))) => ([authored, observed], 2),
            (Some((record, _)), None) | (None, Some((_, _, record, _))) => ([record, record], 1),
            (None, None) => return Err(LazyWorkspaceError::OriginalSnapshotUnavailable),
        };
        let hold = accounted!(ctx, |work, cancel| self
            .workspace
            .volume
            .fs
            .make_records_durable(
                self.workspace.volume.config,
                &durable[..count],
                work,
                cancel
            ));
        let hold = hold?;
        ctx.charge(WorkCounters {
            bytes_hashed: 16,
            ..WorkCounters::UNCHARGED
        })?;
        let authored = records
            .authored
            .map(|record| encode_shadow_record(record, ctx))
            .transpose()?;
        let observed = records
            .observed
            .map(|(source, version, record, metadata)| {
                encode_shadow_record((record, metadata), ctx)
                    .map(|record| (source, version, record))
            })
            .transpose();
        let observed = match observed {
            Ok(observed) => observed,
            Err(error) => {
                if let Some(authored) = authored {
                    authored.retire(ctx)?;
                }
                return Err(error);
            }
        };
        let (authored, observed, guard) = match (authored, observed) {
            (Some(mut authored), Some((source, version, mut observed))) => {
                if let Err(error) = authored.allocation.absorb(&mut observed.allocation) {
                    observed.retire(ctx)?;
                    authored.retire(ctx)?;
                    return Err(error.into());
                }
                (
                    Some(authored.value),
                    Some(LazySourceRecord {
                        source,
                        version,
                        content: observed.value,
                    }),
                    authored.allocation,
                )
            }
            (Some(authored), None) => (Some(authored.value), None, authored.allocation),
            (None, Some((source, version, observed))) => (
                None,
                Some(LazySourceRecord {
                    source,
                    version,
                    content: observed.value,
                }),
                observed.allocation,
            ),
            (None, None) => return Err(LazyWorkspaceError::OriginalSnapshotUnavailable),
        };
        let node = LazyShadow::Node {
            file_id: id,
            priority: shadow_priority(id),
            authored,
            observed,
            left: root,
            right: root,
        };
        let result = treap::insert_shadow(&self.store, root, (node, guard), ctx).await;
        drop(hold);
        result
    }

    pub(super) async fn retain_source_before_image(
        &self,
        source: SourceReference,
        version: SourceVersion,
        record: FileRecord,
        metadata: crate::WorkspaceMetadata,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let state = self.state(ctx).await?;
        if state.source != source {
            return Err(LazyWorkspaceError::StaleSource);
        }
        let mut records = self
            .shadow_records(state.shadows, record.file_id, ctx)
            .await?
            .unwrap_or_default();
        if records
            .observed
            .as_ref()
            .is_some_and(|(observed_source, observed_version, _, _)| {
                *observed_source == source && *observed_version == version
            })
        {
            return Ok(());
        }
        records.observed = Some((source, version, record, metadata));
        let shadows = self
            .insert_shadow_records(state.shadows, record.file_id, records, ctx)
            .await?;
        if shadows == state.shadows {
            return Ok(());
        }
        let revision = state.revision;
        let next = LazyWorkspaceState {
            revision: revision.checked_add(1).ok_or(crate::WorkError::Overflow)?,
            shadows,
            ..state
        };
        if self
            .store
            .compare_and_swap_lazy_workspace(self.workspace.id(), revision, next, ctx)
            .await?
        {
            Ok(())
        } else {
            Err(LazyWorkspaceError::Concurrent)
        }
    }

    pub(super) async fn tombstoned(
        &self,
        root: LazyOverlayId,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        PortablePath::validate(path, self.workspace.limits())
            .map_err(crate::kernel::NamespacePathError::Path)?;
        let mut ancestor = path;
        loop {
            if matches!(
                self.overlay_fact(root, ancestor, ctx).await?,
                Some(LazyOverlayChange::Tombstone { .. })
            ) {
                return Ok(true);
            }
            if ancestor == "/" {
                return Ok(false);
            }
            ancestor = parent_path(ancestor).ok_or(LazyWorkspaceError::UnsupportedNode)?;
        }
    }
}
