use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    pub(super) async fn append_overlay(
        &self,
        path: &str,
        change: LazyOverlayChange,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self.state(ctx).await?;
            let overlay = self
                .insert_overlay(state.overlay, path, change.clone(), ctx)
                .await?;
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
        mut root: LazyOverlayId,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<LazyOverlayChange>, LazyWorkspaceError> {
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
                    std::cmp::Ordering::Equal => Some(Some(change.clone())),
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

    pub(super) async fn shadow_record(
        &self,
        mut root: LazyShadowId,
        id: FileId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(FileRecord, crate::WorkspaceMetadata)>, LazyWorkspaceError> {
        loop {
            let Some(node) = self.store.load_lazy_shadow(root, ctx).await? else {
                return Ok(None);
            };
            let result: Result<_, LazyWorkspaceError> = (|| match &node.0 {
                LazyShadow::Empty => Ok(Some(None)),
                LazyShadow::Node {
                    file_id,
                    record,
                    metadata,
                    left,
                    right,
                    ..
                } => match id.cmp(file_id) {
                    std::cmp::Ordering::Equal => {
                        ctx.charge(WorkCounters {
                            items_examined: 1,
                            bytes_copied: record.len() as u64,
                            ..WorkCounters::UNCHARGED
                        })?;
                        Ok(Some(Some((
                            crate::kernel::decode_file_record(record)
                                .map_err(crate::FsError::from)?,
                            *metadata,
                        ))))
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
        let hold = accounted!(ctx, |work, cancel| self
            .workspace
            .volume
            .fs
            .make_records_durable(self.workspace.volume.config, &[record], work, cancel))?;
        ctx.charge(WorkCounters {
            bytes_hashed: 16,
            ..WorkCounters::UNCHARGED
        })?;
        let (bytes, guard) = crate::kernel::codec::encode_owned(ctx, |observe, error| {
            crate::kernel::encode_file_record(record, observe, error)
        })?;
        let node = LazyShadow::Node {
            file_id: id,
            priority: shadow_priority(id),
            record: bytes,
            metadata,
            left: root,
            right: root,
        };
        let result = treap::insert_shadow(&self.store, root, (node, guard), ctx).await;
        drop(hold);
        result
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
