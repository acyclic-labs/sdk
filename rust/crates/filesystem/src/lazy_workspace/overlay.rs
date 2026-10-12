use super::*;
use crate::kernel::{KernelAccounting, Owned};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    pub(super) async fn append_overlay(
        &self,
        path: &str,
        change: LazyOverlayChange,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self.state(accounting).await?;
            let overlay = self
                .insert_overlay(state.overlay, path, change.clone(), accounting)
                .await?;
            let replacement = LazyWorkspaceState {
                revision: state
                    .revision
                    .checked_add(1)
                    .ok_or(crate::WorkError::Overflow)?,
                overlay,
                ..state
            };
            if self
                .store
                .compare_and_swap_lazy_workspace(
                    self.workspace.id(),
                    replacement.revision - 1,
                    replacement,
                    accounting,
                )
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
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<LazyOverlayId, LazyWorkspaceError> {
        let path = self.canonical_path(path, accounting)?;
        insert_immutable_treap(
            &OverlayTreap(&self.store),
            root,
            Owned::new((path.value, change), path.allocation, path.bytes),
            accounting,
        )
        .await
    }

    pub(super) async fn overlay_fact(
        &self,
        mut root: LazyOverlayId,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<LazyOverlayChange>, LazyWorkspaceError> {
        loop {
            let Some(node) = self.store.load_lazy_overlay(root, accounting).await? else {
                return Ok(None);
            };
            let result = if let LazyOverlay::Node {
                path: key,
                change,
                left,
                right,
                ..
            } = &node.0
            {
                match path.cmp(key) {
                    std::cmp::Ordering::Equal => Some(Some(change.clone())),
                    std::cmp::Ordering::Less => {
                        root = *left;
                        None
                    }
                    std::cmp::Ordering::Greater => {
                        root = *right;
                        None
                    }
                }
            } else {
                Some(None)
            };
            retire_node(node, accounting)?;
            if let Some(result) = result {
                return Ok(result);
            }
        }
    }

    pub(super) async fn shadow_record(
        &self,
        mut root: LazyShadowId,
        file_id: FileId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<(FileRecord, crate::WorkspaceMetadata)>, LazyWorkspaceError> {
        loop {
            let Some(node) = self.store.load_lazy_shadow(root, accounting).await? else {
                return Ok(None);
            };
            let result = if let LazyShadow::Node {
                file_id: key,
                record,
                metadata,
                left,
                right,
                ..
            } = &node.0
            {
                match file_id.cmp(key) {
                    std::cmp::Ordering::Equal => {
                        accounting.charge(WorkCounters {
                            items_examined: 1,
                            bytes_copied: record.len() as u64,
                            ..WorkCounters::UNCHARGED
                        })?;
                        Some(Some((
                            crate::kernel::decode_file_record(record)
                                .map_err(crate::FsError::from)?,
                            **metadata,
                        )))
                    }
                    std::cmp::Ordering::Less => {
                        root = *left;
                        None
                    }
                    std::cmp::Ordering::Greater => {
                        root = *right;
                        None
                    }
                }
            } else {
                Some(None)
            };
            retire_node(node, accounting)?;
            if let Some(result) = result {
                return Ok(result);
            }
        }
    }

    pub(super) async fn insert_shadow(
        &self,
        root: LazyShadowId,
        file_id: FileId,
        record: FileRecord,
        metadata: crate::WorkspaceMetadata,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<LazyShadowId, LazyWorkspaceError> {
        let fs = &self.workspace.volume.fs;
        let config = self.workspace.volume.config;
        let cancellation = cancellation(accounting);
        let hold = {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                Some(
                    fs.make_records_durable_with_native_work(
                        config,
                        &[record],
                        original,
                        cancellation,
                    )
                    .await?,
                )
            } else {
                None
            }
            #[cfg(target_arch = "wasm32")]
            {
                None::<crate::PublicationHold>
            }
        };
        let hold = if let Some(hold) = hold {
            hold
        } else {
            accept_receipt(
                fs.make_records_durable(config, &[record], accounting.remaining()?, cancellation)
                    .await,
                accounting,
            )?
        };
        let mut metadata_allocation =
            accounting.claim_owned(std::mem::size_of::<crate::WorkspaceMetadata>() as u64, 1)?;
        let (bytes, mut allocation) =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_file_record(record, observe, allocation_error)
            })?;
        let metadata = Box::new(metadata);
        allocation.absorb(&mut metadata_allocation)?;
        let retained =
            bytes.capacity() as u64 + std::mem::size_of::<crate::WorkspaceMetadata>() as u64;
        let result = insert_immutable_treap(
            &ShadowTreap(&self.store),
            root,
            Owned::new(
                (
                    file_id,
                    ShadowValue {
                        record: bytes,
                        metadata,
                    },
                ),
                allocation,
                retained,
            ),
            accounting,
        )
        .await;
        drop(hold);
        result
    }

    pub(super) async fn tombstoned(
        &self,
        root: LazyOverlayId,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        PortablePath::validate(path, self.workspace.limits())
            .map_err(crate::kernel::NamespacePathError::Path)?;
        if matches!(
            self.overlay_fact(root, "/", accounting).await?,
            Some(LazyOverlayChange::Tombstone { .. })
        ) {
            return Ok(true);
        }
        for (index, byte) in path.bytes().enumerate().skip(1) {
            if byte == b'/'
                && matches!(
                    self.overlay_fact(root, &path[..index], accounting).await?,
                    Some(LazyOverlayChange::Tombstone { .. })
                )
            {
                return Ok(true);
            }
        }
        Ok(matches!(
            self.overlay_fact(root, path, accounting).await?,
            Some(LazyOverlayChange::Tombstone { .. })
        ))
    }
}
