use super::*;
use crate::kernel::{KernelAccounting, Owned};

/// Immutable authored records selected from an authenticated lazy cut.
#[derive(Debug)]
pub struct SelectedImportPlan {
    pub(crate) source: LazySnapshotRef,
    pub(crate) source_config: VolumeConfig,
    pub(crate) source_used: bool,
    pub(crate) roots: Vec<String>,
    pub(crate) entries: Vec<SelectedImportRecord>,
}

#[derive(Debug)]
pub(crate) struct SelectedImportRecord {
    pub(crate) path: String,
    pub(crate) record: Option<(FileRecord, FileMetadata)>,
}

impl SelectedImportPlan {
    /// Actual retained vector and name backing.
    pub fn logical_owned_bytes(&self) -> u64 {
        (self.roots.capacity() * std::mem::size_of::<String>()
            + self.entries.capacity() * std::mem::size_of::<SelectedImportRecord>()) as u64
            + self
                .roots
                .iter()
                .map(|root| root.capacity() as u64)
                .sum::<u64>()
            + self
                .entries
                .iter()
                .map(|entry| entry.path.capacity() as u64)
                .sum::<u64>()
    }
    pub(crate) fn input_digest(
        &self,
        destination: LazySnapshotRef,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<crate::Digest, LazyWorkspaceError> {
        let domain = b"acyclic-fs-selected-import-v1\0";
        let mut hash = blake3::Hasher::new();
        accounting.charge(WorkCounters {
            bytes_hashed: domain.len() as u64 + 64,
            ..WorkCounters::UNCHARGED
        })?;
        hash.update(domain);
        hash.update(&destination.id.into_bytes());
        hash.update(&self.source.id.into_bytes());
        for root in &self.roots {
            accounting.charge(WorkCounters {
                bytes_hashed: root.len() as u64 + 8,
                ..WorkCounters::UNCHARGED
            })?;
            hash.update(&(root.len() as u64).to_le_bytes());
            hash.update(root.as_bytes());
        }
        Ok(crate::Digest::from_bytes(*hash.finalize().as_bytes()))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Freezes selected namespace records without reading regular-file bodies.
    pub async fn prepare_selected_import(
        &self,
        snapshot: LazySnapshotRef,
        requested: &[NamespacePath],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(SelectedImportPlan, crate::kernel::KernelOwnedAllocation), LazyWorkspaceError>
    {
        self.validate_snapshot(snapshot, accounting)?;
        let mut roots = Owned::allocate(requested.len(), accounting)?;
        let result = async {
            for path in requested {
                let (text, allocation) =
                    path.unicode_text_with_accounting(accounting, cancellation(accounting))?;
                let bytes = text.capacity() as u64;
                crate::Transaction::<A, O>::push_workspace_mutation(
                    &mut roots,
                    Owned::new(text, allocation, bytes),
                    accounting,
                )?;
            }
            let (mut paths, mut source_used) = self
                .selected_paths(snapshot, &roots.value, accounting)
                .await?;
            let mut entries = Owned::allocate(paths.value.len(), accounting)?;
            let mut checkout = self
                .checkout(
                    crate::model::GenerationSelector::Exact(snapshot.authored_generation),
                    accounting,
                )
                .await?;
            let state = LazyWorkspaceState::new(
                snapshot.workspace_id,
                snapshot.source,
                snapshot.overlay,
                snapshot.shadows,
            );
            let frozen = async {
                for path in paths.value.drain(..) {
                    let (lookup, used, allocation) = self
                        .inspect_unauthored(
                            &mut checkout.value,
                            &path,
                            Some(snapshot),
                            InspectBasis {
                                state: Some(&state),
                                retain_pinned_observation: true,
                                ..InspectBasis::default()
                            },
                            accounting,
                        )
                        .await?;
                    source_used |= used;
                    let record = match &lookup {
                        None => Ok(None),
                        Some(LazyLookup::Source { .. }) => Err(LazyWorkspaceError::UnsupportedNode),
                        Some(LazyLookup::Shadow { record, .. }) => {
                            crate::facade::read_record_metadata_to_file_root_with_accounting(
                                &self.workspace.volume,
                                *record,
                                accounting,
                                cancellation(accounting),
                            )
                            .await
                            .map(|metadata| Some((*record, metadata)))
                            .map_err(Into::into)
                        }
                        Some(LazyLookup::Authored { .. }) => {
                            self.authored_record(&mut checkout.value, &path, accounting)
                                .await
                        }
                    };
                    if let Some(lookup) = lookup {
                        self.retire_lookup((lookup, allocation), accounting)?;
                    } else {
                        crate::kernel::retire_allocation(allocation, 0, accounting)?;
                    }
                    let record = record?;
                    accounting.charge(WorkCounters {
                        bytes_copied: std::mem::size_of::<SelectedImportRecord>() as u64,
                        ..WorkCounters::UNCHARGED
                    })?;
                    entries.value.push(SelectedImportRecord { path, record });
                }
                Ok::<_, LazyWorkspaceError>(())
            }
            .await;
            checkout.retire(accounting)?;
            if let Err(error) = frozen {
                entries.retire(accounting)?;
                paths.retire(accounting)?;
                return Err(error);
            }
            let slots = (paths.value.capacity() * std::mem::size_of::<String>()) as u64;
            drop(paths.value);
            accounting.release_owned(&mut paths.allocation, slots)?;
            entries.allocation.absorb(&mut paths.allocation)?;
            entries.bytes = entries
                .bytes
                .checked_add(paths.bytes - slots)
                .ok_or(crate::WorkError::Overflow)?;
            Ok((entries, source_used))
        }
        .await;
        let (mut entries, source_used) = match result {
            Ok(result) => result,
            Err(error) => {
                roots.retire(accounting)?;
                return Err(error);
            }
        };
        entries.allocation.absorb(&mut roots.allocation)?;
        Ok((
            SelectedImportPlan {
                source: snapshot,
                source_config: self.workspace.volume.config,
                source_used,
                roots: roots.value,
                entries: entries.value,
            },
            entries.allocation,
        ))
    }

    pub(super) async fn selected_paths(
        &self,
        snapshot: LazySnapshotRef,
        roots: &[String],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Owned<Vec<String>>, bool), LazyWorkspaceError> {
        self.validate_snapshot(snapshot, accounting)?;
        let state = LazyWorkspaceState::new(
            snapshot.workspace_id,
            snapshot.source,
            snapshot.overlay,
            snapshot.shadows,
        );
        let mut paths = Owned::allocate(roots.len(), accounting)?;
        let mut used = false;
        for root in roots {
            let (mut selected, source_used) = self
                .enumerate_paths(root, snapshot, &state, accounting)
                .await?;
            used |= source_used;
            let copied = (|| {
                for path in selected.value.drain(..) {
                    let mut duplicate = false;
                    for present in &paths.value {
                        accounting.charge(WorkCounters {
                            items_examined: 1,
                            ..WorkCounters::UNCHARGED
                        })?;
                        if present == &path {
                            duplicate = true;
                            break;
                        }
                    }
                    if duplicate {
                        let bytes = path.capacity() as u64;
                        drop(path);
                        accounting.release_owned(&mut selected.allocation, bytes)?;
                        selected.bytes -= bytes;
                    } else {
                        paths.ensure_for_push(usize::MAX, accounting)?;
                        accounting.charge(WorkCounters {
                            bytes_copied: std::mem::size_of::<String>() as u64,
                            ..WorkCounters::UNCHARGED
                        })?;
                        paths.value.push(path);
                    }
                }
                Ok::<_, LazyWorkspaceError>(())
            })();
            if let Err(error) = copied {
                paths.retire(accounting)?;
                selected.retire(accounting)?;
                return Err(error);
            }
            let slots = (selected.value.capacity() * std::mem::size_of::<String>()) as u64;
            drop(selected.value);
            accounting.release_owned(&mut selected.allocation, slots)?;
            paths.allocation.absorb(&mut selected.allocation)?;
            paths.bytes = paths
                .bytes
                .checked_add(selected.bytes - slots)
                .ok_or(crate::WorkError::Overflow)?;
        }
        paths
            .value
            .sort_unstable_by(|a, b| exactify_path_order(a, b));
        Ok((paths, used))
    }

    pub(super) async fn authored_record(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<(FileRecord, FileMetadata)>, LazyWorkspaceError> {
        let namespace = self.namespace_path(path, accounting)?;
        let result = checkout
            .lookup_no_follow_with_metadata_with_accounting(
                &namespace.value,
                accounting,
                cancellation(accounting),
            )
            .await;
        namespace.retire(accounting)?;
        Ok(result?.map(|entry| (entry.record, entry.metadata)))
    }
}
