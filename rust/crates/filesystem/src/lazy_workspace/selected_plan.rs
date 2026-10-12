use super::*;
/// Metadata-only selection frozen from an authenticated lazy cut.
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
    /// Retained vector and path backing.
    pub fn logical_owned_bytes(&self) -> u64 {
        (self.roots.capacity() * std::mem::size_of::<String>()
            + self.entries.capacity() * std::mem::size_of::<SelectedImportRecord>()) as u64
            + self.roots.iter().map(|p| p.capacity() as u64).sum::<u64>()
            + self
                .entries
                .iter()
                .map(|e| e.path.capacity() as u64)
                .sum::<u64>()
    }
    pub(crate) fn input_digest(
        &self,
        destination: LazySnapshotRef,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<crate::Digest, LazyWorkspaceError> {
        let domain = b"acyclic-fs-selected-import-v1\0";
        let mut hash = blake3::Hasher::new();
        ctx.charge(WorkCounters {
            bytes_hashed: domain.len() as u64 + 64,
            ..WorkCounters::UNCHARGED
        })?;
        hash.update(domain);
        hash.update(&destination.id.into_bytes());
        hash.update(&self.source.id.into_bytes());
        for root in &self.roots {
            ctx.charge(WorkCounters {
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
    /// Freezes selected namespace records without reading file bodies.
    pub async fn prepare_selected_import(
        &self,
        cut: LazySnapshotRef,
        requested: &[NamespacePath],
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(SelectedImportPlan, KernelOwnedAllocation), LazyWorkspaceError> {
        self.validate_snapshot(cut, ctx)?;
        let mut roots = Owned::allocate(requested.len(), ctx)?;
        let result = async {
            for path in requested {
                let (text, guard) = path.unicode_text_with_accounting(ctx, cancellation(ctx))?;
                let bytes = text.capacity() as u64;
                crate::Transaction::<A, O>::push_mutation(
                    &mut roots,
                    Owned::new(text, guard, bytes),
                    ctx,
                )?;
            }
            let (mut paths, mut used) = self.selected_paths(cut, &roots.value, ctx).await?;
            let mut entries = Owned::allocate(paths.value.len(), ctx)?;
            let mut checkout = self
                .checkout(
                    crate::model::GenerationSelector::Exact(cut.authored_generation),
                    ctx,
                )
                .await?;
            let state =
                LazyWorkspaceState::new(cut.workspace_id, cut.source, cut.overlay, cut.shadows);
            let frozen = async {
                for path in paths.value.drain(..) {
                    let bytes = path.capacity() as u64;
                    let guard = crate::kernel::split_allocation(&mut paths.allocation, bytes)?;
                    paths.bytes -= bytes;
                    let mut path = Some(Owned::new(path, guard, bytes));
                    let captured = async {
                        let name = &path.as_ref().expect("path retained").value;
                        let (lookup, source, guard) = self
                            .inspect_unauthored(
                                &mut checkout.value,
                                name,
                                Some(cut),
                                InspectBasis {
                                    state: Some(&state),
                                    retain_pinned_observation: true,
                                    ..InspectBasis::default()
                                },
                                ctx,
                            )
                            .await?;
                        used |= source;
                        let record = match &lookup {
                            None => Ok(None),
                            Some(LazyLookup::Source { .. }) => {
                                Err(LazyWorkspaceError::UnsupportedNode)
                            }
                            Some(LazyLookup::Shadow { record, .. }) => {
                                crate::facade::read_record_metadata_to_file_root_with_accounting(
                                    &self.workspace.volume,
                                    *record,
                                    ctx,
                                    cancellation(ctx),
                                )
                                .await
                                .map(|meta| Some((*record, meta)))
                                .map_err(Into::into)
                            }
                            Some(LazyLookup::Authored { .. }) => {
                                self.authored_record(&mut checkout.value, name, ctx).await
                            }
                        };
                        if let Some(lookup) = lookup {
                            self.retire_lookup((lookup, guard), ctx)?;
                        } else {
                            crate::kernel::retire_allocation(guard, 0, ctx)?;
                        }
                        let record = record?;
                        let path = path.take().expect("captured path");
                        crate::Transaction::<A, O>::push_mutation(
                            &mut entries,
                            Owned::new(
                                SelectedImportRecord {
                                    path: path.value,
                                    record,
                                },
                                path.allocation,
                                path.bytes,
                            ),
                            ctx,
                        )?;
                        Ok::<_, LazyWorkspaceError>(())
                    }
                    .await;
                    if let Some(path) = path {
                        path.retire(ctx)?;
                    }
                    captured?;
                }
                Ok::<_, LazyWorkspaceError>(())
            }
            .await;
            checkout.retire(ctx)?;
            paths.retire(ctx)?;
            if let Err(error) = frozen {
                entries.retire(ctx)?;
                return Err(error);
            }
            Ok((entries, used))
        }
        .await;
        let (mut entries, used) = match result {
            Ok(value) => value,
            Err(error) => {
                roots.retire(ctx)?;
                return Err(error);
            }
        };
        entries.allocation.absorb(&mut roots.allocation)?;
        Ok((
            SelectedImportPlan {
                source: cut,
                source_config: self.workspace.volume.config,
                source_used: used,
                roots: roots.value,
                entries: entries.value,
            },
            entries.allocation,
        ))
    }

    pub(super) async fn selected_paths(
        &self,
        cut: LazySnapshotRef,
        roots: &[String],
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Owned<Vec<String>>, bool), LazyWorkspaceError> {
        self.validate_snapshot(cut, ctx)?;
        let state = LazyWorkspaceState::new(cut.workspace_id, cut.source, cut.overlay, cut.shadows);
        let mut paths = Owned::allocate(roots.len(), ctx)?;
        let mut used = false;
        let result = async {
            for root in roots {
                let (mut selected, source) = self.enumerate_paths(root, cut, &state, ctx).await?;
                used |= source;
                let moved = (|| {
                    for path in selected.value.drain(..) {
                        let mut duplicate = false;
                        for present in &paths.value {
                            ctx.charge(WorkCounters {
                                items_examined: 1,
                                ..WorkCounters::UNCHARGED
                            })?;
                            if present == &path {
                                duplicate = true;
                                break;
                            }
                        }
                        let bytes = path.capacity() as u64;
                        let guard =
                            crate::kernel::split_allocation(&mut selected.allocation, bytes)?;
                        selected.bytes -= bytes;
                        let path = Owned::new(path, guard, bytes);
                        if duplicate {
                            path.retire(ctx)?;
                            continue;
                        }
                        if let Err(error) = paths.ensure_for_push(usize::MAX, ctx) {
                            path.retire(ctx)?;
                            return Err(error.into());
                        }
                        crate::Transaction::<A, O>::push_mutation(&mut paths, path, ctx)?;
                    }
                    Ok::<_, LazyWorkspaceError>(())
                })();
                selected.retire(ctx)?;
                moved?;
            }
            Ok::<_, LazyWorkspaceError>(())
        }
        .await;
        if let Err(error) = result {
            paths.retire(ctx)?;
            return Err(error);
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
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(FileRecord, FileMetadata)>, LazyWorkspaceError> {
        let path = self.namespace_path(path, ctx)?;
        let record = checkout
            .lookup_no_follow_with_metadata_with_accounting(&path.value, ctx, cancellation(ctx))
            .await;
        path.retire(ctx)?;
        Ok(record?.map(|entry| (entry.record, entry.metadata)))
    }
}
