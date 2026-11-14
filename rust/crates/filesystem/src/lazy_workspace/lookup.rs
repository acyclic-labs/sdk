use super::*;

/// Facts already established by the caller's authored checkout or source page.
#[derive(Clone, Copy, Default)]
pub(crate) struct InspectBasis<'a> {
    pub(crate) state: Option<&'a LazyWorkspaceState>,
    pub(crate) authored_absent: bool,
    pub(crate) listed: Option<SourceNode>,
    pub(crate) retain_pinned_observation: bool,
    pub(crate) unobserved: bool,
}

/// Scalar path classification without fetching content.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LazyStat {
    /// Observed kind.
    pub kind: SourceNodeKind,
    /// Logical length when meaningful.
    pub logical_bytes: Option<u64>,
    /// Whether authored data supplies the identity.
    pub authored: bool,
}

/// No-follow path facts from authored data or an exact source observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LazyLookup {
    /// Authenticated authored binding.
    Authored {
        /// Exact binding path.
        path: String,
        /// Authenticated immutable file record.
        record: FileRecord,
        /// Authenticated scalar metadata.
        metadata: crate::WorkspaceMetadata,
    },
    /// Latest authored data shared by unresolved aliases.
    Shadow {
        /// Immutable file record.
        record: FileRecord,
        /// Authenticated scalar metadata.
        metadata: crate::WorkspaceMetadata,
        /// Source-native alias count.
        source_link_count: u64,
        /// Original source cut shared by unresolved native aliases.
        source: SourceReference,
        /// Source version and identity whose authored record supplies bytes.
        node: SourceNode,
    },
    /// Exact source fact including its pinned epoch.
    Source {
        /// Observed source cut.
        source: SourceReference,
        /// Immutable node/version.
        node: SourceNode,
    },
}

impl LazyLookup {
    /// Scalar classification.
    pub fn stat(&self) -> LazyStat {
        let (kind, logical_bytes, authored) = match self {
            Self::Authored { record, .. } | Self::Shadow { record, .. } => (
                source_kind(record.kind),
                record_logical_bytes(*record),
                true,
            ),
            Self::Source { node, .. } => (node.kind, node.logical_bytes, false),
        };
        LazyStat {
            kind,
            logical_bytes,
            authored,
        }
    }
    /// Scalar metadata without following symbolic links.
    pub fn metadata(&self) -> crate::WorkspaceMetadata {
        match self {
            Self::Authored { metadata, .. } | Self::Shadow { metadata, .. } => *metadata,
            Self::Source { node, .. } => {
                crate::WorkspaceMetadata::from_engine(source_file_metadata(node.metadata))
            }
        }
    }
}

impl<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
> LazyWorkspace<A, O, D, S>
{
    /// Resolves a path and retains its first source observation.
    pub async fn lookup(
        &self,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(LazyLookup, KernelOwnedAllocation), LazyWorkspaceError> {
        let lookup = self.inspect(path, ctx).await?;
        if let Err(error) = self.retain_observation(path, &lookup.0, ctx).await {
            self.retire_lookup(lookup, ctx)?;
            return Err(error);
        }
        Ok(lookup)
    }

    pub(super) async fn retain_observation(
        &self,
        path: &str,
        lookup: &LazyLookup,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        if matches!(lookup, LazyLookup::Authored { .. }) {
            return Ok(());
        }
        let state = self.state(ctx).await?;
        // The caller already owns its original read. Retaining that knowledge
        // must not replace a newer source cut or resurrect a removed path.
        if matches!(lookup, LazyLookup::Source { source, .. } | LazyLookup::Shadow { source, .. }
            if *source != state.source)
            || self.tombstoned(state.overlay, path, ctx).await?
        {
            return Ok(());
        }
        let overlay = self
            .observed_overlay(state.overlay, path, lookup, ctx)
            .await?;
        if overlay != state.overlay {
            self.publish_overlay(state, overlay, ctx).await?;
        }
        Ok(())
    }

    pub(super) async fn observed_overlay(
        &self,
        overlay: LazyOverlayId,
        path: &str,
        lookup: &LazyLookup,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<LazyOverlayId, LazyWorkspaceError> {
        if let LazyLookup::Source { source, node } | LazyLookup::Shadow { source, node, .. } =
            lookup
        {
            let previous = self.overlay_fact(overlay, path, ctx).await?;
            let complete = matches!(&previous, Some(LazyOverlayChange::Observe {
                source: saved_source, node: saved_node, complete: true,
            }) if saved_source == source && saved_node == node);
            let change = LazyOverlayChange::Observe {
                source: source.clone(),
                node: *node,
                complete,
            };
            if previous != Some(change.clone()) {
                return self.insert_overlay(overlay, path, change, ctx).await;
            }
        }
        Ok(overlay)
    }

    /// Inspects transiently without changing source observations.
    pub async fn inspect(
        &self,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(LazyLookup, KernelOwnedAllocation), LazyWorkspaceError> {
        let mut checkout = self
            .checkout(crate::model::GenerationSelector::Head, ctx)
            .await?;
        let result = self
            .inspect_unauthored(&mut checkout.value, path, InspectBasis::default(), ctx)
            .await;
        checkout.retire(ctx)?;
        let (lookup, _, guard) = result?;
        Ok((lookup.ok_or(LazyWorkspaceError::NotFound)?, guard))
    }

    /// Resolves a frozen cut, returning known absence and actual source participation.
    pub async fn lookup_at_snapshot(
        &self,
        cut: &LazySnapshotRef,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Option<LazyLookup>, bool, KernelOwnedAllocation), LazyWorkspaceError> {
        self.validate_snapshot(&cut, ctx).await?;
        let mut checkout = self
            .checkout(
                crate::model::GenerationSelector::Exact(cut.authored_generation),
                ctx,
            )
            .await?;
        let state = LazyWorkspaceState::new(
            cut.workspace_id,
            cut.source.clone(),
            cut.overlay,
            cut.shadows,
        );
        let result = self
            .inspect_unauthored(
                &mut checkout.value,
                path,
                InspectBasis {
                    state: Some(&state),
                    retain_pinned_observation: true,
                    ..InspectBasis::default()
                },
                ctx,
            )
            .await;
        checkout.retire(ctx)?;
        result
    }

    /// Classifies a demanding lookup.
    pub async fn stat(
        &self,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<LazyStat, LazyWorkspaceError> {
        let lookup = self.lookup(path, ctx).await?;
        let stat = lookup.0.stat();
        self.retire_lookup(lookup, ctx)?;
        Ok(stat)
    }

    /// Classifies a path at a frozen cut, preserving authenticated absence.
    pub async fn stat_at_snapshot(
        &self,
        cut: &LazySnapshotRef,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<LazyStat>, LazyWorkspaceError> {
        let (lookup, _, guard) = self.lookup_at_snapshot(&cut, path, ctx).await?;
        let result = Ok(lookup.as_ref().map(LazyLookup::stat));
        match lookup {
            Some(lookup) => self.retire_lookup((lookup, guard), ctx)?,
            None => crate::kernel::retire_allocation(guard, 0, ctx)?,
        }
        result
    }

    /// Complete no-follow metadata at the same immutable authored/source cut
    /// as a range read. Resolving metadata never materializes the file body.
    pub async fn workspace_stat_at_snapshot(
        &self,
        cut: &LazySnapshotRef,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<crate::WorkspaceStat>, LazyWorkspaceError> {
        let (lookup, _, guard) = self.lookup_at_snapshot(cut, path, ctx).await?;
        let Some(lookup) = lookup else {
            crate::kernel::retire_allocation(guard, 0, ctx)?;
            return Ok(None);
        };
        let result = async {
            let stat = lookup.stat();
            let kind = match stat.kind {
                SourceNodeKind::RegularFile => FileKind::Regular,
                SourceNodeKind::Directory => FileKind::Directory,
                SourceNodeKind::SymbolicLink => FileKind::SymbolicLink,
                SourceNodeKind::Fifo => FileKind::Fifo,
                SourceNodeKind::Socket => FileKind::Socket,
                SourceNodeKind::CharacterDevice => FileKind::CharacterDevice,
                SourceNodeKind::BlockDevice => FileKind::BlockDevice,
                SourceNodeKind::Unsupported => return Err(LazyWorkspaceError::UnsupportedNode),
            };
            let link_count = match &lookup {
                LazyLookup::Authored { record, .. } => record.link_count,
                LazyLookup::Shadow {
                    source_link_count, ..
                } => *source_link_count,
                LazyLookup::Source { node, .. } => node.link_count.unwrap_or(1),
            };
            Ok(Some(crate::WorkspaceStat {
                file_id: self
                    .file_id_at_overlay(path, &lookup, Some(cut.overlay), ctx)
                    .await?,
                kind,
                link_count,
                logical_bytes: stat.logical_bytes,
                metadata: lookup.metadata(),
            }))
        }
        .await;
        self.retire_lookup((lookup, guard), ctx)?;
        result
    }

    pub(crate) async fn inspect_unauthored(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        basis: InspectBasis<'_>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Option<LazyLookup>, bool, KernelOwnedAllocation), LazyWorkspaceError> {
        PortablePath::validate(path, self.workspace.limits())
            .map_err(crate::kernel::NamespacePathError::Path)?;
        if !basis.authored_absent {
            let namespace = self.namespace_path(path, ctx)?;
            let result = checkout
                .lookup_no_follow_with_metadata_with_accounting(
                    &namespace.value,
                    ctx,
                    cancellation(ctx),
                )
                .await;
            namespace.retire(ctx)?;
            let authored = match result {
                // An authored non-directory hides every source descendant.
                // This optional lookup proves absence; it must not fall back
                // to the source behind that authored binding.
                Err(crate::FsError::Path(crate::kernel::PathLookupError::NotDirectory)) => {
                    return Ok((None, false, ctx.claim_owned(0, 0)?));
                }
                result => result?,
            };
            if let Some(authored) = authored {
                let path = self.canonical_path(path, ctx)?;
                return Ok((
                    Some(LazyLookup::Authored {
                        path: path.value,
                        record: authored.record,
                        metadata: crate::WorkspaceMetadata::from_engine(authored.metadata),
                    }),
                    false,
                    path.allocation,
                ));
            }
        }
        let loaded;
        let state = if let Some(state) = basis.state {
            state
        } else {
            loaded = self.state(ctx).await?;
            &loaded
        };
        if self.tombstoned(state.overlay, path, ctx).await?
            || self
                .saved_source_subtree(state.overlay, &state.source, path, ctx)
                .await?
        {
            return Ok((None, false, ctx.claim_owned(0, 0)?));
        }
        let fact = if basis.unobserved {
            None
        } else {
            self.overlay_fact(state.overlay, path, ctx).await?
        };
        let (source, node, used) = match fact {
            Some(LazyOverlayChange::Observe { source, node, .. })
                if basis.retain_pinned_observation || source == state.source =>
            {
                (source, Some(node), false)
            }
            _ => {
                if let Some(node) = basis.listed {
                    (state.source.clone(), Some(node), false)
                } else {
                    (
                        state.source.clone(),
                        self.source_lookup(&state.source, path, ctx).await?,
                        true,
                    )
                }
            }
        };
        let Some(node) = node else {
            return Ok((None, used, ctx.claim_owned(0, 0)?));
        };
        // A fresh native link count can fall to one after another alias was
        // unlinked. Its stable identity still belongs to authored data/shadows.
        if node.kind != SourceNodeKind::Directory
            && (basis.unobserved || node.link_count != Some(1) || basis.retain_pinned_observation)
        {
            let id = self.source_file_id(&node.file_identity, ctx)?;
            let single_source = !basis.unobserved && node.link_count == Some(1);
            let shadow = if single_source {
                self.shadow_records(state.shadows, id, ctx)
                    .await?
                    .and_then(|records| records.at_source(&source, node.version))
            } else {
                let record = accounted!(ctx, |work, cancel| checkout
                    .read_file_record_by_id(id, work, cancel));
                let shadow = match record {
                    Ok(record) => Some((
                        record,
                        crate::WorkspaceMetadata::from_engine(
                            checkout
                                .read_metadata_with_accounting(
                                    record.file_id,
                                    ctx,
                                    cancellation(ctx),
                                )
                                .await?,
                        ),
                    )),
                    Err(LazyWorkspaceError::Filesystem(crate::FsError::NotFound)) => self
                        .shadow_records(state.shadows, id, ctx)
                        .await?
                        .and_then(|records| {
                            records.authored.or_else(|| {
                                if basis.retain_pinned_observation {
                                    records.at_source(&source, node.version)
                                } else {
                                    None
                                }
                            })
                        }),
                    Err(error) => return Err(error),
                };
                shadow
            };
            if let Some((record, metadata)) = shadow {
                return Ok((
                    Some(LazyLookup::Shadow {
                        record,
                        metadata,
                        source_link_count: node.link_count.unwrap_or(1),
                        source,
                        node,
                    }),
                    used,
                    ctx.claim_owned(0, 0)?,
                ));
            }
        }
        Ok((
            Some(LazyLookup::Source { source, node }),
            used,
            ctx.claim_owned(0, 0)?,
        ))
    }

    /// Stable SDK identity shared by all aliases of the source node.
    pub fn source_file_id(
        &self,
        identity: &[u8; 32],
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<FileId, LazyWorkspaceError> {
        Ok(crate::demand::source_file_id(
            &self.source.identity(),
            identity,
            ctx,
        )?)
    }

    /// Preserves a shallowly promoted directory's original source identity.
    pub async fn stable_file_id_for_lookup(
        &self,
        path: &str,
        lookup: &LazyLookup,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<FileId, LazyWorkspaceError> {
        let overlay = if matches!(lookup, LazyLookup::Authored { record, .. } if record.kind == FileKind::Directory)
        {
            Some(self.state(ctx).await?.overlay)
        } else {
            None
        };
        self.file_id_at_overlay(path, lookup, overlay, ctx).await
    }

    async fn file_id_at_overlay(
        &self,
        path: &str,
        lookup: &LazyLookup,
        overlay: Option<LazyOverlayId>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<FileId, LazyWorkspaceError> {
        match lookup {
            LazyLookup::Source { source, node } => Ok(crate::demand::source_file_id(
                &source.identity,
                &node.file_identity,
                ctx,
            )?),
            LazyLookup::Shadow { record, .. } => Ok(record.file_id),
            LazyLookup::Authored { record, .. } => {
                if record.kind == FileKind::Directory
                    && let Some(overlay) = overlay
                    && let Some(LazyOverlayChange::Observe { source, node, .. }) =
                        self.overlay_fact(overlay, path, ctx).await?
                    && node.kind == SourceNodeKind::Directory
                {
                    return Ok(crate::demand::source_file_id(
                        &source.identity,
                        &node.file_identity,
                        ctx,
                    )?);
                }
                Ok(record.file_id)
            }
        }
    }

    /// Releases an inspected lookup and its accounted ownership.
    pub fn retire_lookup(
        &self,
        lookup: (LazyLookup, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let bytes = match &lookup.0 {
            LazyLookup::Authored { path, .. } => path.capacity() as u64,
            _ => 0,
        };
        drop(lookup.0);
        Ok(crate::kernel::retire_allocation(lookup.1, bytes, ctx)?)
    }
}
