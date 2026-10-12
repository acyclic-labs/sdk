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
        /// Authenticated scalar facts.
        stat: WorkspaceStat,
    },
    /// Latest authored data shared by unresolved aliases.
    Shadow {
        /// Immutable file record.
        record: FileRecord,
        /// Authenticated scalar metadata.
        metadata: crate::WorkspaceMetadata,
        /// Source-native alias count.
        source_link_count: u64,
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
    /// Retained heap backing.
    pub fn logical_owned_bytes(&self) -> u64 {
        match self {
            Self::Authored { path, .. } => path.capacity() as u64,
            _ => 0,
        }
    }
    /// Scalar classification.
    pub fn stat(&self) -> LazyStat {
        let (kind, logical_bytes, authored) = match self {
            Self::Authored { stat, .. } => (source_kind(stat.kind), stat.logical_bytes, true),
            Self::Shadow { record, .. } => (
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
            Self::Authored { stat, .. } => stat.metadata,
            Self::Shadow { metadata, .. } => *metadata,
            Self::Source { node, .. } => {
                crate::WorkspaceMetadata::from_engine(source_file_metadata(node.metadata))
            }
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Resolves a path and retains its first source observation.
    pub async fn lookup(
        &self,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(LazyLookup, KernelOwnedAllocation), LazyWorkspaceError> {
        let lookup = self.inspect(path, ctx).await?;
        if let LazyLookup::Source { source, node } = lookup.0 {
            let change = LazyOverlayChange::Observe { source, node };
            let recorded = async {
                let state = self.state(ctx).await?;
                if self.overlay_fact(state.overlay, path, ctx).await? != Some(change.clone()) {
                    self.append_overlay(path, change, ctx).await?;
                }
                Ok::<_, LazyWorkspaceError>(())
            }
            .await;
            if let Err(error) = recorded {
                self.retire_lookup(lookup, ctx)?;
                return Err(error);
            }
        }
        Ok(lookup)
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
            .inspect_unauthored(
                &mut checkout.value,
                path,
                None,
                InspectBasis::default(),
                ctx,
            )
            .await;
        checkout.retire(ctx)?;
        let (lookup, _, guard) = result?;
        Ok((lookup.ok_or(LazyWorkspaceError::NotFound)?, guard))
    }

    /// Resolves a frozen cut, returning known absence and actual source participation.
    pub async fn lookup_at_snapshot(
        &self,
        cut: LazySnapshotRef,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Option<LazyLookup>, bool, KernelOwnedAllocation), LazyWorkspaceError> {
        self.validate_snapshot(cut, ctx)?;
        let mut checkout = self
            .checkout(
                crate::model::GenerationSelector::Exact(cut.authored_generation),
                ctx,
            )
            .await?;
        let state = LazyWorkspaceState::new(cut.workspace_id, cut.source, cut.overlay, cut.shadows);
        let result = self
            .inspect_unauthored(
                &mut checkout.value,
                path,
                Some(cut),
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

    pub(crate) async fn inspect_unauthored(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        frozen: Option<LazySnapshotRef>,
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
            if let Some(authored) = result? {
                let path = self.canonical_path(path, ctx)?;
                return Ok((
                    Some(LazyLookup::Authored {
                        path: path.value,
                        stat: authored_stat(authored.record, authored.metadata),
                    }),
                    false,
                    path.allocation,
                ));
            }
        }
        let loaded;
        let state = match basis.state {
            Some(state) => state,
            None => {
                loaded = self.state(ctx).await?;
                &loaded
            }
        };
        if self.tombstoned(state.overlay, path, ctx).await? {
            return Ok((None, false, ctx.claim_owned(0, 0)?));
        }
        let fact = if basis.unobserved {
            None
        } else {
            self.overlay_fact(state.overlay, path, ctx).await?
        };
        let (source, node, used) = match fact {
            Some(LazyOverlayChange::Observe { source, node })
                if basis.retain_pinned_observation || source == state.source =>
            {
                (source, Some(node), false)
            }
            _ => match basis.listed {
                Some(node) => (state.source, Some(node), false),
                None => {
                    if frozen.is_some() && state.source != self.source.reference() {
                        return Err(LazyWorkspaceError::StaleSource);
                    }
                    (
                        state.source,
                        self.source_lookup(state.source, path, ctx).await?,
                        true,
                    )
                }
            },
        };
        let Some(node) = node else {
            return Ok((None, used, ctx.claim_owned(0, 0)?));
        };
        if node.kind != SourceNodeKind::Directory && node.link_count != Some(1) {
            let id = self.source_file_id(&node.file_identity, ctx)?;
            let record = accounted!(ctx, |work, cancel| checkout
                .read_file_record_by_id(id, work, cancel));
            let shadow = match record {
                Ok(record) => Some((
                    record,
                    crate::WorkspaceMetadata::from_engine(
                        checkout
                            .read_metadata_by_id_with_accounting(
                                record.file_id,
                                ctx,
                                cancellation(ctx),
                            )
                            .await?,
                    ),
                )),
                Err(LazyWorkspaceError::Filesystem(crate::FsError::NotFound)) => {
                    self.shadow_record(state.shadows, id, ctx).await?
                }
                Err(error) => return Err(error),
            };
            if let Some((record, metadata)) = shadow {
                return Ok((
                    Some(LazyLookup::Shadow {
                        record,
                        metadata,
                        source_link_count: node.link_count.unwrap_or(1),
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
        let domain = b"acyclic-fs-lazy-source-file-id-v1\0";
        ctx.charge(WorkCounters {
            bytes_hashed: domain.len() as u64 + 48,
            ..WorkCounters::UNCHARGED
        })?;
        let mut hash = blake3::Hasher::new();
        hash.update(domain);
        hash.update(&self.source.reference().identity);
        hash.update(identity);
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        Ok(FileId::from_bytes(bytes))
    }

    /// Preserves a shallowly promoted directory's original source identity.
    pub async fn stable_file_id_for_lookup(
        &self,
        path: &str,
        lookup: &LazyLookup,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<FileId, LazyWorkspaceError> {
        match lookup {
            LazyLookup::Source { node, .. } => self.source_file_id(&node.file_identity, ctx),
            LazyLookup::Shadow { record, .. } => Ok(record.file_id),
            LazyLookup::Authored { stat, .. } => {
                if stat.kind == FileKind::Directory {
                    let state = self.state(ctx).await?;
                    if let Some(LazyOverlayChange::Observe { node, .. }) =
                        self.overlay_fact(state.overlay, path, ctx).await?
                        && node.kind == SourceNodeKind::Directory
                    {
                        return self.source_file_id(&node.file_identity, ctx);
                    }
                }
                Ok(stat.file_id)
            }
        }
    }

    pub(crate) fn retire_lookup(
        &self,
        lookup: (LazyLookup, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let bytes = lookup.0.logical_owned_bytes();
        drop(lookup.0);
        Ok(crate::kernel::retire_allocation(lookup.1, bytes, ctx)?)
    }
}

pub(super) fn authored_stat(record: FileRecord, metadata: FileMetadata) -> WorkspaceStat {
    WorkspaceStat {
        file_id: record.file_id,
        kind: record.kind,
        link_count: record.link_count,
        logical_bytes: record_logical_bytes(record),
        metadata: crate::WorkspaceMetadata::from_engine(metadata),
    }
}
