use super::*;
use crate::kernel::KernelAccounting;

/// Path facts without fetching content.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LazyStat {
    /// Observed kind.
    pub kind: SourceNodeKind,
    /// Logical length when known.
    pub logical_bytes: Option<u64>,
    /// Whether authenticated authored data supplies the object.
    pub authored: bool,
}

/// A no-follow authored or source fact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LazyLookup {
    /// An authored namespace binding.
    Authored {
        /// Exact binding path.
        path: String,
        /// Authenticated scalar facts.
        stat: WorkspaceStat,
    },
    /// Authored data for a source identity, including unresolved aliases.
    Shadow {
        /// Immutable authored record.
        record: FileRecord,
        /// Authenticated scalar metadata.
        metadata: crate::WorkspaceMetadata,
        /// Source alias count.
        source_link_count: u64,
    },
    /// Pinned no-follow source fact.
    Source {
        /// Exact observed source reference.
        source: SourceReference,
        /// Exact observed node/version.
        node: SourceNode,
    },
}

impl LazyLookup {
    /// Retained SDK heap backing.
    pub fn logical_owned_bytes(&self) -> u64 {
        match self {
            Self::Authored { path, .. } => path.capacity() as u64,
            _ => 0,
        }
    }
    /// Scalar classification without marking promotion as modification.
    pub fn stat(&self) -> LazyStat {
        match self {
            Self::Authored { stat, .. } => LazyStat {
                kind: source_kind(stat.kind),
                logical_bytes: stat.logical_bytes,
                authored: true,
            },
            Self::Shadow { record, .. } => LazyStat {
                kind: source_kind(record.kind),
                logical_bytes: record_logical_bytes(*record),
                authored: true,
            },
            Self::Source { node, .. } => LazyStat {
                kind: node.kind,
                logical_bytes: node.logical_bytes,
                authored: false,
            },
        }
    }
    /// Scalar cross-profile metadata.
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

#[derive(Clone, Copy, Default)]
pub(crate) struct InspectBasis<'a> {
    pub(crate) state: Option<&'a LazyWorkspaceState>,
    pub(crate) authored_absent: bool,
    pub(crate) listed: Option<SourceNode>,
    pub(crate) retain_pinned_observation: bool,
    pub(crate) unobserved: bool,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Resolves and records a first source observation.
    pub async fn lookup(
        &self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(LazyLookup, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError>
    {
        let lookup = self.inspect(path, accounting).await?;
        if let LazyLookup::Source { source, node } = lookup.0 {
            let state = self.state(accounting).await?;
            let change = LazyOverlayChange::Observe { source, node };
            if self.overlay_fact(state.overlay, path, accounting).await? != Some(change.clone()) {
                self.append_overlay(path, change, accounting).await?;
            }
        }
        Ok(lookup)
    }

    /// Inspects transiently, leaving the observation index unchanged.
    pub async fn inspect(
        &self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(LazyLookup, crate::kernel::allocation::KernelOwnedAllocation), LazyWorkspaceError>
    {
        let mut checkout = self
            .checkout(crate::model::GenerationSelector::Head, accounting)
            .await?;
        let result = self
            .inspect_unauthored(
                &mut checkout.value,
                path,
                None,
                InspectBasis::default(),
                accounting,
            )
            .await;
        checkout.retire(accounting)?;
        let (lookup, _, allocation) = result?;
        Ok((lookup.ok_or(LazyWorkspaceError::NotFound)?, allocation))
    }

    /// Resolves an immutable cut, including known absence and actual source participation.
    pub async fn lookup_at_snapshot(
        &self,
        snapshot: LazySnapshotRef,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<
        (
            Option<LazyLookup>,
            bool,
            crate::kernel::allocation::KernelOwnedAllocation,
        ),
        LazyWorkspaceError,
    > {
        self.validate_snapshot(snapshot, accounting)?;
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
        let result = self
            .inspect_unauthored(
                &mut checkout.value,
                path,
                Some(snapshot),
                InspectBasis {
                    state: Some(&state),
                    retain_pinned_observation: true,
                    ..InspectBasis::default()
                },
                accounting,
            )
            .await;
        checkout.retire(accounting)?;
        result
    }

    /// Classifies one demanding lookup.
    pub async fn stat(
        &self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<LazyStat, LazyWorkspaceError> {
        let lookup = self.lookup(path, accounting).await?;
        let stat = lookup.0.stat();
        self.retire_lookup(lookup, accounting)?;
        Ok(stat)
    }

    pub(crate) async fn inspect_unauthored(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        snapshot: Option<LazySnapshotRef>,
        basis: InspectBasis<'_>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<
        (
            Option<LazyLookup>,
            bool,
            crate::kernel::allocation::KernelOwnedAllocation,
        ),
        LazyWorkspaceError,
    > {
        PortablePath::validate(path, self.workspace.limits())
            .map_err(crate::kernel::NamespacePathError::Path)?;
        let cancellation = cancellation(accounting);
        if !basis.authored_absent {
            let namespace = self.namespace_path(path, accounting)?;
            let authored = checkout
                .lookup_no_follow_with_metadata_with_accounting(
                    &namespace.value,
                    accounting,
                    cancellation,
                )
                .await;
            namespace.retire(accounting)?;
            if let Some(authored) = authored? {
                let path = self.canonical_path(path, accounting)?;
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
        let state = if let Some(state) = basis.state {
            state
        } else {
            loaded = self.state(accounting).await?;
            &loaded
        };
        if self.tombstoned(state.overlay, path, accounting).await? {
            return Ok((None, false, accounting.claim_owned(0, 0)?));
        }
        let observed = if basis.unobserved {
            None
        } else {
            self.overlay_fact(state.overlay, path, accounting).await?
        };
        let mut used = false;
        let (source, node) = match observed {
            Some(LazyOverlayChange::Observe { source, node })
                if basis.retain_pinned_observation || source == state.source =>
            {
                (source, Some(node))
            }
            _ => {
                if let Some(node) = basis.listed {
                    (state.source, Some(node))
                } else {
                    if snapshot.is_some() && state.source != self.source.reference() {
                        return Err(LazyWorkspaceError::StaleSource);
                    }
                    used = true;
                    (
                        state.source,
                        self.source_lookup(state.source, path, accounting).await?,
                    )
                }
            }
        };
        let Some(node) = node else {
            return Ok((None, used, accounting.claim_owned(0, 0)?));
        };
        if node.kind != SourceNodeKind::Directory && node.link_count != Some(1) {
            let id = self.source_file_id(&node.file_identity, accounting)?;
            let record = 'accounted: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = accounting.native_work() {
                    break 'accounted checkout
                        .read_file_record_by_id(id, original, cancellation)
                        .await
                        .map_err(LazyWorkspaceError::from);
                }
                let receipt = checkout
                    .read_file_record_by_id(id, accounting.remaining()?, cancellation)
                    .await;
                accept_receipt(receipt, accounting)
            };
            let shadow = match record {
                Ok(record) => Some((
                    record,
                    crate::WorkspaceMetadata::from_engine(
                        checkout
                            .read_metadata_by_id_with_accounting(
                                record.file_id,
                                accounting,
                                cancellation,
                            )
                            .await?,
                    ),
                )),
                Err(LazyWorkspaceError::Filesystem(crate::FsError::NotFound)) => {
                    self.shadow_record(state.shadows, id, accounting).await?
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
                    accounting.claim_owned(0, 0)?,
                ));
            }
        }
        Ok((
            Some(LazyLookup::Source { source, node }),
            used,
            accounting.claim_owned(0, 0)?,
        ))
    }

    /// Stable SDK identity of an unresolved source node.
    pub fn source_file_id(
        &self,
        identity: &[u8; 32],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<FileId, LazyWorkspaceError> {
        let domain = b"acyclic-fs-lazy-source-file-id-v1\0";
        accounting.charge(WorkCounters {
            bytes_hashed: domain.len() as u64 + 48,
            ..WorkCounters::UNCHARGED
        })?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(domain);
        hasher.update(&self.source.reference().identity);
        hasher.update(identity);
        let digest = hasher.finalize();
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        Ok(FileId::from_bytes(bytes))
    }

    /// Resolves the original directory identity after shallow promotion.
    pub async fn stable_file_id_for_lookup(
        &self,
        path: &str,
        lookup: &LazyLookup,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<FileId, LazyWorkspaceError> {
        match lookup {
            LazyLookup::Source { node, .. } => self.source_file_id(&node.file_identity, accounting),
            LazyLookup::Shadow { record, .. } => Ok(record.file_id),
            LazyLookup::Authored { stat, .. } => {
                if stat.kind == FileKind::Directory {
                    let state = self.state(accounting).await?;
                    if let Some(LazyOverlayChange::Observe { node, .. }) =
                        self.overlay_fact(state.overlay, path, accounting).await?
                        && node.kind == SourceNodeKind::Directory
                    {
                        return self.source_file_id(&node.file_identity, accounting);
                    }
                }
                Ok(stat.file_id)
            }
        }
    }

    pub(crate) fn retire_lookup(
        &self,
        lookup: (LazyLookup, crate::kernel::allocation::KernelOwnedAllocation),
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let bytes = lookup.0.logical_owned_bytes();
        drop(lookup.0);
        crate::kernel::retire_allocation(lookup.1, bytes, accounting).map_err(Into::into)
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
