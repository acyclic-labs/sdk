use super::*;

/// Facts an inspection's caller already established, so it need not repeat them.
#[derive(Clone, Copy, Default)]
pub(super) struct InspectBasis<'a> {
    /// State the caller read; otherwise the current state is loaded.
    pub(super) state: Option<&'a LazyWorkspaceState>,
    /// The caller's authored view already lacks the path.
    pub(super) authored_absent: bool,
    /// Node a listing observed for the path after excluding tombstones.
    pub(super) listed: Option<SourceNode>,
    /// Keep an observation pinned to an earlier source epoch.
    pub(super) retain_pinned_observation: bool,
    /// The overlay records no fact for the path, as its listing proved.
    pub(super) unobserved: bool,
}

/// Path facts returned without forcing unrelated source state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LazyStat {
    /// Observed kind.
    pub kind: SourceNodeKind,
    /// Observed logical bytes when meaningful.
    pub logical_bytes: Option<u64>,
    /// Whether the authored checkout overrides the source.
    pub authored: bool,
}

/// Complete facts returned from either authored state or a source observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LazyLookup {
    /// Authenticated authored state overrides the source.
    Authored {
        /// Existing authored binding used to serve this path. This may differ
        /// from the requested source alias until that alias is promoted.
        path: String,
        /// Authenticated authored facts for the shared identity.
        stat: WorkspaceStat,
    },
    /// Latest authored state retained after the final promoted binding was removed.
    Shadow {
        /// Path-independent immutable file record.
        record: FileRecord,
        /// Projected scalar metadata retained with the record.
        metadata: crate::WorkspaceMetadata,
        /// Source-native alias count evidence.
        source_link_count: u64,
    },
    /// Immutable first-observation source state.
    Source(SourceNode),
}

pub(super) struct ResolvedLazyLookup {
    pub(super) lookup: LazyLookup,
    pub(super) source: Option<SourceReference>,
}

pub(super) struct PinnedSourceNode {
    pub(super) source: SourceReference,
    pub(super) node: SourceNode,
}


impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{


    /// Observes one path without reading file content.
    pub async fn stat(&self, path: &str) -> Result<LazyStat, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.stat_measured(path, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value)
    }

    async fn stat_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<LazyStat>, LazyWorkspaceError> {
        let receipt = self.lookup_measured(path, budget, cancellation).await?;
        let value = match receipt.value.lookup {
            LazyLookup::Authored { stat, .. } => LazyStat {
                kind: source_kind(stat.kind),
                logical_bytes: stat.logical_bytes,
                authored: true,
            },
            LazyLookup::Shadow { record, .. } => LazyStat {
                kind: source_kind(record.kind),
                logical_bytes: record_logical_bytes(record),
                authored: true,
            },
            LazyLookup::Source(node) => LazyStat {
                kind: node.kind,
                logical_bytes: node.logical_bytes,
                authored: false,
            },
        };
        Ok(OperationReceipt {
            value,
            work: receipt.work,
        })
    }

    /// Returns complete demanded facts without reading regular-file content.
    pub async fn lookup(&self, path: &str) -> Result<LazyLookup, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.lookup_measured(path, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value.lookup)
    }

    async fn lookup_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        in_heap(move || async move {
            let authored = self
                .workspace
                .stat_optional_measured(path, budget, cancellation)
                .await
                .map_err(workspace_error)?;
            let mut work = authored.work;
            if let Some(stat) = authored.value {
                return Ok(OperationReceipt {
                    value: ResolvedLazyLookup {
                        lookup: LazyLookup::Authored {
                            path: self.canonical_path(path)?,
                            stat,
                        },
                        source: None,
                    },
                    work,
                });
            }
            let state_receipt = self
                .state_measured(remaining_work(work, budget)?, cancellation)
                .await?;
            work = account_work(work, state_receipt.work, budget)?;
            let state = state_receipt.value;
            let tombstoned = self
                .tombstoned_measured(
                    state.overlay,
                    path,
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await?;
            work = account_work(work, tombstoned.work, budget)?;
            if tombstoned.value {
                return Err(LazyWorkspaceError::NotFound);
            }
            let observed = self.observe_measured(path, state, cancellation).await?;
            work = account_work(work, observed.work, budget)?;
            let (source, node) = observed.value;
            let authored = self
                .authored_alias_measured(&node, remaining_work(work, budget)?, cancellation)
                .await?;
            work = account_work(work, authored.work, budget)?;
            if let Some(authored) = authored.value {
                return Ok(OperationReceipt {
                    value: ResolvedLazyLookup {
                        lookup: authored,
                        source: None,
                    },
                    work,
                });
            }
            Ok(OperationReceipt {
                value: ResolvedLazyLookup {
                    lookup: LazyLookup::Source(node),
                    source: Some(source),
                },
                work,
            })
        })
        .await
    }

    /// Returns current facts without extending the immutable observation index.
    ///
    /// Directory projection uses this after receiving a source page so that
    /// gathering entry metadata cannot invalidate the page's continuation.
    pub async fn inspect(&self, path: &str) -> Result<LazyLookup, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.inspect_measured(path, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value.lookup)
    }

    async fn inspect_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        self.inspect_with(path, InspectBasis::default(), budget, cancellation)
            .await
    }

    async fn inspect_snapshot_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        let basis = InspectBasis {
            retain_pinned_observation: true,
            ..InspectBasis::default()
        };
        self.inspect_with(path, basis, budget, cancellation).await
    }

    async fn inspect_snapshot_after_authored_absence(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        let basis = InspectBasis {
            retain_pinned_observation: true,
            authored_absent: true,
            ..InspectBasis::default()
        };
        self.inspect_with(path, basis, budget, cancellation).await
    }

    /// Resolves `path` for a mount whose own checkout already lacks it.
    ///
    /// The mount's checkout, not the published head, is its authored view, so
    /// the head is not consulted again. `state` pins the snapshot a directory
    /// page was listed against; otherwise the current state is read.
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) async fn inspect_unauthored(
        &self,
        path: &str,
        state: Option<&LazyWorkspaceState>,
    ) -> Result<(LazyLookup, Option<SourceReference>), LazyWorkspaceError> {
        in_heap(move || async move {
            let basis = InspectBasis {
                state,
                authored_absent: true,
                ..InspectBasis::default()
            };
            self.inspect_with(
                path,
                basis,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map(|receipt| (receipt.value.lookup, receipt.value.source))
        })
        .await
    }

    /// The source view lookups currently resolve in.
    #[cfg(feature = "native-mount")]
    pub(crate) fn source_reference(&self) -> SourceReference {
        self.source.reference()
    }

    /// What the source itself names at `path` in the view `source`, read
    /// afresh and bypassing the overlay.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn source_lookup(
        &self,
        source: SourceReference,
        path: &str,
    ) -> Result<Option<SourceNode>, LazyWorkspaceError> {
        self.source
            .lookup(
                source,
                &self.namespace_path(path)?,
                &CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| LazyWorkspaceError::from(failure.error))
    }

    /// Resolves a source entry of a page listed against `state`, reusing the
    /// node the listing observed. The listing already excluded tombstoned and
    /// authored names, so neither the source nor the tombstones are consulted.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn inspect_listed(
        &self,
        state: &LazyWorkspaceState,
        path: &str,
        node: SourceNode,
        unobserved: bool,
    ) -> Result<(LazyLookup, Option<SourceReference>), LazyWorkspaceError> {
        in_heap(move || async move {
            let basis = InspectBasis {
                state: Some(state),
                authored_absent: true,
                listed: Some(node),
                unobserved,
                ..InspectBasis::default()
            };
            self.inspect_with(
                path,
                basis,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map(|receipt| (receipt.value.lookup, receipt.value.source))
        })
        .await
    }

    /// The source node `path` names in `state`: a pinned observation, the node
    /// a listing supplied, or a fresh source lookup, never a tombstoned path.
    async fn source_node_in(
        &self,
        state: &LazyWorkspaceState,
        path: &str,
        basis: InspectBasis<'_>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<(SourceReference, SourceNode)>, LazyWorkspaceError> {
        let fact = if basis.unobserved && basis.listed.is_some() {
            OperationReceipt {
                value: None,
                work: WorkCounters::default(),
            }
        } else {
            self.overlay_fact_measured(state.overlay, path, budget, cancellation)
                .await?
        };
        let mut work = fact.work;
        let observed = match fact.value {
            Some(LazyOverlayChange::Tombstone { .. }) => return Err(LazyWorkspaceError::NotFound),
            Some(LazyOverlayChange::Observe { source, node })
                if basis.retain_pinned_observation || source == state.source =>
            {
                Some((source, node))
            }
            Some(LazyOverlayChange::Observe { .. }) | None => None,
        };
        let (source, node) = if let Some(observed) = observed {
            observed
        } else if let Some(node) = basis.listed {
            (state.source, node)
        } else {
            let receipt = self
                .source
                .lookup(state.source, &self.namespace_path(path)?, cancellation)
                .await
                .map_err(|failure| LazyWorkspaceError::from(failure.error))?;
            work = account_work(work, receipt.work, budget)?;
            (
                state.source,
                receipt.value.ok_or(LazyWorkspaceError::NotFound)?,
            )
        };
        if basis.listed.is_none() {
            let tombstoned = self
                .tombstoned_measured(
                    state.overlay,
                    path,
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await?;
            work = account_work(work, tombstoned.work, budget)?;
            if tombstoned.value {
                return Err(LazyWorkspaceError::NotFound);
            }
        }
        Ok(OperationReceipt {
            value: (source, node),
            work,
        })
    }

    async fn inspect_with(
        &self,
        path: &str,
        basis: InspectBasis<'_>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<ResolvedLazyLookup>, LazyWorkspaceError> {
        let authored = if basis.authored_absent {
            OperationReceipt {
                value: None,
                work: WorkCounters::default(),
            }
        } else {
            self.workspace
                .stat_optional_measured(path, budget, cancellation)
                .await
                .map_err(workspace_error)?
        };
        let mut work = authored.work;
        if let Some(stat) = authored.value {
            return Ok(OperationReceipt {
                value: ResolvedLazyLookup {
                    lookup: LazyLookup::Authored {
                        path: self.canonical_path(path)?,
                        stat,
                    },
                    source: None,
                },
                work,
            });
        }
        let state = if let Some(state) = basis.state {
            state.clone()
        } else {
            let receipt = self
                .state_measured(remaining_work(work, budget)?, cancellation)
                .await?;
            work = account_work(work, receipt.work, budget)?;
            receipt.value
        };
        let resolved = self
            .source_node_in(
                &state,
                path,
                basis,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await?;
        work = account_work(work, resolved.work, budget)?;
        let (source, node) = resolved.value;
        let authored = self
            .authored_alias_measured(&node, remaining_work(work, budget)?, cancellation)
            .await?;
        work = account_work(work, authored.work, budget)?;
        if let Some(authored) = authored.value {
            return Ok(OperationReceipt {
                value: ResolvedLazyLookup {
                    lookup: authored,
                    source: None,
                },
                work,
            });
        }
        Ok(OperationReceipt {
            value: ResolvedLazyLookup {
                lookup: LazyLookup::Source(node),
                source: Some(source),
            },
            work,
        })
    }

    /// Returns a stable SDK identity for an unresolved source object.
    ///
    /// Aliases carrying the same source-native identity map to one `FileId`,
    /// independent of path, so mount lookups preserve hard-link topology.
    pub fn source_file_id(&self, node: &SourceNode) -> FileId {
        self.source_file_id_of(&node.file_identity)
    }

    /// The identity of the source node whose [`SourceNode::file_identity`]
    /// is `file_identity`.
    pub(crate) fn source_file_id_of(&self, file_identity: &[u8; 32]) -> FileId {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"acyclic-fs-lazy-source-file-id-v1\0");
        hasher.update(&self.source.reference().identity);
        hasher.update(file_identity);
        let digest = hasher.finalize();
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        FileId::from_bytes(bytes)
    }

    /// Starts the source reporting its changes to `sink`; `None` when it
    /// cannot (see [`DemandSource::watch`]).
    #[cfg(feature = "native-mount")]
    pub(crate) fn watch_source(
        &self,
        sink: Arc<dyn crate::demand::SourceChangeSink>,
    ) -> Result<Option<Box<dyn crate::demand::SourceWatch>>, LazyWorkspaceError> {
        self.source.watch(sink).map_err(LazyWorkspaceError::from)
    }

    /// Direct identity of the resolved fact. Mounted directory identity is a
    /// separate projection rule in `stable_file_id_for_lookup`.
    fn direct_file_id_for_lookup(&self, lookup: &LazyLookup) -> FileId {
        match lookup {
            LazyLookup::Source(node) => self.source_file_id(node),
            LazyLookup::Authored { stat, .. } => stat.file_id,
            LazyLookup::Shadow { record, .. } => record.file_id,
        }
    }

    #[cfg(any(
        feature = "native-mount",
        all(test, feature = "native-watch", not(target_arch = "wasm32"))
    ))]
    pub(crate) async fn stable_file_id_for_lookup(
        &self,
        path: &str,
        lookup: &LazyLookup,
    ) -> Result<FileId, LazyWorkspaceError> {
        match lookup {
            LazyLookup::Source(node) => Ok(self.source_file_id(node)),
            LazyLookup::Shadow { record, .. } => Ok(record.file_id),
            LazyLookup::Authored { stat, .. } if stat.kind == FileKind::Directory => {
                let state = self.state().await?;
                match self.overlay_fact(state.overlay, path).await? {
                    Some(LazyOverlayChange::Observe { node, .. })
                        if node.kind == SourceNodeKind::Directory =>
                    {
                        Ok(self.source_file_id(&node))
                    }
                    Some(LazyOverlayChange::Observe { .. })
                    | Some(LazyOverlayChange::Tombstone { .. })
                    | None => Ok(stat.file_id),
                }
            }
            LazyLookup::Authored { stat, .. } => Ok(stat.file_id),
        }
    }

    async fn authored_alias_measured(
        &self,
        node: &SourceNode,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Option<LazyLookup>>, LazyWorkspaceError> {
        in_heap(move || async move {
            // Only source objects with multiple directory bindings can resolve
            // through an authored alias. Avoid the generation-wide reverse-path
            // query for ordinary files and directories; on large compiler trees
            // that otherwise creates one derived path-index entry per stat.
            if node.kind == SourceNodeKind::Directory || node.link_count == Some(1) {
                return Ok(OperationReceipt {
                    value: None,
                    work: WorkCounters::default(),
                });
            }
            let file_id = self.source_file_id(node);
            let state = self.state_measured(budget, cancellation).await?;
            // A source that does not count links cannot rule an alias out, but
            // an identity the head's file table does not hold has no name there.
            let head = self
                .workspace
                .head_with_file_record_measured(
                    file_id,
                    remaining_work(state.work, budget)?,
                    cancellation,
                )
                .await
                .map_err(workspace_error)?;
            let mut work = account_work(state.work, head.work, budget)?;
            let state = state.value;
            let (generation, held) = head.value;
            let path = if held {
                let paths = generation
                    .paths_for_file_id_measured(
                        file_id,
                        u32::MAX,
                        remaining_work(work, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(workspace_error)?;
                work = account_work(work, paths.work, budget)?;
                paths.value.into_iter().next()
            } else {
                None
            };
            if let Some(path) = path {
                let stat = self
                    .workspace
                    .stat_optional_measured(&path, remaining_work(work, budget)?, cancellation)
                    .await
                    .map_err(workspace_error)?;
                work = account_work(work, stat.work, budget)?;
                let mut stat = stat.value.ok_or(LazyWorkspaceError::Concurrent)?;
                if let Some(source_links) = node.link_count {
                    stat.link_count = stat.link_count.max(source_links);
                }
                return Ok(OperationReceipt {
                    value: Some(LazyLookup::Authored { path, stat }),
                    work,
                });
            }
            let shadow = self
                .shadow_record_measured(
                    state.shadows,
                    file_id,
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await?;
            work = account_work(work, shadow.work, budget)?;
            let Some((record, metadata)) = shadow.value else {
                return Ok(OperationReceipt { value: None, work });
            };
            Ok(OperationReceipt {
                value: Some(LazyLookup::Shadow {
                    record,
                    metadata,
                    source_link_count: node.link_count.unwrap_or(1),
                }),
                work,
            })
        })
        .await
    }
}
