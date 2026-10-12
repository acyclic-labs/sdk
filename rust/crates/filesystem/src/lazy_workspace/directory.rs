use super::*;

/// Sparse seek target for a lazily sourced regular file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LazySeekTarget {
    /// First represented data byte.
    Data,
    /// First hole byte, including the logical end of a dense source file.
    Hole,
}

/// One merged directory entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LazyDirectoryEntry {
    /// Exact profile-aware child name.
    pub name: LogicalName,
    /// Observed or authored kind.
    pub kind: SourceNodeKind,
    /// Whether the authored checkout overrides the source.
    pub authored: bool,
    /// The source node observed while listing, for entries the source supplies.
    pub source: Option<SourceNode>,
    /// Whether the overlay records no fact for the entry, which the listing
    /// proved of its whole directory: the listed node is then its node.
    pub unobserved: bool,
}

/// Opaque bounded continuation for a merged source/authored directory scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LazyDirectoryCursor {
    pub(super) source: SourceReference,
    pub(super) overlay: LazyOverlayId,
    pub(super) directory: NamespacePath,
    pub(super) phase: LazyDirectoryPhase,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum LazyDirectoryPhase {
    Source(Option<SourceCursor>),
    Authored(Option<LogicalName>),
}

/// One bounded page from the merged lazy directory view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LazyDirectoryPage {
    /// At most the requested number of visible entries.
    pub entries: Vec<LazyDirectoryEntry>,
    /// Opaque continuation, absent after the authored phase is exhausted.
    pub next: Option<LazyDirectoryCursor>,
}


impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{


    /// Returns one bounded page combining unresolved source entries with the
    /// authored overlay. Source-native order is followed by authored order;
    /// authored entries replace same-name source entries and tombstones hide
    /// source entries without forcing unrelated subtrees.
    pub async fn list_directory(
        &self,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32,
    ) -> Result<LazyDirectoryPage, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.list_directory_measured(
            path,
            cursor,
            maximum_entries,
            WorkBudget::UNBOUNDED,
            &cancellation,
            None,
            None,
        )
        .await
        .map(|receipt| receipt.value)
    }

    /// Reads the same merged sparse directory against an unpublished mounted
    /// checkout candidate instead of the workspace's last committed head.
    #[cfg(feature = "native-mount")]
    ///
    /// Returns the state the page was listed against, so callers resolve its
    /// entries with [`Self::inspect_unauthored`] from the same snapshot.
    pub(crate) async fn list_directory_in_checkout(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32,
        mounted_mask: Option<(&[String], bool)>,
    ) -> Result<(LazyDirectoryPage, LazyWorkspaceState), LazyWorkspaceError> {
        in_heap(move || async move {
            let cancellation = CancellationToken::new();
            let state = { self.state().await? };
            let page = self
                .list_directory_at(
                    &state,
                    WorkCounters::default(),
                    path,
                    cursor,
                    maximum_entries,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                    Some(checkout),
                    mounted_mask,
                )
                .await?;
            Ok((page.value, state))
        })
        .await
    }

    /// A mounted replacement must not inherit entries from an unrelated
    /// source directory at the same path.
    #[allow(
        clippy::too_many_arguments,
        reason = "compares one mounted directory against its source with the shared checkout, work counters, budget, and cancellation"
    )]
    async fn mounted_directory_replaces_source(
        &self,
        path: &str,
        directory: &NamespacePath,
        source: SourceReference,
        overlay: LazyOverlayId,
        checkout: &mut crate::Checkout<A, O>,
        work: &mut WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<MountedDirectory, LazyWorkspaceError> {
        in_heap(move || async move {
            let candidate = checkout
                .lookup_no_follow(directory, remaining_work(*work, budget)?, cancellation)
                .await
                .map_err(|failure| LazyWorkspaceError::Workspace(failure.error.to_string()))?;
            *work = account_work(*work, candidate.work, budget)?;
            let tombstoned = self
                .tombstoned_measured(overlay, path, remaining_work(*work, budget)?, cancellation)
                .await?;
            *work = account_work(*work, tombstoned.work, budget)?;
            let replaces_source = match candidate.value.record {
                None if tombstoned.value => return Err(LazyWorkspaceError::NotFound),
                Some(_) if tombstoned.value => true,
                Some(record) => {
                    let source = self
                        .source
                        .lookup(source, directory, cancellation)
                        .await
                        .map_err(|failure| failure.error)?;
                    *work = account_work(*work, source.work, budget)?;
                    match source.value {
                        None => true,
                        Some(node) if self.source_file_id(&node) == record.file_id => false,
                        // A directory the workspace already held merges with
                        // the source's: once published, only a tombstone
                        // replaces a source directory. One the checkout made
                        // since replaces an unrelated one at its path.
                        Some(_) => {
                            let head = self
                                .workspace
                                .head_measured(remaining_work(*work, budget)?, cancellation)
                                .await
                                .map_err(workspace_error)?;
                            *work = account_work(*work, head.work, budget)?;
                            let committed = head
                                .value
                                .lookup_paths(
                                    std::slice::from_ref(directory),
                                    remaining_work(*work, budget)?,
                                    cancellation,
                                )
                                .await
                                .map_err(|failure| {
                                    LazyWorkspaceError::Workspace(failure.error.to_string())
                                })?;
                            *work = account_work(*work, committed.work, budget)?;
                            committed
                                .value
                                .first()
                                .copied()
                                .flatten()
                                .is_none_or(|committed| committed.file_id != record.file_id)
                        }
                    }
                }
                None => false,
            };
            Ok(MountedDirectory {
                replaces_source,
                authored: candidate.value.record.is_some(),
            })
        })
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "forwards the page request plus the optional mounted checkout and mask to list_directory_at"
    )]
    async fn list_directory_measured(
        &self,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        mounted: Option<&mut crate::Checkout<A, O>>,
        mounted_mask: Option<(&[String], bool)>,
    ) -> Result<OperationReceipt<LazyDirectoryPage>, LazyWorkspaceError> {
        let state = self.state_measured(budget, cancellation).await?;
        self.list_directory_at(
            &state.value,
            state.work,
            path,
            cursor,
            maximum_entries,
            budget,
            cancellation,
            mounted,
            mounted_mask,
        )
        .await
    }

    #[allow(
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "one page merges overlay, source, and mounted entries in name order under a single budget"
    )]
    async fn list_directory_at(
        &self,
        state: &LazyWorkspaceState,
        mut work: WorkCounters,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum_entries: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        mut mounted: Option<&mut crate::Checkout<A, O>>,
        mounted_mask: Option<(&[String], bool)>,
    ) -> Result<OperationReceipt<LazyDirectoryPage>, LazyWorkspaceError> {
        in_heap(move || async move {
            if maximum_entries == 0 {
                return Err(LazyWorkspaceError::InvalidPageBound);
            }
            let directory = self.namespace_path(path)?;
            let mut phase = match cursor {
                Some(cursor)
                    if cursor.source == state.source
                        && cursor.overlay == state.overlay
                        && cursor.directory == directory =>
                {
                    cursor.phase
                }
                Some(_) => return Err(LazyWorkspaceError::StaleCursor),
                None => LazyDirectoryPhase::Source(None),
            };
            if mounted_mask.is_some_and(|(_, opaque)| opaque)
                && matches!(phase, LazyDirectoryPhase::Source(None))
            {
                phase = LazyDirectoryPhase::Authored(None);
            }
            // Whether the checkout holds the directory: a child can be
            // authored only in a directory that is.
            let mut directory_authored = None;
            if matches!(phase, LazyDirectoryPhase::Source(None))
                && !directory.is_root()
                && let Some(checkout) = mounted.as_deref_mut()
            {
                let mounted_directory = self
                    .mounted_directory_replaces_source(
                        path,
                        &directory,
                        state.source,
                        state.overlay,
                        checkout,
                        &mut work,
                        budget,
                        cancellation,
                    )
                    .await?;
                directory_authored = Some(mounted_directory.authored);
                if mounted_directory.replaces_source {
                    phase = LazyDirectoryPhase::Authored(None);
                }
            }
            let mut source_absent = false;
            loop {
                match phase {
                    LazyDirectoryPhase::Source(source_cursor) => {
                        let receipt = match self
                            .source
                            .list_page(
                                state.source,
                                &directory,
                                source_cursor,
                                maximum_entries,
                                cancellation,
                            )
                            .await
                        {
                            Ok(receipt) => receipt,
                            // A directory only the authored generation holds
                            // (one a fork created) has no source entries; the
                            // authored phase lists it, or reports it absent.
                            Err(failure)
                                if matches!(
                                    failure.error,
                                    DemandError::Absent | DemandError::NotDirectory
                                ) =>
                            {
                                work = account_work(work, *failure.work, budget)?;
                                source_absent = true;
                                phase = LazyDirectoryPhase::Authored(None);
                                continue;
                            }
                            Err(failure) => return Err(failure.error.into()),
                        };
                        work = account_work(work, receipt.work, budget)?;
                        let page = receipt.value;
                        if page.entries.len()
                            > usize::try_from(maximum_entries).unwrap_or(usize::MAX)
                        {
                            return Err(LazyWorkspaceError::Work(
                                "source directory page exceeded its requested bound".to_owned(),
                            ));
                        }
                        let temporary_bytes = retained_directory_page_bytes(
                            page.entries.capacity(),
                            std::mem::size_of::<SourceDirectoryEntry>(),
                            page.entries.iter().map(|entry| entry.name.retained_bytes()),
                        )?;
                        let mut entries = Vec::new();
                        let projection_bytes = reserve_lazy_directory_page(
                            &mut entries,
                            page.entries.len(),
                            temporary_bytes,
                            &mut work,
                            budget,
                        )?;
                        // A source page shares one authored generation. Resolve its
                        // candidate children together so the authenticated directory
                        // and file-table frontier is not rebuilt once per entry.
                        let count = page.entries.len();
                        let child_storage = count
                            .checked_mul(
                                std::mem::size_of::<NamespacePath>()
                                    + std::mem::size_of::<(bool, Option<usize>)>()
                                    + 1
                                    + (directory.components().len() + 1)
                                        * std::mem::size_of::<LogicalName>(),
                            )
                            .and_then(|total| {
                                page.entries.iter().try_fold(total, |sum, entry| {
                                    // Portable-to-native transcoding can expand a
                                    // name (notably UTF-8 to Windows UTF-16).
                                    sum.checked_add(entry.name.retained_bytes().saturating_mul(4))
                                })
                            })
                            .and_then(|total| {
                                total
                                    .checked_add(count.saturating_mul(path.len().saturating_mul(4)))
                            })
                            .and_then(|bytes| u64::try_from(bytes).ok())
                            .ok_or_else(|| {
                                LazyWorkspaceError::Work("counter overflow".to_owned())
                            })?;
                        let live_bytes =
                            projection_bytes.checked_add(child_storage).ok_or_else(|| {
                                LazyWorkspaceError::Work("counter overflow".to_owned())
                            })?;
                        work.peak_allocation_bytes = work.peak_allocation_bytes.max(live_bytes);
                        work.verify(budget)?;
                        let mut source_paths = Vec::new();
                        let mut slots = Vec::new();
                        source_paths.try_reserve_exact(count).map_err(|_| {
                            LazyWorkspaceError::Work(
                                "source page lookup allocation failed".to_owned(),
                            )
                        })?;
                        slots.try_reserve_exact(count).map_err(|_| {
                            LazyWorkspaceError::Work(
                                "source page lookup allocation failed".to_owned(),
                            )
                        })?;
                        let limits = self.workspace.limits();
                        // A child is hidden when it or an ancestor is a
                        // tombstone; the ancestors are this page's directory
                        // and its own, so they are read once for the page.
                        let directory_tombstoned = self
                            .tombstoned_measured(
                                state.overlay,
                                path,
                                remaining_work(work, budget)?,
                                cancellation,
                            )
                            .await?;
                        work = account_nested_with_live_memory(
                            work,
                            directory_tombstoned.work,
                            live_bytes,
                            budget,
                        )?;
                        // A child's own fact is read only when the overlay
                        // records anything beneath the directory at all.
                        let facts_beneath = self
                            .overlay_has_beneath(
                                state.overlay,
                                path,
                                remaining_work(work, budget)?,
                                cancellation,
                            )
                            .await?;
                        work = account_nested_with_live_memory(
                            work,
                            facts_beneath.work,
                            live_bytes,
                            budget,
                        )?;
                        let directory_authored = match directory_authored {
                            Some(authored) => authored,
                            None => match mounted.as_deref_mut() {
                                Some(checkout) if !directory.is_root() => {
                                    let record = checkout
                                        .lookup_no_follow(
                                            &directory,
                                            remaining_work(work, budget)?,
                                            cancellation,
                                        )
                                        .await
                                        .map_err(|failure| {
                                            LazyWorkspaceError::Workspace(failure.error.to_string())
                                        })?;
                                    work = account_nested_with_live_memory(
                                        work,
                                        record.work,
                                        live_bytes,
                                        budget,
                                    )?;
                                    record.value.record.is_some()
                                }
                                _ => true,
                            },
                        };
                        for entry in &page.entries {
                            let child_path = logical_child_path(path, &entry.name);
                            if let Some(child) = child_path.as_ref() {
                                work = account_transient_string(work, child, live_bytes, budget)?;
                                if directory_tombstoned.value
                                    || mounted_mask.is_some_and(|(paths, _)| {
                                        paths.binary_search(child).is_ok()
                                    })
                                {
                                    slots.push((true, None));
                                    continue;
                                }
                                let tombstoned = if facts_beneath.value {
                                    let fact = self
                                        .overlay_fact_measured(
                                            state.overlay,
                                            &self.canonical_path(child)?,
                                            remaining_work(work, budget)?,
                                            cancellation,
                                        )
                                        .await?;
                                    work = account_nested_with_live_memory(
                                        work, fact.work, live_bytes, budget,
                                    )?;
                                    matches!(fact.value, Some(LazyOverlayChange::Tombstone { .. }))
                                } else {
                                    false
                                };
                                if tombstoned {
                                    slots.push((true, None));
                                    continue;
                                }
                                if !directory_authored {
                                    slots.push((false, None));
                                    continue;
                                }
                                let child = self.namespace_path(child)?;
                                slots.push((false, Some(source_paths.len())));
                                source_paths.push(child);
                            } else {
                                slots.push((false, None));
                            }
                        }
                        let mut authored = Vec::new();
                        authored
                            .try_reserve_exact(source_paths.len())
                            .map_err(|_| {
                                LazyWorkspaceError::Work(
                                    "source page lookup allocation failed".to_owned(),
                                )
                            })?;
                        authored.resize(source_paths.len(), false);
                        if directory_authored && !source_paths.is_empty() {
                            let generation = if mounted.is_some() {
                                None
                            } else {
                                let head = self
                                    .workspace
                                    .head_measured(remaining_work(work, budget)?, cancellation)
                                    .await
                                    .map_err(workspace_error)?;
                                work = account_nested_with_live_memory(
                                    work, head.work, live_bytes, budget,
                                )?;
                                Some(head.value)
                            };
                            let maximum = usize::try_from(limits.maximum_paths_per_batch)
                                .unwrap_or(usize::MAX)
                                .max(1);
                            let mut offset = 0_usize;
                            for chunk in source_paths.chunks(maximum) {
                                let (records, lookup_work) = if let Some(checkout) =
                                    mounted.as_deref_mut()
                                {
                                    let lookup = checkout
                                        .lookup_batch_no_follow(
                                            chunk,
                                            remaining_work(work, budget)?,
                                            cancellation,
                                        )
                                        .await
                                        .map_err(|failure| {
                                            LazyWorkspaceError::Workspace(failure.error.to_string())
                                        })?;
                                    (
                                        lookup
                                            .value
                                            .entries
                                            .into_iter()
                                            .map(|entry| entry.record)
                                            .collect::<Vec<_>>(),
                                        lookup.work,
                                    )
                                } else {
                                    let lookup = generation
                                        .as_ref()
                                        .ok_or(LazyWorkspaceError::Concurrent)?
                                        .lookup_paths(
                                            chunk,
                                            remaining_work(work, budget)?,
                                            cancellation,
                                        )
                                        .await
                                        .map_err(|failure| {
                                            LazyWorkspaceError::Workspace(failure.error.to_string())
                                        })?;
                                    (lookup.value, lookup.work)
                                };
                                work = account_nested_with_live_memory(
                                    work,
                                    lookup_work,
                                    live_bytes,
                                    budget,
                                )?;
                                for (slot, record) in authored
                                    .get_mut(offset..offset + chunk.len())
                                    .ok_or_else(|| {
                                        LazyWorkspaceError::Work(
                                            "lookup result offset overflow".to_owned(),
                                        )
                                    })?
                                    .iter_mut()
                                    .zip(records)
                                {
                                    *slot = record.is_some();
                                }
                                offset += chunk.len();
                            }
                        }
                        for (entry, (hidden, index)) in page.entries.into_iter().zip(slots) {
                            if hidden
                                || index.is_some_and(|index| {
                                    authored.get(index).copied().unwrap_or(false)
                                })
                            {
                                continue;
                            }
                            entries.push(LazyDirectoryEntry {
                                name: entry.name,
                                kind: entry.node.kind,
                                authored: false,
                                source: Some(entry.node),
                                unobserved: !facts_beneath.value,
                            });
                        }
                        // The checkout's own names follow the source's, unless
                        // it holds no such directory to name them in.
                        let next = match page.next {
                            Some(next) => Some(LazyDirectoryPhase::Source(Some(next))),
                            None if directory_authored => Some(LazyDirectoryPhase::Authored(None)),
                            None => None,
                        }
                        .map(|phase| LazyDirectoryCursor {
                            source: state.source,
                            overlay: state.overlay,
                            directory,
                            phase,
                        });
                        return Ok(OperationReceipt {
                            value: LazyDirectoryPage { entries, next },
                            work,
                        });
                    }
                    LazyDirectoryPhase::Authored(after) => {
                        let authored_page = if let Some(checkout) = mounted {
                            match checkout
                                .list_directory(
                                    &directory,
                                    after.as_ref(),
                                    maximum_entries,
                                    remaining_work(work, budget)?,
                                    cancellation,
                                )
                                .await
                            {
                                Ok(page) => Ok(OperationReceipt {
                                    value: crate::WorkspaceDirectoryPage {
                                        entries: page
                                            .value
                                            .entries
                                            .into_iter()
                                            .map(|entry| crate::WorkspaceDirectoryEntry {
                                                name: entry.name,
                                                file_id: entry.file_id,
                                                kind: entry.kind,
                                            })
                                            .collect(),
                                        has_more: page.value.has_more,
                                    },
                                    work: page.work,
                                }),
                                Err(failure)
                                    if matches!(failure.error, crate::FsError::NotFound) =>
                                {
                                    Err(WorkspaceError::NotFound)
                                }
                                Err(failure) => Err(WorkspaceError::from(failure.error)),
                            }
                        } else {
                            self.workspace
                                .list_directory_measured(
                                    path,
                                    after.as_ref(),
                                    maximum_entries,
                                    remaining_work(work, budget)?,
                                    cancellation,
                                )
                                .await
                        };
                        let page = match authored_page {
                            Ok(page) => page,
                            Err(WorkspaceError::NotFound) if source_absent => {
                                return Err(LazyWorkspaceError::NotFound);
                            }
                            Err(WorkspaceError::NotFound) => {
                                return Ok(OperationReceipt {
                                    value: LazyDirectoryPage {
                                        entries: Vec::new(),
                                        next: None,
                                    },
                                    work,
                                });
                            }
                            Err(error) => return Err(workspace_error(error)),
                        };
                        work = account_work(work, page.work, budget)?;
                        let page = page.value;
                        if page.entries.len()
                            > usize::try_from(maximum_entries).unwrap_or(usize::MAX)
                        {
                            return Err(LazyWorkspaceError::Work(
                                "authored directory page exceeded its requested bound".to_owned(),
                            ));
                        }
                        let next = if page.has_more {
                            page.entries.last().map(|entry| LazyDirectoryCursor {
                                source: state.source,
                                overlay: state.overlay,
                                directory,
                                phase: LazyDirectoryPhase::Authored(Some(entry.name.clone())),
                            })
                        } else {
                            None
                        };
                        let temporary_bytes = retained_directory_page_bytes(
                            page.entries.capacity(),
                            std::mem::size_of::<crate::WorkspaceDirectoryEntry>(),
                            page.entries.iter().map(|entry| entry.name.retained_bytes()),
                        )?;
                        let mut entries = Vec::new();
                        let _projection_bytes = reserve_lazy_directory_page(
                            &mut entries,
                            page.entries.len(),
                            temporary_bytes,
                            &mut work,
                            budget,
                        )?;
                        entries.extend(page.entries.into_iter().map(authored_entry));
                        return Ok(OperationReceipt {
                            value: LazyDirectoryPage { entries, next },
                            work,
                        });
                    }
                }
            }
        })
        .await
    }
}
