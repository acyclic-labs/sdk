use super::*;
use crate::demand::SourceDirectoryPage;
use crate::kernel::{KernelAccounting, Owned, allocation::KernelOwnedAllocation};
use std::ops::Bound;

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
    /// Whether the authored checkout supplies the name.
    pub authored: bool,
    /// Source fact captured by this listing.
    pub source: Option<SourceNode>,
    /// Whether the observation index contains no child fact.
    pub unobserved: bool,
}

/// One bounded merged directory page.
#[derive(Debug, Eq, PartialEq)]
pub struct LazyDirectoryPage {
    /// At most the requested number of visible entries.
    pub entries: Vec<LazyDirectoryEntry>,
    /// Continuation, absent after the authored phase is exhausted.
    pub next: Option<LazyDirectoryCursor>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Reads the current merged directory under the supplied accounting context.
    pub async fn list_directory(
        &self,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum: u32,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(LazyDirectoryPage, KernelOwnedAllocation), LazyWorkspaceError> {
        let state = self.state(accounting).await?;
        let mut checkout = self
            .checkout(crate::model::GenerationSelector::Head, accounting)
            .await?;
        let result = self
            .list_directory_at(
                &state,
                &mut checkout.value,
                path,
                cursor,
                maximum,
                None,
                accounting,
            )
            .await;
        checkout.retire(accounting)?;
        result.map(|(page, _, allocation)| (page, allocation))
    }

    /// Reads the merged directory against an unpublished mounted checkout.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn list_directory_in_checkout(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum: u32,
        mask: Option<(&[String], bool)>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(LazyDirectoryPage, LazyWorkspaceState, KernelOwnedAllocation), LazyWorkspaceError>
    {
        let state = self.state(accounting).await?;
        let (page, _, allocation) = self
            .list_directory_at(&state, checkout, path, cursor, maximum, mask, accounting)
            .await?;
        Ok((page, state, allocation))
    }

    pub(crate) async fn list_directory_at(
        &self,
        state: &LazyWorkspaceState,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum: u32,
        mask: Option<(&[String], bool)>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(LazyDirectoryPage, bool, KernelOwnedAllocation), LazyWorkspaceError> {
        if maximum == 0 {
            return Err(LazyWorkspaceError::InvalidPageBound);
        }
        let directory = self.namespace_path(path, accounting)?;
        accounting.charge(WorkCounters {
            bytes_hashed: LAZY_SNAPSHOT_DOMAIN.len() as u64 + 136,
            ..WorkCounters::UNCHARGED
        })?;
        let cut = LazySnapshotRef::new(
            self.workspace.id(),
            checkout.generation_id(),
            state.source,
            state.overlay,
            state.shadows,
        );
        let mut prior = None;
        let mut phase = LazyDirectoryPhase::Source(None);
        if let Some(cursor) = cursor {
            if cursor.cut != cut || cursor.directory != directory.value {
                cursor.retire(accounting)?;
                directory.retire(accounting)?;
                return Err(LazyWorkspaceError::StaleCursor);
            }
            let bytes = cursor.retained_bytes()?;
            phase = cursor.phase;
            prior = Some(Owned::new(cursor.directory, cursor.allocation, bytes));
        }
        let result = self
            .list_directory_phase(
                state, checkout, path, directory, cut, phase, maximum, mask, accounting,
            )
            .await;
        if let Some(prior) = prior {
            prior.retire(accounting)?;
        }
        result
    }

    async fn list_directory_phase(
        &self,
        state: &LazyWorkspaceState,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        directory: Owned<NamespacePath>,
        cut: LazySnapshotRef,
        mut phase: LazyDirectoryPhase,
        maximum: u32,
        mask: Option<(&[String], bool)>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(LazyDirectoryPage, bool, KernelOwnedAllocation), LazyWorkspaceError> {
        let mut source_used = false;
        let cancellation = cancellation(accounting);
        let authored = checkout
            .lookup_no_follow_with_metadata_with_accounting(
                &directory.value,
                accounting,
                cancellation,
            )
            .await?;
        if authored
            .as_ref()
            .is_some_and(|entry| entry.record.kind != FileKind::Directory)
        {
            directory.retire(accounting)?;
            return Err(LazyWorkspaceError::NotDirectory);
        }
        let directory_authored = authored.is_some();
        let hidden = self.tombstoned(state.overlay, path, accounting).await?;
        if hidden && !directory_authored {
            directory.retire(accounting)?;
            return Err(LazyWorkspaceError::NotFound);
        }
        if matches!(phase, LazyDirectoryPhase::Source(None))
            && (hidden || mask.is_some_and(|(_, opaque)| opaque))
        {
            phase = LazyDirectoryPhase::Authored(None);
        }
        if matches!(phase, LazyDirectoryPhase::Source(None))
            && !directory.value.is_root()
            && let Some(authored) = authored
        {
            source_used = true;
            let source = self.source_lookup(state.source, path, accounting).await?;
            let replaces = match source {
                None => true,
                Some(node) => {
                    self.source_file_id(&node.file_identity, accounting)? != authored.record.file_id
                }
            };
            if replaces {
                let mut committed = self
                    .checkout(crate::model::GenerationSelector::Head, accounting)
                    .await?;
                let record = committed
                    .value
                    .lookup_no_follow_with_metadata_with_accounting(
                        &directory.value,
                        accounting,
                        cancellation,
                    )
                    .await;
                committed.retire(accounting)?;
                if record?.is_none_or(|record| record.record.file_id != authored.record.file_id) {
                    phase = LazyDirectoryPhase::Authored(None);
                }
            }
        }
        let mut source_absent = false;
        loop {
            match phase {
                LazyDirectoryPhase::Source(source_cursor) => {
                    source_used = true;
                    if state.source != self.source.reference() {
                        directory.retire(accounting)?;
                        return Err(LazyWorkspaceError::StaleSource);
                    }
                    let bytes = std::mem::size_of::<LazyWorkspaceError>() as u64;
                    let cleanup_allocation = accounting.claim_owned(bytes, 1)?;
                    let cleanup_slot = Box::<LazyWorkspaceError>::new_uninit();
                    let answer = 'accounted: {
                        #[cfg(not(target_arch = "wasm32"))]
                        if let Some(original) = accounting.native_work() {
                            break 'accounted {
                                self.source
                                    .list_page(
                                        state.source,
                                        &directory.value,
                                        source_cursor,
                                        maximum,
                                        original,
                                        cancellation,
                                    )
                                    .await
                                    .map_err(LazyWorkspaceError::from)
                            };
                        }
                        accept_receipt(
                            self.source
                                .list_page(
                                    state.source,
                                    &directory.value,
                                    source_cursor,
                                    maximum,
                                    accounting.remaining()?,
                                    cancellation,
                                )
                                .await,
                            accounting,
                        )
                    };
                    let mut page = match answer {
                        Ok(page) => page,
                        Err(LazyWorkspaceError::Demand(
                            DemandError::Absent | DemandError::NotDirectory,
                        )) => {
                            drop(cleanup_slot);
                            crate::kernel::retire_allocation(
                                cleanup_allocation,
                                bytes,
                                accounting,
                            )?;
                            source_absent = true;
                            phase = LazyDirectoryPhase::Authored(None);
                            continue;
                        }
                        Err(error) => {
                            drop(cleanup_slot);
                            crate::kernel::retire_allocation(
                                cleanup_allocation,
                                bytes,
                                accounting,
                            )?;
                            directory.retire(accounting)?;
                            return Err(error);
                        }
                    };
                    let retained = page.retained_bytes();
                    let mut source_allocation = accounting.claim_owned(0, 0)?;
                    let admitted = if accounting.standalone_parts().is_some() {
                        let (claim, admitted) = accounting.record_actual_owned(retained)?;
                        source_allocation = claim;
                        admitted.map_err(LazyWorkspaceError::from)
                    } else {
                        Ok(())
                    };
                    let projection = match admitted {
                        Ok(()) => {
                            self.project_source_page(
                                state,
                                checkout,
                                &directory.value,
                                &page,
                                maximum,
                                mask,
                                accounting,
                            )
                            .await
                        }
                        Err(error) => Err(error),
                    };
                    let projection = match projection {
                        Ok(entries) => {
                            let accepted = page
                                .publication
                                .as_mut()
                                .map_or(Ok(()), |publication| publication.accept(accounting));
                            match accepted {
                                Ok(()) => {
                                    page.publication.take();
                                    Ok(entries)
                                }
                                Err(error) => {
                                    entries.retire(accounting)?;
                                    Err(error.into())
                                }
                            }
                        }
                        Err(error) => Err(error),
                    };
                    let entries = match projection {
                        Ok(entries) => entries,
                        Err(primary) => {
                            let rollback = page
                                .publication
                                .as_mut()
                                .map_or(Ok(()), |publication| publication.abort(accounting));
                            if let Err(cleanup) = rollback {
                                let publication = page.publication.take();
                                let primary = Box::write(cleanup_slot, primary);
                                #[cfg(not(target_arch = "wasm32"))]
                                let allocation = match cleanup_allocation {
                                    KernelOwnedAllocation::NativeOriginal(allocation) => {
                                        Some(allocation)
                                    }
                                    KernelOwnedAllocation::Standalone => None,
                                };
                                drop(page);
                                crate::kernel::retire_allocation(
                                    source_allocation,
                                    retained,
                                    accounting,
                                )?;
                                directory.retire(accounting)?;
                                return Err(LazyWorkspaceError::SourcePageCleanup {
                                    primary,
                                    cleanup,
                                    publication,
                                    #[cfg(not(target_arch = "wasm32"))]
                                    allocation,
                                });
                            }
                            drop(page);
                            crate::kernel::retire_allocation(
                                source_allocation,
                                retained,
                                accounting,
                            )?;
                            drop(cleanup_slot);
                            crate::kernel::retire_allocation(
                                cleanup_allocation,
                                bytes,
                                accounting,
                            )?;
                            directory.retire(accounting)?;
                            return Err(primary);
                        }
                    };
                    drop(cleanup_slot);
                    crate::kernel::retire_allocation(cleanup_allocation, bytes, accounting)?;
                    let next = match page.next.take() {
                        Some(cursor) => Some(LazyDirectoryPhase::Source(Some(cursor))),
                        None if directory_authored => Some(LazyDirectoryPhase::Authored(None)),
                        None => None,
                    };
                    let transferred = next.as_ref().map_or(0, |phase| match phase {
                        LazyDirectoryPhase::Source(Some(cursor)) => cursor.retained_bytes(),
                        _ => 0,
                    });
                    drop(page);
                    crate::kernel::retire_allocation(
                        source_allocation,
                        retained - transferred,
                        accounting,
                    )?;
                    let cursor = if let Some(phase) = next {
                        Some(LazyDirectoryCursor {
                            cut,
                            directory: directory.value,
                            phase,
                            allocation: directory.allocation,
                        })
                    } else {
                        directory.retire(accounting)?;
                        None
                    };
                    return Ok((
                        LazyDirectoryPage {
                            entries: entries.value,
                            next: cursor,
                        },
                        source_used,
                        entries.allocation,
                    ));
                }
                LazyDirectoryPhase::Authored(after) => {
                    let answer = checkout
                        .list_directory_with_accounting(
                            &directory.value,
                            after.as_ref().map_or(Bound::Unbounded, Bound::Excluded),
                            maximum,
                            accounting,
                            cancellation,
                        )
                        .await;
                    let (page, allocation) = match answer {
                        Ok(page) => page,
                        Err(crate::FsError::NotFound) if !source_absent => {
                            directory.retire(accounting)?;
                            return Ok((
                                LazyDirectoryPage {
                                    entries: Vec::new(),
                                    next: None,
                                },
                                source_used,
                                accounting.claim_owned(0, 0)?,
                            ));
                        }
                        Err(error) => {
                            directory.retire(accounting)?;
                            return Err(error.into());
                        }
                    };
                    let retained = retained_directory_page_bytes(
                        &page.entries,
                        page.entries.iter().map(|entry| entry.name.retained_bytes()),
                    )?;
                    let mut entries = Owned::allocate(page.entries.len(), accounting)?;
                    for entry in &page.entries {
                        let (name, mut claim) =
                            entry.name.clone_with_accounting(accounting, cancellation)?;
                        entries.bytes += name.retained_bytes() as u64;
                        entries.allocation.absorb(&mut claim)?;
                        entries.value.push(LazyDirectoryEntry {
                            name,
                            kind: source_kind(entry.kind),
                            authored: true,
                            source: None,
                            unobserved: false,
                        });
                    }
                    let next = if page.has_more {
                        let name = page
                            .entries
                            .last()
                            .ok_or(LazyWorkspaceError::StaleCursor)?
                            .name
                            .clone_with_accounting(accounting, cancellation)?;
                        Some(name)
                    } else {
                        None
                    };
                    drop(page);
                    crate::kernel::retire_allocation(allocation, retained, accounting)?;
                    let next = if let Some((name, mut claim)) = next {
                        let mut allocation = directory.allocation;
                        allocation.absorb(&mut claim)?;
                        Some(LazyDirectoryCursor {
                            cut,
                            directory: directory.value,
                            phase: LazyDirectoryPhase::Authored(Some(name)),
                            allocation,
                        })
                    } else {
                        directory.retire(accounting)?;
                        None
                    };
                    return Ok((
                        LazyDirectoryPage {
                            entries: entries.value,
                            next,
                        },
                        source_used,
                        entries.allocation,
                    ));
                }
            }
        }
    }

    async fn project_source_page(
        &self,
        state: &LazyWorkspaceState,
        checkout: &mut crate::Checkout<A, O>,
        directory: &NamespacePath,
        page: &SourceDirectoryPage,
        maximum: u32,
        mask: Option<(&[String], bool)>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Owned<Vec<LazyDirectoryEntry>>, LazyWorkspaceError> {
        if page.entries.len() > maximum as usize {
            return Err(LazyWorkspaceError::TooLarge);
        }
        let mut entries = Owned::allocate(page.entries.len(), accounting)?;
        let result = async {
            for entry in page.entries.iter() {
                let child = self.child_path(directory, &entry.name, accounting)?;
                let mut unobserved = true;
                let include = if let Some(child) = child {
                    let result = async {
                        if mask.is_some_and(|(paths, _)| paths.binary_search(&child.value).is_ok())
                        {
                            return Ok(false);
                        }
                        let fact = self
                            .overlay_fact(state.overlay, &child.value, accounting)
                            .await?;
                        unobserved = fact.is_none();
                        if matches!(fact, Some(LazyOverlayChange::Tombstone { .. })) {
                            return Ok(false);
                        }
                        let namespace = self.namespace_path(&child.value, accounting)?;
                        let authored = checkout
                            .lookup_no_follow_with_metadata_with_accounting(
                                &namespace.value,
                                accounting,
                                cancellation(accounting),
                            )
                            .await;
                        namespace.retire(accounting)?;
                        Ok::<_, LazyWorkspaceError>(authored?.is_none())
                    }
                    .await;
                    child.retire(accounting)?;
                    result?
                } else {
                    true
                };
                if include {
                    let (name, mut claim) = entry
                        .name()
                        .clone_with_accounting(accounting, cancellation(accounting))?;
                    entries.bytes += name.retained_bytes() as u64;
                    entries.allocation.absorb(&mut claim)?;
                    entries.value.push(LazyDirectoryEntry {
                        name,
                        kind: entry.node.kind,
                        authored: false,
                        source: Some(entry.node),
                        unobserved,
                    });
                }
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            entries.retire(accounting)?;
            return Err(error);
        }
        Ok(entries)
    }
}
