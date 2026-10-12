use super::*;
use crate::demand::SourceDirectoryPage;
use std::ops::Bound;

/// Sparse seek target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LazySeekTarget {
    /// First data byte.
    Data,
    /// First hole, including the end of a dense source file.
    Hole,
}
/// One merged authored/source directory entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LazyDirectoryEntry {
    /// Profile-aware child name.
    pub name: LogicalName,
    /// Exact observed kind.
    pub kind: SourceNodeKind,
    /// Whether the authored checkout supplies the binding.
    pub authored: bool,
    /// Source fact captured by this page.
    pub source: Option<SourceNode>,
    /// Whether no observation was previously recorded.
    pub unobserved: bool,
}
/// A bounded merged page and its continuation.
#[derive(Debug)]
pub struct LazyDirectoryPage {
    /// At most the requested number of visible entries.
    pub entries: Vec<LazyDirectoryEntry>,
    /// Continuation for the same cut and directory.
    pub next: Option<LazyDirectoryCursor>,
}
/// Continuation tied to one immutable logical cut.
pub struct LazyDirectoryCursor {
    cut: LazySnapshotRef,
    directory: NamespacePath,
    phase: LazyDirectoryPhase,
    allocation: KernelOwnedAllocation,
}
#[derive(Debug)]
enum LazyDirectoryPhase {
    Source(Option<SourceCursor>),
    Authored(Option<LogicalName>),
}
impl std::fmt::Debug for LazyDirectoryCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyDirectoryCursor")
            .field("cut", &self.cut)
            .field("directory", &self.directory)
            .field("phase", &self.phase)
            .finish()
    }
}
impl LazyDirectoryCursor {
    /// Actual retained directory and continuation backing.
    pub fn retained_bytes(&self) -> Result<u64, LazyWorkspaceError> {
        self.directory
            .retained_bytes()
            .checked_add(match &self.phase {
                LazyDirectoryPhase::Source(c) => c.as_ref().map_or(0, SourceCursor::retained_bytes),
                LazyDirectoryPhase::Authored(n) => {
                    n.as_ref().map_or(0, |n| n.retained_bytes() as u64)
                }
            })
            .ok_or_else(|| crate::WorkError::Overflow.into())
    }
    /// Copies a continuation after funding its backing.
    pub fn clone(&self, ctx: &mut KernelAccounting<'_>) -> Result<Self, LazyWorkspaceError> {
        let (directory, guard) = self
            .directory
            .clone_with_accounting(ctx, cancellation(ctx))?;
        let bytes = directory.retained_bytes();
        let mut owned = Owned::new(directory, guard, bytes);
        let phase = (|| {
            Ok::<_, LazyWorkspaceError>(match &self.phase {
                LazyDirectoryPhase::Source(Some(cursor)) => {
                    let bytes = cursor.retained_bytes();
                    let copied = WorkCounters {
                        bytes_copied: bytes,
                        ..WorkCounters::UNCHARGED
                    };
                    ctx.admit(copied)?;
                    let mut guard = ctx.claim_owned(bytes, self.directory.depth() as u64 + 1)?;
                    let mut cursor = Clone::clone(cursor);
                    ctx.reconcile_owned(&mut guard, bytes, cursor.retained_bytes())?;
                    ctx.record_completed(copied)?;
                    #[cfg(not(target_arch = "wasm32"))]
                    if let KernelOwnedAllocation::NativeOriginal(allocation) = &mut guard {
                        cursor.attach_native_allocation(allocation)?;
                    }
                    owned.allocation.absorb(&mut guard)?;
                    LazyDirectoryPhase::Source(Some(cursor))
                }
                LazyDirectoryPhase::Authored(Some(name)) => {
                    let (name, mut guard) = name.clone_with_accounting(ctx, cancellation(ctx))?;
                    owned.allocation.absorb(&mut guard)?;
                    LazyDirectoryPhase::Authored(Some(name))
                }
                LazyDirectoryPhase::Source(None) => LazyDirectoryPhase::Source(None),
                LazyDirectoryPhase::Authored(None) => LazyDirectoryPhase::Authored(None),
            })
        })();
        match phase {
            Ok(phase) => Ok(Self {
                cut: self.cut,
                directory: owned.value,
                phase,
                allocation: owned.allocation,
            }),
            Err(error) => {
                owned.retire(ctx)?;
                Err(error)
            }
        }
    }
    /// Drops the cursor before releasing its backing.
    pub fn retire(self, ctx: &mut KernelAccounting<'_>) -> Result<(), LazyWorkspaceError> {
        let bytes = self.retained_bytes()?;
        drop(self.directory);
        drop(self.phase);
        Ok(crate::kernel::retire_allocation(
            self.allocation,
            bytes,
            ctx,
        )?)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Lists the current merged directory.
    pub async fn list_directory(
        &self,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum: u32,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(LazyDirectoryPage, KernelOwnedAllocation), LazyWorkspaceError> {
        let state = self.state(ctx).await?;
        let mut checkout = self
            .checkout(crate::model::GenerationSelector::Head, ctx)
            .await?;
        let result = self
            .list_directory_at(
                &state,
                &mut checkout.value,
                path,
                cursor,
                maximum,
                None,
                ctx,
            )
            .await;
        checkout.retire(ctx)?;
        result.map(|(page, _, guard)| (page, guard))
    }
    /// Lists one frozen authored/source cut.
    pub async fn list_directory_at_snapshot(
        &self,
        cut: LazySnapshotRef,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum: u32,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(LazyDirectoryPage, bool, KernelOwnedAllocation), LazyWorkspaceError> {
        self.validate_snapshot(cut, ctx)?;
        let state = LazyWorkspaceState::new(cut.workspace_id, cut.source, cut.overlay, cut.shadows);
        let mut checkout = self
            .checkout(
                crate::model::GenerationSelector::Exact(cut.authored_generation),
                ctx,
            )
            .await?;
        let result = self
            .list_directory_at(
                &state,
                &mut checkout.value,
                path,
                cursor,
                maximum,
                None,
                ctx,
            )
            .await;
        checkout.retire(ctx)?;
        result
    }
    /// Lists a mounted unpublished checkout.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn list_directory_in_checkout(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum: u32,
        mask: Option<(&[String], bool)>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(LazyDirectoryPage, LazyWorkspaceState, KernelOwnedAllocation), LazyWorkspaceError>
    {
        let state = self.state(ctx).await?;
        let (page, _, guard) = self
            .list_directory_at(&state, checkout, path, cursor, maximum, mask, ctx)
            .await?;
        Ok((page, state, guard))
    }

    /// Lists the merged directory represented by the supplied state and checkout.
    pub async fn list_directory_at(
        &self,
        state: &LazyWorkspaceState,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum: u32,
        mask: Option<(&[String], bool)>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(LazyDirectoryPage, bool, KernelOwnedAllocation), LazyWorkspaceError> {
        if maximum == 0 {
            return Err(LazyWorkspaceError::InvalidPageBound);
        }
        let directory = self.namespace_path(path, ctx)?;
        let mut prior = None;
        let mut phase = LazyDirectoryPhase::Source(None);
        let cut = LazySnapshotRef::new(
            self.workspace.id(),
            checkout.generation_id(),
            state.source,
            state.overlay,
            state.shadows,
        );
        if let Some(cursor) = cursor {
            if cursor.cut != cut || cursor.directory != directory.value {
                cursor.retire(ctx)?;
                directory.retire(ctx)?;
                return Err(LazyWorkspaceError::StaleCursor);
            }
            let bytes = cursor.retained_bytes()?;
            phase = cursor.phase;
            prior = Some(Owned::new(cursor.directory, cursor.allocation, bytes));
        }
        let mut used = false;
        let result = async {
            let authored = checkout
                .lookup_no_follow_with_metadata_with_accounting(
                    &directory.value,
                    ctx,
                    cancellation(ctx),
                )
                .await?;
            if authored
                .as_ref()
                .is_some_and(|entry| entry.record.kind != FileKind::Directory)
            {
                return Err(LazyWorkspaceError::NotDirectory);
            }
            let authored_directory = authored.is_some();
            let hidden = self.tombstoned(state.overlay, path, ctx).await?;
            if hidden && !authored_directory {
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
                used = true;
                let source = self.source_lookup(state.source, path, ctx).await?;
                let replaces = match source {
                    None => true,
                    Some(node) => {
                        self.source_file_id(&node.file_identity, ctx)? != authored.record.file_id
                    }
                };
                if replaces {
                    let mut committed = self
                        .checkout(crate::model::GenerationSelector::Head, ctx)
                        .await?;
                    let original = committed
                        .value
                        .lookup_no_follow_with_metadata_with_accounting(
                            &directory.value,
                            ctx,
                            cancellation(ctx),
                        )
                        .await;
                    committed.retire(ctx)?;
                    if original?
                        .is_none_or(|record| record.record.file_id != authored.record.file_id)
                    {
                        phase = LazyDirectoryPhase::Authored(None);
                    }
                }
            }
            let mut absent = false;
            loop {
                match phase {
                    LazyDirectoryPhase::Source(cursor) => {
                        used = true;
                        if state.source != self.source.reference() {
                            return Err(LazyWorkspaceError::StaleSource);
                        }
                        let bytes = std::mem::size_of::<LazyWorkspaceError>() as u64;
                        let guard = ctx.claim_owned(bytes, 1)?;
                        let mut failure =
                            Owned::new(Box::<LazyWorkspaceError>::new_uninit(), guard, bytes);
                        let fetched = accounted!(ctx, |work, cancel| self.source.list_page(
                            state.source,
                            &directory.value,
                            cursor,
                            maximum,
                            work,
                            cancel
                        ));
                        let mut page = match fetched {
                            Ok(page) => page,
                            Err(LazyWorkspaceError::Demand(
                                DemandError::Absent | DemandError::NotDirectory,
                            )) => {
                                failure.retire(ctx)?;
                                absent = true;
                                phase = LazyDirectoryPhase::Authored(None);
                                continue;
                            }
                            Err(error) => {
                                failure.retire(ctx)?;
                                return Err(error);
                            }
                        };
                        let retained = page.retained_bytes();
                        let mut source_guard = ctx.claim_owned(0, 0)?;
                        let projected = async {
                            if ctx.standalone_parts().is_some() {
                                let (guard, admitted) = ctx.record_actual_owned(retained)?;
                                source_guard = guard;
                                admitted?;
                            }
                            let entries = self
                                .project_source_page(
                                    state,
                                    checkout,
                                    &directory.value,
                                    &page,
                                    maximum,
                                    mask,
                                    ctx,
                                )
                                .await?;
                            if let Some(publication) = page.publication.as_mut()
                                && let Err(error) = publication.accept(ctx)
                            {
                                entries.retire(ctx)?;
                                return Err(error.into());
                            }
                            page.publication.take();
                            Ok::<_, LazyWorkspaceError>(entries)
                        }
                        .await;
                        let entries = match projected {
                            Ok(entries) => entries,
                            Err(primary) => {
                                if let Err(cleanup) =
                                    page.publication.as_mut().map_or(Ok(()), |p| p.abort(ctx))
                                {
                                    let publication = page.publication.take();
                                    let primary = Box::write(failure.value, primary);
                                    #[cfg(not(target_arch = "wasm32"))]
                                    let allocation = match failure.allocation {
                                        KernelOwnedAllocation::NativeOriginal(guard) => Some(guard),
                                        KernelOwnedAllocation::Standalone => None,
                                    };
                                    drop(page);
                                    crate::kernel::retire_allocation(source_guard, retained, ctx)?;
                                    return Err(LazyWorkspaceError::SourcePageCleanup {
                                        primary,
                                        cleanup,
                                        publication,
                                        #[cfg(not(target_arch = "wasm32"))]
                                        allocation,
                                    });
                                }
                                drop(page);
                                crate::kernel::retire_allocation(source_guard, retained, ctx)?;
                                failure.retire(ctx)?;
                                return Err(primary);
                            }
                        };
                        drop(failure.value);
                        ctx.release_owned(&mut failure.allocation, bytes)?;
                        let next = page
                            .next
                            .take()
                            .map(|c| LazyDirectoryPhase::Source(Some(c)))
                            .or_else(|| {
                                authored_directory.then_some(LazyDirectoryPhase::Authored(None))
                            });
                        let transfer = match &next {
                            Some(LazyDirectoryPhase::Source(Some(c))) => c.retained_bytes(),
                            _ => 0,
                        };
                        drop(page);
                        crate::kernel::retire_allocation(source_guard, retained - transfer, ctx)?;
                        return Ok((entries, next.map(|phase| (phase, failure.allocation))));
                    }
                    LazyDirectoryPhase::Authored(after) => {
                        let listed = checkout
                            .list_directory_with_accounting(
                                &directory.value,
                                after.as_ref().map_or(Bound::Unbounded, Bound::Excluded),
                                maximum,
                                ctx,
                                cancellation(ctx),
                            )
                            .await;
                        let (page, mut guard) = match listed {
                            Ok(page) => page,
                            Err(crate::FsError::NotFound) if !absent => {
                                return Ok((Owned::allocate(0, ctx)?, None));
                            }
                            Err(error) => return Err(error.into()),
                        };
                        let retained = retained_directory_page_bytes(
                            &page.entries,
                            page.entries.iter().map(|e| e.name.retained_bytes()),
                        )?;
                        let mut entries = Owned::allocate(page.entries.len(), ctx)?;
                        let next = if page.has_more {
                            let (name, guard) = page
                                .entries
                                .last()
                                .ok_or(LazyWorkspaceError::StaleCursor)?
                                .name
                                .clone_with_accounting(ctx, cancellation(ctx))?;
                            Some((LazyDirectoryPhase::Authored(Some(name)), guard))
                        } else {
                            None
                        };
                        let mut names = 0;
                        for entry in page.entries {
                            names += entry.name.retained_bytes() as u64;
                            entries.value.push(LazyDirectoryEntry {
                                name: entry.name,
                                kind: source_kind(entry.kind),
                                authored: true,
                                source: None,
                                unobserved: false,
                            });
                        }
                        entries.bytes += names;
                        ctx.release_owned(&mut guard, retained - names)?;
                        entries.allocation.absorb(&mut guard)?;
                        return Ok((entries, next));
                    }
                }
            }
        }
        .await;
        if let Some(prior) = prior {
            prior.retire(ctx)?;
        }
        let (entries, next) = match result {
            Ok(result) => result,
            Err(error) => {
                directory.retire(ctx)?;
                return Err(error);
            }
        };
        let next = match next {
            Some((phase, mut guard)) => {
                let mut allocation = directory.allocation;
                allocation.absorb(&mut guard)?;
                Some(LazyDirectoryCursor {
                    cut,
                    directory: directory.value,
                    phase,
                    allocation,
                })
            }
            None => {
                directory.retire(ctx)?;
                None
            }
        };
        Ok((
            LazyDirectoryPage {
                entries: entries.value,
                next,
            },
            used,
            entries.allocation,
        ))
    }

    async fn project_source_page(
        &self,
        state: &LazyWorkspaceState,
        checkout: &mut crate::Checkout<A, O>,
        directory: &NamespacePath,
        page: &SourceDirectoryPage,
        maximum: u32,
        mask: Option<(&[String], bool)>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Owned<Vec<LazyDirectoryEntry>>, LazyWorkspaceError> {
        if page.entries.len() > maximum as usize {
            return Err(LazyWorkspaceError::TooLarge);
        }
        let mut paths = Owned::allocate(page.entries.len(), ctx)?;
        let mut slots = Owned::allocate(page.entries.len(), ctx)?;
        let mut entries = Owned::allocate(page.entries.len(), ctx)?;
        let result = async {
            for entry in page.entries.iter() {
                let mut slot = (false, true, None);
                if let Some(child) = self.child_path(directory, entry.name(), ctx)? {
                    let result = async {
                        let fact = self.overlay_fact(state.overlay, &child.value, ctx).await?;
                        slot.1 = fact.is_none();
                        slot.0 = matches!(fact, Some(LazyOverlayChange::Tombstone { .. }))
                            || mask.is_some_and(|(paths, _)| {
                                paths.binary_search(&child.value).is_ok()
                            });
                        if !slot.0 {
                            slot.2 = Some(paths.value.len());
                            let namespace = self.namespace_path(&child.value, ctx)?;
                            crate::Transaction::<A, O>::push_mutation(&mut paths, namespace, ctx)?;
                        }
                        Ok::<_, LazyWorkspaceError>(())
                    }
                    .await;
                    child.retire(ctx)?;
                    result?;
                }
                slots.value.push(slot);
            }
            let mut offset = 0;
            for chunk in paths
                .value
                .chunks((self.workspace.limits().maximum_paths_per_batch as usize).max(1))
            {
                let (lookup, guard) = checkout
                    .lookup_batch_no_follow_with_accounting(chunk, false, ctx, cancellation(ctx))
                    .await?;
                let bytes = lookup.retained_allocation_bytes;
                for (hidden, _, slot) in &mut slots.value {
                    if let Some(index) = *slot
                        && index >= offset
                        && index < offset + chunk.len()
                    {
                        *hidden |= lookup.entries[index - offset].record.is_some();
                    }
                }
                offset += chunk.len();
                drop(lookup);
                crate::kernel::retire_allocation(guard, bytes, ctx)?;
            }
            for (entry, (hidden, unobserved, _)) in page.entries.iter().zip(&slots.value) {
                if *hidden {
                    continue;
                }
                let (name, mut guard) =
                    entry.name().clone_with_accounting(ctx, cancellation(ctx))?;
                entries.bytes += name.retained_bytes() as u64;
                entries.allocation.absorb(&mut guard)?;
                entries.value.push(LazyDirectoryEntry {
                    name,
                    kind: entry.node.kind,
                    authored: false,
                    source: Some(entry.node),
                    unobserved: *unobserved,
                });
            }
            Ok::<_, LazyWorkspaceError>(())
        }
        .await;
        paths.retire(ctx)?;
        slots.retire(ctx)?;
        if let Err(error) = result {
            entries.retire(ctx)?;
            return Err(error);
        }
        Ok(entries)
    }
}
