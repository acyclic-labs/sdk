use super::*;
use acyclic_fs::LazyDirectoryListing as DirectoryPage;
use acyclic_fs::access::{Action, PreparedPath};

/// Immutable pagination and authority limits for a private directory read.
/// Admission of one row or empty cursor advance under a consumer's byte credit.
pub type DirectoryCredit<'a> = dyn FnMut(
        &GenerationRef,
        Option<&crate::conversation::PrivateDirectoryEntry>,
        Option<&[u8]>,
    ) -> Result<bool>
    + Send
    + Sync
    + 'a;

#[derive(Clone, Copy)]
pub struct DirectoryOptions<'a> {
    /// Exact selected generation, or the current head.
    pub generation: Option<&'a GenerationRef>,
    /// Canonical provider continuation at the selected directory and cut.
    pub after: Option<&'a [u8]>,
    /// Maximum visible entries in the returned page.
    pub maximum_entries: u32,
    /// Additional caller authority ceiling.
    pub ceiling: Option<&'a crate::Capabilities>,
}

pub(in crate::filesystem) struct DirectoryListing<'a> {
    pub volume: &'a VolumeRef,
    pub grant: &'a ContentGrant,
    pub ceiling: Option<&'a crate::Capabilities>,
    pub path: &'a str,
    pub generation: Option<&'a GenerationRef>,
    pub after: Option<&'a [u8]>,
    pub maximum_entries: u32,
}

fn body_bytes(page: &DirectoryPage) -> Result<u64> {
    page.entries.iter().try_fold(
        (page.entries.capacity() as u64)
            .checked_mul(std::mem::size_of::<acyclic_fs::LazyDirectoryEntry>() as u64)
            .and_then(|bytes| {
                bytes.checked_add(page.next.as_ref().map_or(0, |next| next.capacity() as u64))
            })
            .ok_or_else(|| Error::Invalid("directory ownership overflow".into()))?,
        |total, entry| {
            total
                .checked_add(entry.name.logical_owned_bytes())
                .ok_or_else(|| Error::Invalid("directory ownership overflow".into()))
        },
    )
}

fn visible(parent: PreparedPath<'_>, name: &LogicalName, now: u64) -> bool {
    parent
        .child_name(name)
        .is_ok_and(|path| path.decide(Action::List, now).allowed)
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Lists one bounded private page under its current policy.
    pub async fn list_private_directory(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        options: crate::filesystem::DirectoryOptions<'_>,
    ) -> Result<Option<PrivateDirectoryPage>> {
        self.list_private_directory_with_credit(volume, grant, path, options, None)
            .await
    }
    /// Lists visible names at one selected generation under the current policy.
    /// `None` means only that the authorized directory is absent.
    pub async fn list_private_directory_with_credit(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        options: crate::filesystem::DirectoryOptions<'_>,
        credit: Option<&mut DirectoryCredit<'_>>,
    ) -> Result<Option<PrivateDirectoryPage>> {
        let crate::filesystem::DirectoryOptions {
            generation: expected_generation,
            after,
            maximum_entries,
            ceiling,
        } = options;

        let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
        let selected = self
            .list_directory_page(
                DirectoryListing {
                    volume,
                    grant,
                    ceiling,
                    path,
                    generation: expected_generation,
                    after,
                    maximum_entries,
                },
                &io,
                credit,
                &mut || std::future::ready(Ok(())),
            )
            .await?;
        selected
            .map(|(page, hold)| {
                hold.into_retained()?;
                Ok(page)
            })
            .transpose()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) async fn list_private_directory_guarded_with_native_work<F, Fut>(
        &self,
        request: DirectoryListing<'_>,
        work: &acyclic_fs::NativeMaterializationWork,
        mut guard: F,
    ) -> Result<(PrivateDirectoryPage, acyclic_fs::NativeAllocation)>
    where
        F: FnMut() -> Fut + acyclic_stream::ProviderTask,
        Fut: Future<Output = Result<()>> + acyclic_stream::ProviderTask,
    {
        let io = PreparationIo::from_native_work(work)?;
        let (page, hold) = self
            .list_directory_page(request, &io, None, &mut guard)
            .await?
            .ok_or_else(|| {
                stock_boundary(
                    Some(&io),
                    crate::wire::ErrorCode::NotFound,
                    "selected directory does not exist",
                )
            })?;
        let retained = hold.bytes();
        hold.into_retained()?;
        Ok((page, io.into_native_custody(retained)?))
    }

    pub(super) async fn select_directory<'io, F, Fut>(
        &self,
        request: &DirectoryListing<'_>,
        io: &'io PreparationIo,
        guard: &mut F,
    ) -> Result<DirectorySelection<'io, A, O>>
    where
        F: FnMut() -> Fut + acyclic_stream::ProviderTask,
        Fut: Future<Output = Result<()>> + acyclic_stream::ProviderTask,
    {
        self.require_private_directory(request.volume, request.grant, request.path, Some(io))?;
        super::super::access::require_directory_ceiling(
            request.ceiling,
            request.volume,
            request.path,
            Some(io),
        )?;
        if request.maximum_entries == 0
            || request.maximum_entries as usize > MAX_PRIVATE_DIRECTORY_PAGE
        {
            return Err(io.invalid("private directory page limit is invalid"));
        }
        let reference = bounded_io::content_workspace(&self.provider, request.volume, Some(io))?;
        let reference_hold = io.guard_owned(reference.logical_owned_bytes()?)?;
        let reference = (reference, reference_hold);
        guard().await?;
        let workspace = self.open(&reference.0, Some(io)).await?;
        guard().await?;
        #[cfg(not(target_arch = "wasm32"))]
        let snapshot = self
            .private_snapshot(&workspace, request.generation, io)
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        let lazy_generation = snapshot.as_ref().map(|cut| cut.authored_generation);
        #[cfg(target_arch = "wasm32")]
        let lazy_generation: Option<acyclic_fs::GenerationId> = None;
        let generation = match (lazy_generation, request.generation) {
            (Some(generation), _) => self.select_generation(&workspace, generation, io).await?,
            (None, Some(expected)) => self.generation(&workspace, expected, Some(io)).await?,
            (None, None) => self.current_generation(&workspace, io).await?,
        };
        guard().await?;
        let scope = request.grant.policy_scope();
        let label = crate::core::filesystem_scope_label(
            scope.capabilities(),
            &mut Some(&mut io.history_io()),
        )?;
        let view = crate::filesystem::access::access_view(
            &workspace,
            scope,
            Some(generation.id()),
            super::super::access::capability_payload(label, VolumeOperation::Read, io)?,
            request.ceiling,
            io,
        )
        .await?;
        guard().await?;
        super::super::access::require(&view.0, Action::List, request.path, io)?;
        let data_access = super::super::access::selected_data_access(
            &workspace,
            &generation,
            scope,
            request.ceiling,
            io,
        )
        .await?;
        if let Some((_, view)) = &data_access {
            super::super::access::require(&view.0, Action::List, request.path, io)?;
        }
        Ok(DirectorySelection {
            workspace,
            generation,
            view,
            data_access,
            #[cfg(not(target_arch = "wasm32"))]
            snapshot,
        })
    }

    pub(in crate::filesystem) async fn list_directory_page<'io, F, Fut>(
        &self,
        request: DirectoryListing<'_>,
        io: &'io PreparationIo,
        mut credit: Option<&mut DirectoryCredit<'_>>,
        guard: &mut F,
    ) -> Result<Option<(PrivateDirectoryPage, DecodeReservation<'io>)>>
    where
        F: FnMut() -> Fut + acyclic_stream::ProviderTask,
        Fut: Future<Output = Result<()>> + acyclic_stream::ProviderTask,
    {
        let selection = self.select_directory(&request, io, guard).await?;
        let generation = &selection.generation;
        let parent = selection
            .view
            .0
            .prepare(request.path)
            .map_err(|_| io.invalid("invalid directory access path"))?;
        let data_parent = selection
            .data_access
            .as_ref()
            .map(|(_, view)| view.0.prepare(request.path))
            .transpose()
            .map_err(|_| io.invalid("invalid directory access path"))?;
        bounded_io::retain(generation, Some(io)).await?;
        guard().await?;
        let requested = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{}", request.path), work)
        })?;
        let requested_hold = io.guard_owned(requested.capacity() as u64)?;
        let requested = (requested, requested_hold);
        #[cfg(not(target_arch = "wasm32"))]
        let reference = match &selection.snapshot {
            Some(cut) => self.private_generation_ref(cut, request.generation, io)?,
            None => bounded_io::generation_ref(&self.provider, generation.id(), Some(io))?,
        };
        #[cfg(target_arch = "wasm32")]
        let reference = bounded_io::generation_ref(&self.provider, generation.id(), Some(io))?;
        let mut held = io.reserve_decode(std::mem::size_of::<PrivateDirectoryPage>() as u64)?;
        held.adopt(reference.logical_owned_bytes()?)?;
        let mut selected = (
            PrivateDirectoryPage {
                generation: reference,
                entries: Vec::new(),
                next: None,
            },
            held,
        );
        let chunk = if credit.is_some() {
            1
        } else {
            request.maximum_entries
        };
        let mut first = true;
        'pages: for _ in 0..if credit.is_some() {
            request.maximum_entries
        } else {
            1
        } {
            let after = if first {
                request.after
            } else {
                selected.0.next.as_deref()
            };
            let Some(mut incoming) = self
                .read_directory_listing(&selection, (&requested.0, after, chunk), io)
                .await?
            else {
                selection.require_current(io).await?;
                request.grant.recheck(Some(io))?;
                return Ok(None);
            };
            guard().await?;
            io.charge(WorkCounters {
                items_examined: incoming.0.entries.len() as u64,
                ..Default::default()
            })?;
            let now = super::super::access::access_now()?;
            let removed = filter_page(&mut incoming.0, parent, data_parent, now);
            incoming.1.substitute(incoming.1.bytes() - removed)?;
            if incoming.0.entries.is_empty()
                && let Some(credit) = credit.as_mut()
            {
                if !credit(&selected.0.generation, None, incoming.0.next.as_deref())? {
                    break;
                }
            }
            let mut entries = std::mem::take(&mut incoming.0.entries).into_iter();
            while let Some(entry) = entries.next() {
                let entry = crate::conversation::PrivateDirectoryEntry::from_filesystem(entry, io)?;
                let offered = (entry, io.guard_owned(0)?);
                let mut offered = offered;
                offered.1.adopt(offered.0.name.capacity() as u64)?;
                if let Some(credit) = credit.as_mut() {
                    if !credit(
                        &selected.0.generation,
                        Some(&offered.0),
                        incoming.0.next.as_deref(),
                    )? {
                        drop(offered);
                        drop(entries);
                        drop(incoming);
                        break 'pages;
                    }
                }
                let before = selected.0.entries.capacity();
                crate::store::history::reserve_for_push(
                    &mut selected.0.entries,
                    &mut io.history_io(),
                )?;
                selected.1.adopt(
                    ((selected.0.entries.capacity() - before)
                        * std::mem::size_of::<crate::conversation::PrivateDirectoryEntry>())
                        as u64,
                )?;
                selected.0.entries.push(offered.0);
                selected.1.absorb(offered.1)?;
            }
            drop(entries);
            let old = selected.0.next.take();
            let bytes = old.as_ref().map_or(0, |next| next.capacity() as u64);
            drop(old);
            selected.1.substitute(selected.1.bytes() - bytes)?;
            selected.0.next = incoming.0.next.take();
            let bytes = selected
                .0
                .next
                .as_ref()
                .map_or(0, |next| next.capacity() as u64);
            drop(incoming.0);
            incoming.1.substitute(bytes)?;
            selected.1.absorb(incoming.1)?;
            first = false;
            if selected.0.next.is_none() {
                break;
            }
        }
        io.account(|ctx| {
            acyclic_fs::kernel::sort_by_with_accounting(
                &mut selected.0.entries,
                ctx,
                |left, right| left.name.as_bytes().cmp(right.name.as_bytes()),
            )
        })?;
        selection.require_current(io).await?;
        request.grant.recheck(Some(io))?;
        Ok(Some(selected))
    }
    async fn read_directory_listing<'io>(
        &self,
        selection: &DirectorySelection<'_, A, O>,
        page: (&str, Option<&[u8]>, u32),
        io: &'io PreparationIo,
    ) -> Result<Option<(DirectoryPage, DecodeReservation<'io>)>> {
        let (path, after, maximum) = page;
        #[cfg(not(target_arch = "wasm32"))]
        {
            use acyclic_fs::kernel::{KernelAccounting, KernelOwnedAllocation, NativeAccounting};
            let owner = io.account(NativeAccounting::take)?;
            let native_io = PreparationIo::from_native_work(&owner.work)?;
            let mut ctx = KernelAccounting::NativeOriginal {
                work: &owner.work,
                cancellation: io.cancellation(),
            };
            let produced = async {
                match &selection.snapshot {
                    Some(cut) => {
                        let view = self
                            .private_view
                            .as_ref()
                            .ok_or_else(|| io.invalid("private source binding is unavailable"))?;
                        view.directory(cut, path, after, maximum, &mut ctx, &native_io)?
                            .await
                    }
                    None => selection
                        .generation
                        .list_directory_listing_with_accounting(path, after, maximum, &mut ctx)
                        .await
                        .map_err(Into::into),
                }
            }
            .await;
            let produced = produced.and_then(|(page, allocation)| {
                let KernelOwnedAllocation::NativeOriginal(allocation) = allocation else {
                    return Err(io.invalid("directory output lost its original native custody"));
                };
                let bytes = body_bytes(&page)?;
                let allocation = owner.return_allocation(allocation, bytes)?;
                Ok((page, allocation, bytes))
            });
            drop(ctx);
            drop(native_io);
            io.account(|ctx| owner.restore(ctx))?;
            let (page, allocation, bytes) = match produced {
                Ok(value) => value,
                Err(Error::NativeSource { source, .. })
                    if matches!(*source, acyclic_fs::LazyWorkspaceError::NotFound) =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            if let KernelOwnedAllocation::NativeOriginal(allocation) = allocation {
                io.absorb_native_custody(allocation)?;
            }
            let hold = io.guard_owned(bytes)?;
            Ok(Some((page, hold)))
        }
        #[cfg(target_arch = "wasm32")]
        {
            let (result, recorded) = selection
                .generation
                .list_directory_listing(path, after, maximum, io, io.cancellation())
                .await;
            let result = match result {
                Ok(page) => Ok(Some(page)),
                Err(acyclic_fs::LazyWorkspaceError::NotFound) => Ok(None),
                Err(error) => Err(error),
            };
            let Some(page) = io.sdk((result, recorded))? else {
                return Ok(None);
            };
            let bytes = body_bytes(&page)?;
            io.retain_owned(bytes)?;
            let hold = io.guard_owned(bytes)?;
            Ok(Some((page, hold)))
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

fn filter_page(
    page: &mut DirectoryPage,
    parent: PreparedPath<'_>,
    data_parent: Option<PreparedPath<'_>>,
    now: u64,
) -> u64 {
    let mut removed = 0;
    page.entries.retain(|entry| {
        let keep = visible(parent, &entry.name, now)
            && data_parent.is_none_or(|parent| visible(parent, &entry.name, now));
        if !keep {
            removed += entry.name.logical_owned_bytes();
        }
        keep
    });
    removed
}
pub(super) struct DirectorySelection<'io, A: 'static, O: 'static> {
    workspace: Workspace<A, O>,
    pub(super) generation: acyclic_fs::Generation<A, O>,
    view: super::super::access::CompiledAccess<'io>,
    data_access: Option<(Workspace<A, O>, super::super::access::CompiledAccess<'io>)>,
    #[cfg(not(target_arch = "wasm32"))]
    snapshot: Option<acyclic_fs::kernel::Retained<acyclic_fs::LazySnapshotRef>>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> DirectorySelection<'_, A, O> {
    /// Whether both policy owners let this selection's caller list `path`.
    pub(super) fn lists(&self, path: &str, now: u64) -> bool {
        self.allows(Action::List, path, now)
    }

    /// Whether both policy owners let this selection's caller take `action`
    /// on `path`.
    pub(super) fn allows(&self, action: Action, path: &str, now: u64) -> bool {
        self.view.0.decide(action, path, now).allowed
            && self
                .data_access
                .as_ref()
                .is_none_or(|(_, view)| view.0.decide(action, path, now).allowed)
    }

    /// Linearizes a disclosure from this selection against the current policy.
    pub(super) async fn require_current(&self, io: &PreparationIo) -> Result<()> {
        super::super::access::require_current(&self.workspace, &self.view.0, io).await?;
        if let Some((workspace, view)) = &self.data_access {
            super::super::access::require_current(workspace, &view.0, io).await?;
        }
        Ok(())
    }
}
