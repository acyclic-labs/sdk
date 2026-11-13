use super::*;
use acyclic_fs::access::{Action, PreparedPath};
pub(in crate::filesystem) mod cursor;
#[cfg(not(target_arch = "wasm32"))]
mod lazy;
#[cfg(not(target_arch = "wasm32"))]
mod prefix;

/// In-memory producer positions for this one authorized physical page.
/// These are consumed before publication, never sent as directory entries.
#[derive(Debug)]
pub(in crate::filesystem) enum DirectoryPosition {
    Source(acyclic_fs::demand::SourceCursor),
    Authored,
    Unavailable,
    Removed,
}

pub(in crate::filesystem) fn position_bytes(capacity: usize) -> u64 {
    (capacity * std::mem::size_of::<DirectoryPosition>()) as u64
}

/// A bounded authorized physical page. Empty entries with `next` are progress.
#[derive(Debug)]
pub struct AuthorizedDirectoryPage {
    /// Only names allowed by both current policy owners.
    pub page: acyclic_fs::DirectoryPage,
    /// Confidential continuation at the same immutable generation and directory.
    pub next: Option<String>,
    /// Candidate names consumed before policy filtering; excludes SDK seek work.
    pub examined: u32,
    pub(in crate::filesystem) positions: Vec<DirectoryPosition>,
}

impl std::ops::Deref for AuthorizedDirectoryPage {
    type Target = acyclic_fs::DirectoryPage;
    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

/// Immutable pagination and authority limits for a private directory read.
#[derive(Clone, Copy)]
pub struct DirectoryOptions<'a> {
    /// Exact selected generation, or the current head.
    pub generation: Option<&'a GenerationRef>,
    /// Confidential continuation returned by the previous page.
    pub after: Option<&'a str>,
    /// Maximum candidate entries consumed before policy filtering.
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
    pub after: Option<&'a str>,
    pub maximum_entries: u32,
}

fn raw_owned_bytes(page: &DirectoryPage) -> Result<u64> {
    page.entries.iter().try_fold(
        (page.entries.capacity() as u64)
            .checked_mul(std::mem::size_of::<acyclic_fs::kernel::TreeEntry>() as u64)
            .and_then(|bytes| {
                bytes.checked_add(std::mem::size_of::<(GenerationRef, DirectoryPage)>() as u64)
            })
            .ok_or_else(|| Error::Invalid("directory ownership overflow".into()))?,
        |total, entry| {
            total
                .checked_add(entry.name.logical_owned_bytes())
                .ok_or_else(|| Error::Invalid("directory ownership overflow".into()))
        },
    )
}

#[cfg(test)]
fn owned_bytes(page: &AuthorizedDirectoryPage) -> Result<u64> {
    Ok(raw_owned_bytes(&page.page)?
        + position_bytes(page.positions.capacity())
        + (std::mem::size_of::<AuthorizedDirectoryPage>() - std::mem::size_of::<DirectoryPage>())
            as u64
        + page
            .next
            .as_ref()
            .map_or(0, |token| token.capacity() as u64))
}
fn visible(parent: PreparedPath<'_>, name: &LogicalName, now: u64) -> bool {
    parent
        .child_name(name)
        .is_ok_and(|path| path.decide(Action::List, now).allowed)
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Lists visible names at one selected generation under the current policy.
    /// `None` means only that the authorized directory is absent.
    pub async fn list_private_directory(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        options: crate::filesystem::DirectoryOptions<'_>,
    ) -> Result<Option<(GenerationRef, AuthorizedDirectoryPage)>> {
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
    ) -> Result<(
        (GenerationRef, AuthorizedDirectoryPage),
        acyclic_fs::NativeAllocation,
    )>
    where
        F: FnMut() -> Fut + acyclic_stream::ProviderTask,
        Fut: Future<Output = Result<()>> + acyclic_stream::ProviderTask,
    {
        let io = PreparationIo::from_native_work(work)?;
        let (page, hold) = self
            .list_directory_page(request, &io, &mut guard)
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
        let lazy_generation: Option<GenerationId> = None;
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
        guard: &mut F,
    ) -> Result<
        Option<(
            (GenerationRef, AuthorizedDirectoryPage),
            DecodeReservation<'io>,
        )>,
    >
    where
        F: FnMut() -> Fut + acyclic_stream::ProviderTask,
        Fut: Future<Output = Result<()>> + acyclic_stream::ProviderTask,
    {
        #[cfg(not(target_arch = "wasm32"))]
        if self.private_view.is_some() && io.original_native_work().is_none() {
            // One original-ledger handoff covers cursor decode, source work and
            // disclosure. No cursor crosses independently admitted ledgers.
            let owner = io.account(acyclic_fs::kernel::NativeAccounting::take)?;
            let native = PreparationIo::from_native_work(&owner.work)?;
            let result = async {
                let Some((page, hold)) = self
                    .list_directory_page_inner(request, &native, guard)
                    .await?
                else {
                    return Ok(None);
                };
                let bytes = hold.bytes();
                hold.into_retained()?;
                let allocation = native.into_native_custody(bytes)?;
                io.accept_native_output(&owner, allocation, bytes)?;
                Ok::<_, Error>(Some((page, bytes)))
            }
            .await;
            io.account(|ctx| owner.restore(ctx))?;
            return result?
                .map(|(page, bytes)| Ok((page, io.guard_owned(bytes)?)))
                .transpose();
        }
        self.list_directory_page_inner(request, io, guard).await
    }

    async fn list_directory_page_inner<'io, F, Fut>(
        &self,
        request: DirectoryListing<'_>,
        io: &'io PreparationIo,
        guard: &mut F,
    ) -> Result<
        Option<(
            (GenerationRef, AuthorizedDirectoryPage),
            DecodeReservation<'io>,
        )>,
    >
    where
        F: FnMut() -> Fut + acyclic_stream::ProviderTask,
        Fut: Future<Output = Result<()>> + acyclic_stream::ProviderTask,
    {
        #[cfg(target_arch = "wasm32")]
        if request
            .after
            .is_some_and(|token| token.starts_with("lazy:"))
        {
            return Err(io.invalid("lazy directory cursors require native source support"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let lazy_cursor = self.open_lazy_directory_cursor(&request, io)?;
        #[cfg(not(target_arch = "wasm32"))]
        let lazy_reference = lazy_cursor
            .as_ref()
            .map(|cursor| self.private_generation_ref(cursor.snapshot(), None, io))
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let _lazy_reference_hold = lazy_reference
            .as_ref()
            .map(|reference| io.guard_owned(reference.logical_owned_bytes()?))
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let (Some(expected), Some(selected)) = (request.generation, &lazy_reference) {
            if expected != selected {
                return Err(io.invalid("directory cursor generation differs from selection"));
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        let request = DirectoryListing {
            generation: lazy_reference.as_ref().or(request.generation),
            ..request
        };
        let continuation = request
            .after
            .filter(|token| !token.starts_with("lazy:"))
            .map(|token| cursor::open(&self.provider, request.volume, request.path, token, io))
            .transpose()?;
        if let (Some(expected), Some((generation, _, _))) = (request.generation, &continuation) {
            if expected != generation {
                return Err(io.invalid("directory cursor generation differs from selection"));
            }
        }
        let request = DirectoryListing {
            generation: continuation
                .as_ref()
                .map(|cursor| &cursor.0)
                .or(request.generation),
            ..request
        };
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
        let selected = async {
            guard().await?;
            let after = continuation.as_ref().map(|cursor| &cursor.1);
            #[cfg(not(target_arch = "wasm32"))]
            let lazy_page = if let Some(cut) = &selection.snapshot {
                Some(
                    self.lazy_directory_page(
                        cut,
                        request.volume,
                        request.path,
                        &requested.0,
                        lazy_cursor,
                        request.maximum_entries,
                        io,
                    )
                    .await?,
                )
            } else {
                if lazy_cursor.is_some() {
                    return Err(io.invalid("lazy directory cursor requires its original view"));
                }
                None
            };
            #[cfg(not(target_arch = "wasm32"))]
            let (loaded, lazy_next, physical_examined, mut positions) = match lazy_page {
                Some((page, next, custody, examined, positions)) => {
                    (Some((page, Some(custody))), next, Some(examined), positions)
                }
                None => (
                    io.sdk_if_present(
                        generation
                            .list_directory(
                                &requested.0,
                                after,
                                request.maximum_entries,
                                io,
                                io.cancellation(),
                            )
                            .await,
                    )?,
                    None,
                    None,
                    Vec::new(),
                ),
            };
            #[cfg(target_arch = "wasm32")]
            let (loaded, lazy_next, physical_examined, mut positions): (
                _,
                Option<String>,
                Option<u32>,
                Vec<DirectoryPosition>,
            ) = (
                io.sdk_if_present(
                    generation
                        .list_directory(
                            &requested.0,
                            after,
                            request.maximum_entries,
                            io,
                            io.cancellation(),
                        )
                        .await,
                )?,
                None,
                None,
                Vec::new(),
            );
            let Some((mut page, custody)) = loaded else {
                return Ok(None);
            };
            let page_bytes = raw_owned_bytes(&page)? + position_bytes(positions.capacity());
            let examined = physical_examined.unwrap_or(
                u32::try_from(page.entries.len())
                    .map_err(|_| io.invalid("directory page is too large"))?,
            );
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(custody) = custody {
                io.absorb_native_custody(custody)?;
                io.retain_owned(std::mem::size_of::<(GenerationRef, DirectoryPage)>() as u64)?;
            } else {
                io.retain_owned(page_bytes)?;
            }
            #[cfg(target_arch = "wasm32")]
            {
                let _ = custody;
                io.retain_owned(page_bytes)?;
            }
            let mut page_hold = io.guard_owned(page_bytes)?;
            guard().await?;
            io.charge(WorkCounters {
                items_examined: examined as u64,
                ..Default::default()
            })?;
            let now = super::super::access::access_now()?;
            let next = if let Some(next) = lazy_next {
                page_hold.adopt(next.capacity() as u64)?;
                Some(next)
            } else if page.has_more {
                let name = page
                    .entries
                    .last()
                    .ok_or_else(|| io.invalid("directory page made no cursor progress"))?;
                if after.is_some_and(|after| &name.name <= after) {
                    return Err(io.invalid("directory cursor did not advance"));
                }
                let (token, hold) = cursor::seal(
                    request.volume,
                    request.path,
                    generation.id(),
                    &name.name,
                    io,
                )?;
                page_hold.absorb(hold)?;
                Some(token)
            } else {
                None
            };
            let mut index = 0;
            let removed = filter_page(&mut page, parent, data_parent, now, |keep| {
                if !keep && let Some(position) = positions.get_mut(index) {
                    *position = DirectoryPosition::Removed;
                }
                index += 1;
            });
            positions.retain(|position| !matches!(position, DirectoryPosition::Removed));
            page_hold.substitute(
                page_bytes - removed + next.as_ref().map_or(0, |token| token.capacity() as u64),
            )?;
            // Do not expose a physical prefetch hint for an unauthorized successor.
            page.next_residency = None;
            #[cfg(not(target_arch = "wasm32"))]
            let reference = match &selection.snapshot {
                Some(cut) => self.private_generation_ref(cut, request.generation, io)?,
                None => bounded_io::generation_ref(&self.provider, generation.id(), Some(io))?,
            };
            #[cfg(target_arch = "wasm32")]
            let reference = bounded_io::generation_ref(&self.provider, generation.id(), Some(io))?;
            page_hold.adopt(reference.logical_owned_bytes()?)?;
            let wrapper_bytes = (std::mem::size_of::<AuthorizedDirectoryPage>()
                - std::mem::size_of::<DirectoryPage>()) as u64;
            io.retain_owned(wrapper_bytes)?;
            page_hold.adopt(wrapper_bytes)?;
            Ok::<_, Error>(Some((
                (
                    reference,
                    AuthorizedDirectoryPage {
                        page,
                        next,
                        examined,
                        positions,
                    },
                ),
                page_hold,
            )))
        }
        .await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(cut) = &selection.snapshot {
            self.validate_private_snapshot(cut, io).await?;
        }
        selection.require_current(io).await?;
        request.grant.recheck(Some(io))?;
        Ok(selected)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

fn filter_page(
    page: &mut DirectoryPage,
    parent: PreparedPath<'_>,
    data_parent: Option<PreparedPath<'_>>,
    now: u64,
    mut visited: impl FnMut(bool),
) -> u64 {
    let mut removed = 0;
    page.entries.retain(|entry| {
        let keep = visible(parent, &entry.name, now)
            && data_parent.is_none_or(|parent| visible(parent, &entry.name, now));
        visited(keep);
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
