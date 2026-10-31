use super::*;
use acyclic_fs::access::{Action, PreparedPath};

/// Immutable pagination and authority limits for a private directory read.
#[derive(Clone, Copy)]
pub struct DirectoryOptions<'a> {
    /// Exact selected generation, or the current head.
    pub generation: Option<&'a GenerationRef>,
    /// Exclusive logical-name cursor.
    pub after: Option<&'a LogicalName>,
    /// Maximum visible entries in the returned page.
    pub maximum_entries: u32,
    /// Additional caller authority ceiling.
    pub ceiling: Option<&'a crate::Capabilities>,
}

/// First authorized page of an immutable directory view and its follow cursor.
/// Continue paging with `generation` through the ordinary directory query;
/// follow changes from `publication.next` and its original preceding commit.
pub struct DirectorySnapshot {
    /// Original selected immutable generation, also used for conditional edits.
    pub generation: GenerationRef,
    /// Visible entries under the same grant and current persisted policy.
    pub page: DirectoryPage,
    /// Original producer's cursor; not a grant to follow undisclosed entries.
    pub publication: acyclic_fs::PublicationSnapshot,
}

struct DirectoryRead {
    generation: GenerationRef,
    page: DirectoryPage,
    publication: Option<acyclic_fs::PublicationSnapshot>,
}

impl DirectoryRead {
    fn into_page(self) -> (GenerationRef, DirectoryPage) {
        (self.generation, self.page)
    }
}

pub(in crate::filesystem) struct DirectoryListing<'a> {
    pub volume: &'a VolumeRef,
    pub grant: &'a ContentGrant,
    pub ceiling: Option<&'a crate::Capabilities>,
    pub path: &'a str,
    pub generation: Option<&'a GenerationRef>,
    pub after: Option<&'a LogicalName>,
    pub maximum_entries: u32,
}

fn owned_bytes(page: &DirectoryPage) -> Result<u64> {
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

fn visible(parent: PreparedPath<'_>, name: &LogicalName, root: bool, now: u64) -> bool {
    let internal = root
        && match name.encoding() {
            NameEncoding::Utf8 | NameEncoding::PosixBytes => name.as_bytes() == b".system",
            NameEncoding::WindowsUtf16Le => name.as_bytes() == b".\0s\0y\0s\0t\0e\0m\0",
        };
    !internal
        && parent
            .child_name(name)
            .is_ok_and(|path| path.decide(Action::List, now).allowed)
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Lists visible names at one selected generation under the current policy.
    pub async fn list_private_directory(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        options: crate::filesystem::DirectoryOptions<'_>,
    ) -> Result<(GenerationRef, DirectoryPage)> {
        let crate::filesystem::DirectoryOptions {
            generation: expected_generation,
            after,
            maximum_entries,
            ceiling,
        } = options;

        let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
        let (read, hold) = self
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
                false,
            )
            .await?;
        hold.into_retained()?;
        Ok(read.into_page())
    }

    /// Opens the first watched directory page without a snapshot/follow gap.
    /// An exact generation is paged through `list_private_directory` after this
    /// initial selection; it cannot be paired with a later current cursor.
    pub async fn snapshot_private_directory(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        options: crate::filesystem::DirectoryOptions<'_>,
    ) -> Result<DirectorySnapshot> {
        if options.generation.is_some() {
            return Err(Error::Invalid(
                "a watched snapshot requires an initial current selection".into(),
            ));
        }
        let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
        let (read, hold) = self
            .list_directory_page(
                DirectoryListing {
                    volume,
                    grant,
                    path,
                    generation: None,
                    after: options.after,
                    maximum_entries: options.maximum_entries,
                    ceiling: options.ceiling,
                },
                &io,
                &mut || std::future::ready(Ok(())),
                true,
            )
            .await?;
        let publication = read
            .publication
            .ok_or_else(|| io.invalid("directory snapshot lost its original publication cut"))?;
        hold.into_retained()?;
        Ok(DirectorySnapshot {
            generation: read.generation,
            page: read.page,
            publication,
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) async fn list_private_directory_guarded_with_native_work<F, Fut>(
        &self,
        request: DirectoryListing<'_>,
        work: &acyclic_fs::NativeMaterializationWork,
        mut guard: F,
    ) -> Result<((GenerationRef, DirectoryPage), acyclic_fs::NativeAllocation)>
    where
        F: FnMut() -> Fut + acyclic_stream::ProviderTask,
        Fut: Future<Output = Result<()>> + acyclic_stream::ProviderTask,
    {
        let io = PreparationIo::from_native_work(work)?;
        let (read, hold) = self
            .list_directory_page(request, &io, &mut guard, false)
            .await?;
        let retained = hold.bytes();
        hold.into_retained()?;
        Ok((read.into_page(), io.into_native_custody(retained)?))
    }

    async fn select_directory<'io, F, Fut>(
        &self,
        request: &DirectoryListing<'_>,
        io: &'io PreparationIo,
        guard: &mut F,
        capture_changes: bool,
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
        let (generation, publication) = match (capture_changes, request.generation) {
            (false, Some(expected)) => {
                (self.generation(&workspace, expected, Some(io)).await?, None)
            }
            (false, None) => (self.current_generation(&workspace, io).await?, None),
            (true, None) => {
                let snapshot = io.sdk(workspace.snapshot(io, io.cancellation()).await)?;
                (snapshot.generation, Some(snapshot.publication))
            }
            (true, Some(_)) => {
                return Err(io.invalid("an exact generation has no current follow cursor"));
            }
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
            publication,
        })
    }

    async fn list_directory_page<'io, F, Fut>(
        &self,
        request: DirectoryListing<'_>,
        io: &'io PreparationIo,
        guard: &mut F,
        capture_changes: bool,
    ) -> Result<(DirectoryRead, DecodeReservation<'io>)>
    where
        F: FnMut() -> Fut + acyclic_stream::ProviderTask,
        Fut: Future<Output = Result<()>> + acyclic_stream::ProviderTask,
    {
        let DirectorySelection {
            workspace,
            generation,
            view,
            data_access,
            publication,
        } = self
            .select_directory(&request, io, guard, capture_changes)
            .await?;
        let parent = view
            .0
            .prepare(request.path)
            .map_err(|_| io.invalid("invalid directory access path"))?;
        let data_parent = data_access
            .as_ref()
            .map(|(_, view)| view.0.prepare(request.path))
            .transpose()
            .map_err(|_| io.invalid("invalid directory access path"))?;
        bounded_io::retain(&generation, Some(io)).await?;
        guard().await?;
        let requested = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{}", request.path), work)
        })?;
        let requested_hold = io.guard_owned(requested.capacity() as u64)?;
        let requested = (requested, requested_hold);
        let mut cursor: Option<(LogicalName, DecodeReservation<'io>)> = None;
        let mut spent = WorkCounters::default();
        loop {
            guard().await?;
            let after = cursor.as_ref().map(|cursor| &cursor.0).or(request.after);
            let (mut page, custody) = io.sdk(
                generation
                    .list_directory(
                        &requested.0,
                        after,
                        request.maximum_entries,
                        io,
                        io.cancellation(),
                    )
                    .await,
            )?;
            let page_bytes = owned_bytes(&page)?;
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
            spent = spent
                .checked_add(page.work)
                .map_err(|_| io.invalid("directory work overflow"))?;
            io.charge(WorkCounters {
                items_examined: page.entries.len() as u64,
                ..Default::default()
            })?;
            let now = super::super::access::access_now()?;
            let (removed, successor) =
                filter_page(&mut page, parent, data_parent, request.path.is_empty(), now);
            page_hold.substitute(page_bytes - removed)?;
            if page.entries.is_empty() && page.has_more {
                let name = successor
                    .ok_or_else(|| io.invalid("directory page made no cursor progress"))?;
                if after.is_some_and(|after| &name <= after) {
                    return Err(io.invalid("directory cursor did not advance"));
                }
                page_hold.retain_part(name.logical_owned_bytes())?;
                let hold = io.guard_owned(name.logical_owned_bytes())?;
                drop(page);
                drop(page_hold);
                cursor = Some((name, hold));
                continue;
            }
            // Do not expose a physical prefetch hint for an unauthorized successor.
            page.next_residency = None;
            page.work = spent;
            let reference = bounded_io::generation_ref(&self.provider, generation.id(), Some(io))?;
            page_hold.adopt(reference.logical_owned_bytes()?)?;
            if publication.is_some() {
                let bytes = std::mem::size_of::<acyclic_fs::PublicationSnapshot>() as u64;
                io.retain_owned(bytes)?;
                page_hold.adopt(bytes)?;
            }
            super::super::access::require_current(&workspace, &view.0, io).await?;
            if let Some((workspace, view)) = &data_access {
                super::super::access::require_current(workspace, &view.0, io).await?;
            }
            request.grant.recheck(Some(io))?;
            return Ok((
                DirectoryRead {
                    generation: reference,
                    page,
                    publication,
                },
                page_hold,
            ));
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

fn filter_page(
    page: &mut DirectoryPage,
    parent: PreparedPath<'_>,
    data_parent: Option<PreparedPath<'_>>,
    root: bool,
    now: u64,
) -> (u64, Option<LogicalName>) {
    // Keep the physical successor by moving its name, never copying it.
    let last = page.entries.pop();
    let mut removed = 0;
    page.entries.retain(|entry| {
        let keep = visible(parent, &entry.name, root, now)
            && data_parent.is_none_or(|parent| visible(parent, &entry.name, root, now));
        if !keep {
            removed += entry.name.logical_owned_bytes();
        }
        keep
    });
    let mut successor = None;
    if let Some(last) = last {
        if visible(parent, &last.name, root, now)
            && data_parent.is_none_or(|parent| visible(parent, &last.name, root, now))
        {
            page.entries.push(last);
        } else if page.entries.is_empty() && page.has_more {
            successor = Some(last.name);
        } else {
            removed += last.name.logical_owned_bytes();
        }
    }
    (removed, successor)
}

struct DirectorySelection<'io, A: 'static, O: 'static> {
    workspace: Workspace<A, O>,
    generation: acyclic_fs::Generation<A, O>,
    view: super::super::access::CompiledAccess<'io>,
    data_access: Option<(Workspace<A, O>, super::super::access::CompiledAccess<'io>)>,
    publication: Option<acyclic_fs::PublicationSnapshot>,
}
