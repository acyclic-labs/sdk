use super::*;
use acyclic_fs::access::{Action, PreparedPath};

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
        expected_generation: Option<&GenerationRef>,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        ceiling: Option<&crate::Capabilities>,
    ) -> Result<(GenerationRef, DirectoryPage)> {
        let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
        let (page, hold) = self
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
        hold.into_retained()?;
        Ok(page)
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
        let (page, hold) = self.list_directory_page(request, &io, &mut guard).await?;
        let retained = hold.bytes();
        hold.into_retained()?;
        Ok((page, io.into_native_custody(retained)?))
    }

    async fn list_directory_page<'io, F, Fut>(
        &self,
        request: DirectoryListing<'_>,
        io: &'io PreparationIo,
        guard: &mut F,
    ) -> Result<((GenerationRef, DirectoryPage), DecodeReservation<'io>)>
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
        let reference =
            fork_preparer_io::content_workspace(&self.provider, request.volume, Some(io))?;
        let reference_hold = io.guard_owned(reference.logical_owned_bytes()?)?;
        let reference = (reference, reference_hold);
        guard().await?;
        let workspace = self.open(&reference.0, Some(io)).await?;
        guard().await?;
        let generation = match request.generation {
            Some(expected) => self.generation(&workspace, expected, Some(io)).await?,
            None => io.sdk(workspace.head(io, io.cancellation()).await)?,
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
        let parent = view
            .0
            .prepare(request.path)
            .map_err(|_| io.invalid("invalid directory access path"))?;
        fork_preparer_io::retain(&generation, Some(io)).await?;
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
            let now = super::super::access::access_now();
            // Keep the physical successor by moving its name, never copying it.
            let last = page.entries.pop();
            let mut removed = 0;
            page.entries.retain(|entry| {
                let keep = visible(parent, &entry.name, request.path.is_empty(), now);
                if !keep {
                    removed += entry.name.logical_owned_bytes();
                }
                keep
            });
            let mut successor = None;
            if let Some(last) = last {
                if visible(parent, &last.name, request.path.is_empty(), now) {
                    page.entries.push(last);
                } else if page.entries.is_empty() && page.has_more {
                    successor = Some(last.name);
                } else {
                    removed += last.name.logical_owned_bytes();
                }
            }
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
            let reference =
                fork_preparer_io::generation_ref(&self.provider, generation.id(), Some(io))?;
            page_hold.adopt(reference.logical_owned_bytes()?)?;
            super::super::access::require_current(&workspace, &view.0, io).await?;
            return Ok(((reference, page), page_hold));
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
