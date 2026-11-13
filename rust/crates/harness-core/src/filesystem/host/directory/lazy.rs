//! Frozen lazy pages use the same policy filtering and confidential envelope.
use super::*;
use acyclic_fs::{LazyDirectoryCursor, LazySnapshotRef, kernel::KernelOwnedAllocation};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(super) fn open_lazy_directory_cursor(
        &self,
        request: &DirectoryListing<'_>,
        io: &PreparationIo,
    ) -> Result<Option<LazyDirectoryCursor>> {
        let Some(token) = request.after.and_then(|token| token.strip_prefix("lazy:")) else {
            return Ok(None);
        };
        let view = self
            .private_view
            .as_ref()
            .ok_or_else(|| io.invalid("lazy directory cursor requires its original view"))?;
        let work = io
            .original_native_work()
            .ok_or_else(|| io.invalid("lazy directory requires original native work"))?;
        let plain = cursor::open_payload(request.volume, request.path, token, io)?;
        let config = view.config();
        Ok(Some(LazyDirectoryCursor::decode(
            &plain.0,
            config.profile,
            config.limits,
            work,
        )?))
    }

    pub(super) async fn lazy_directory_page(
        &self,
        cut: &LazySnapshotRef,
        volume: &VolumeRef,
        path: &str,
        requested: &str,
        cursor: Option<LazyDirectoryCursor>,
        maximum: u32,
        caller: &PreparationIo,
    ) -> Result<(
        DirectoryPage,
        Option<String>,
        acyclic_fs::NativeAllocation,
        u32,
    )> {
        let view = self
            .private_view
            .as_ref()
            .filter(|view| view.workspace_id() == cut.workspace_id)
            .ok_or_else(|| caller.invalid("private source binding is unavailable"))?;
        let work = caller
            .original_native_work()
            .ok_or_else(|| caller.invalid("lazy directory requires original native work"))?;
        let io = PreparationIo::from_native_work(work)?;
        let mut ctx = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
            work,
            cancellation: caller.cancellation(),
        };
        let (mut page, allocation) = view
            .directory(cut, requested, cursor, maximum, &mut ctx, &io)?
            .await?;
        let examined = page.examined;
        let next = if let Some(cursor) = page.next.take() {
            let encoded = cursor.encode(work)?;
            cursor.retire(&mut ctx)?;
            Some(encoded)
        } else {
            None
        };
        drop(ctx);
        let KernelOwnedAllocation::NativeOriginal(allocation) = allocation else {
            return Err(io.invalid("lazy page belongs to another allocation ledger"));
        };
        io.absorb_native_custody(allocation)?;
        let next = if let Some((bytes, allocation)) = next {
            io.absorb_native_custody(allocation)?;
            // The encoded primitive cursor already contains its complete cut.
            // The outer envelope hides denied names and binds volume and path.
            let sealed = cursor::seal_payload(volume, path, &bytes, &io)?;
            let bytes_len = bytes.capacity() as u64;
            crate::executor::stock_retire(bytes, bytes_len, Some(&io))?;
            let token = crate::executor::with_history_io(Some(&io), |work| {
                crate::executor::preparation_format(format_args!("lazy:{}", sealed.0), work)
            })?;
            drop(sealed);
            Some(token)
        } else {
            None
        };
        let mut entries = Vec::new();
        let old_bytes = page.entries.capacity() as u64
            * std::mem::size_of::<acyclic_fs::LazyDirectoryEntry>() as u64;
        for entry in page.entries.drain(..) {
            use acyclic_fs::demand::SourceNodeKind as K;
            let kind = match entry.kind {
                K::Directory => acyclic_fs::kernel::FileKind::Directory,
                K::RegularFile => acyclic_fs::kernel::FileKind::Regular,
                K::SymbolicLink => acyclic_fs::kernel::FileKind::SymbolicLink,
                K::Fifo => acyclic_fs::kernel::FileKind::Fifo,
                K::Socket => acyclic_fs::kernel::FileKind::Socket,
                K::CharacterDevice => acyclic_fs::kernel::FileKind::CharacterDevice,
                K::BlockDevice => acyclic_fs::kernel::FileKind::BlockDevice,
                K::Unsupported => {
                    return Err(io.invalid("source directory kind cannot be represented"));
                }
            };
            crate::executor::stock_vec_push(
                &mut entries,
                acyclic_fs::kernel::TreeEntry {
                    name: entry.name,
                    file_id: entry.file_id,
                    kind,
                },
                Some(&io),
            )?;
        }
        crate::executor::stock_retire(page.entries, old_bytes, Some(&io))?;
        let page = DirectoryPage {
            entries,
            has_more: next.is_some(),
            next_residency: None,
        };
        let retained = raw_owned_bytes(&page)?
            - std::mem::size_of::<(GenerationRef, DirectoryPage)>() as u64
            + next.as_ref().map_or(0, |token| token.capacity() as u64);
        Ok((page, next, io.into_native_custody(retained)?, examined))
    }
}
