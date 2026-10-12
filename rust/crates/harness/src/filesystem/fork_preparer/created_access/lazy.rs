//! Reads from the original immutable lazy Root snapshot.
use super::*;

#[cfg(not(target_arch = "wasm32"))]
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    CreatedFilesystemAccess<A, O>
{
    fn authorize_path(
        &self,
        volume: &VolumeRef,
        prefix: &str,
        path: &str,
        io: &PreparationIo,
    ) -> Result<()> {
        let root = self
            .root()
            .expect("lazy content belongs to an original Root");
        io.check_cancellation()?;
        if volume != &root.request.volume {
            return Err(io.invalid("Root source path names another namespace"));
        }
        for path in [path, prefix] {
            if !path.is_empty() {
                crate::executor::with_history_io(Some(io), |work| {
                    crate::conversation::validate_content_path(path, work)
                })?;
            }
            if crate::conversation::is_internal_path(path) {
                return Err(io.invalid("Root source path escapes its signed read boundary"));
            }
        }
        if !prefix.is_empty()
            && path != prefix
            && !path
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with('/'))
        {
            return Err(io.invalid("Root source path escapes its signed read boundary"));
        }
        let grants = self.reader().scope.capabilities();
        let directory = io.contains_capability(grants, &(volume, prefix), "directory:read:")?;
        let entire = io.contains_capability(grants, volume, "volume:read:")?;
        if self.reader().scope.agent().is_none() || (!directory && !entire) {
            return Err(io.invalid("Root source path has no original signed read authority"));
        }
        Ok(())
    }

    async fn read_snapshot_bytes(
        &self,
        snapshot: acyclic_fs::LazySnapshotRef,
        path: &str,
        maximum: u64,
        io: &PreparationIo,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let root = self
            .root()
            .expect("lazy content belongs to an original Root");
        let source = root.native_source.as_ref().ok_or_else(|| {
            bounded_io::failure(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "Root source lacks its retained lazy backing",
            )
        })?;
        let hold;
        let absolute = crate::contract::preparation_format(format_args!("/{path}"), Some(io))?;
        hold = io.guard_owned(absolute.capacity() as u64)?;
        let bytes = io
            .finish_future(source.backing.read(snapshot, &absolute, maximum, io)?)
            .await;
        drop(absolute);
        drop(hold);
        bytes
    }

    pub(super) async fn read_snapshot_file(
        &self,
        file: &FileRef,
        maximum: u64,
        io: &PreparationIo,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        let root = self
            .root()
            .expect("lazy content belongs to an original Root");
        let Some(snapshot) = root.snapshot() else {
            return Ok(None);
        };
        if root.source_generation(Some(file), Some(io))?.is_none() {
            return Ok(None);
        }
        self.authorize_path(file.volume(), "", file.path(), io)?;
        self.authenticate(file.volume(), Some(io)).await?;
        let bytes = self
            .read_snapshot_bytes(
                snapshot,
                file.path(),
                maximum.min(self.reader().maximum_bytes),
                io,
            )
            .await?;
        crate::executor::with_history_io(Some(io), |work| {
            file.descriptor().verify(bytes.as_ref(), work)
        })?;
        Ok(Some(bytes))
    }

    pub(super) async fn read_snapshot_path(
        &self,
        volume: &VolumeRef,
        prefix: &str,
        path: &str,
        expected: Option<&GenerationRef>,
        maximum: u64,
        io: Option<&PreparationIo>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        let root = self
            .root()
            .expect("lazy content belongs to an original Root");
        let owned;
        let io = match io {
            Some(io) => io,
            None => {
                owned = PreparationIo::for_unmeasured(root._custody.work().cancellation())?;
                &owned
            }
        };
        let snapshot = root
            .snapshot()
            .ok_or_else(|| io.invalid("Root path has no original lazy snapshot"))?;
        if expected.is_some_and(|generation| generation != &root.request.generation)
            || maximum == 0
            || maximum > self.reader().maximum_bytes
        {
            return Err(
                io.invalid("Root path differs from its immutable source selection or limit")
            );
        }
        crate::executor::with_history_io(Some(io), |work| {
            crate::conversation::validate_content_path(path, work)
        })?;
        self.authorize_path(volume, prefix, path, io)?;
        self.authenticate(volume, Some(io)).await?;
        let bytes = self
            .read_snapshot_bytes(snapshot, path, maximum, io)
            .await?;
        let header = if io.original_native_work().is_some() {
            std::mem::size_of::<crate::conversation::FileDescriptor>() as u64
        } else {
            0
        };
        io.retain_owned(header)?;
        let descriptor = crate::executor::with_history_io(Some(io), |work| {
            crate::conversation::FileDescriptor::from_bytes(
                &bytes,
                "application/octet-stream",
                work,
            )
        })?;
        io.charge(WorkCounters {
            bytes_encoded: 64,
            bytes_copied: 64,
            ..Default::default()
        })?;
        let version = blake3::Hash::from_bytes(snapshot.id.into_bytes()).to_hex();
        if io.original_native_work().is_some() {
            io.retain_owned(std::mem::size_of::<FileRef>() as u64)?;
        }
        let file = crate::executor::with_history_io(Some(io), |work| {
            use crate::store::history::HistoryIo as _;
            let volume = volume.copy(work)?;
            let path = io.history_io().clone_text(path)?;
            let version = io.history_io().clone_text(version.as_str())?;
            let display = io
                .history_io()
                .clone_text(path.rsplit('/').next().unwrap_or(&path))?;
            FileRef::new(volume, path, version, descriptor, display, work)
        })?;
        io.release(header)?;
        Ok((file, bytes))
    }

    pub(super) async fn list_snapshot_directory(
        &self,
        volume: &VolumeRef,
        prefix: &str,
        path: &str,
        expected: Option<&GenerationRef>,
        after: Option<&str>,
        maximum: u32,
        io: Option<&PreparationIo>,
    ) -> Result<PrivateDirectoryPage> {
        let root = self
            .root()
            .expect("lazy content belongs to an original Root");
        let owned;
        let io = match io {
            Some(io) => io,
            None => {
                owned = PreparationIo::for_unmeasured(root._custody.work().cancellation())?;
                &owned
            }
        };
        io.check_cancellation()?;
        if maximum == 0 || expected.is_some_and(|generation| generation != &root.request.generation)
        {
            return Err(
                io.invalid("Root directory page differs from its immutable selection or bound")
            );
        }
        self.authorize_path(volume, prefix, path, io)?;
        self.authenticate(volume, Some(io)).await?;
        let snapshot = root
            .snapshot()
            .ok_or_else(|| io.invalid("Root directory has no original lazy snapshot"))?;
        let source = root
            .native_source
            .as_ref()
            .ok_or_else(|| io.invalid("Root directory lacks its retained lazy backing"))?;
        let absolute_hold;
        let absolute = crate::contract::preparation_format(format_args!("/{path}"), Some(io))?;
        absolute_hold = io.guard_owned(absolute.capacity() as u64)?;
        let bound = (maximum as usize)
            .checked_add(1)
            .ok_or_else(|| io.invalid("Root directory page bound overflow"))?;
        let mut entries: Vec<crate::conversation::PrivateDirectoryEntry> = Vec::new();
        let mut cursor = None;
        loop {
            let page = io.receive_content(
                source
                    .backing
                    .list(
                        snapshot,
                        &absolute,
                        cursor,
                        128,
                        io.remaining()?,
                        io.cancellation(),
                    )
                    .await,
            )?;
            for entry in page.entries {
                let kind = match entry.kind {
                    acyclic_fs::demand::SourceNodeKind::RegularFile => {
                        crate::conversation::PrivateDirectoryEntryKind::File
                    }
                    acyclic_fs::demand::SourceNodeKind::Directory => {
                        crate::conversation::PrivateDirectoryEntryKind::Directory
                    }
                    _ => continue,
                };
                let name = entry.name.unicode_text().ok_or_else(|| {
                    io.invalid("Root directory name is not representable losslessly")
                })?;
                if after.is_some_and(|after| name.as_ref() <= after) {
                    continue;
                }
                let position = match entries
                    .binary_search_by(|entry| entry.name.as_str().cmp(name.as_ref()))
                {
                    Ok(_) => continue,
                    Err(position) => position,
                };
                if position >= bound {
                    continue;
                }
                if entries.len() == bound {
                    let removed = entries.pop().expect("full directory page").name;
                    let bytes = removed.capacity() as u64;
                    drop(removed);
                    io.release(bytes)?;
                }
                if entries.len() == entries.capacity() {
                    let capacity = entries.capacity().saturating_mul(2).max(4).min(bound);
                    bounded_io::reserve(&mut entries, capacity, Some(io))?;
                }
                entries.insert(
                    position,
                    crate::conversation::PrivateDirectoryEntry {
                        name: bounded_io::clone_text(name.as_ref(), Some(io))?,
                        kind,
                    },
                );
            }
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        drop(absolute);
        drop(absolute_hold);
        let has_more = entries.len() > maximum as usize;
        if has_more {
            let removed = entries.pop().expect("directory page lookahead").name;
            let bytes = removed.capacity() as u64;
            drop(removed);
            io.release(bytes)?;
        }
        let page = PrivateDirectoryPage {
            generation: root.request.generation.clone_with_io(io)?,
            entries,
            has_more,
        };
        page.validate()?;
        Ok(page)
    }
}
