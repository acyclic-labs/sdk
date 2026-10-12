use super::*;
use crate::conversation::{FileDescriptor, Limits, PrivateDirectoryEntry, PrivateDirectoryEntryKind};
use crate::durable_host::TaskJournalOwner;
use crate::filesystem::{FilesystemHost, PreparationIo};
use crate::filesystem::fork_preparer::bounded_io;
use crate::source_authorization::{ProjectSourceAuthorization, ProjectSourceReadHandle};
use acyclic_fs::{Generation, LazySnapshotRef, WorkCounters};
use acyclic_fs::kernel::{FileKind, LogicalName, NameEncoding};

/// One transient reader over an existing owner and immutable allocation.
pub(super) struct ProjectSourceReader<P, A, O> {
    host: Arc<FilesystemHost<A, O>>,
    owner: Arc<TaskJournalOwner<P>>,
    handle: ProjectSourceReadHandle,
    generation: Generation<A, O>,
    limits: Limits,
    pub(super) io: PreparationIo,
    pub(super) serial: tokio::sync::Mutex<()>,
}

impl<P, A, O> ProjectSourceReader<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    pub(super) async fn open(
        host: Arc<FilesystemHost<A, O>>, owner: Arc<TaskJournalOwner<P>>,
        handle: ProjectSourceReadHandle, limits: Limits, io: PreparationIo,
    ) -> Result<Self> {
        limits.validate(&mut None)?;
        let authorization = handle.authorization();
        host.require_private_directory(&handle.authorization().project, handle.grant(), "", Some(&io))?;
        let reference = bounded_io::content_workspace(&host.provider, &authorization.project, Some(&io))?;
        Self::verify_owner(&owner, &io).await?;
        let workspace = bounded_io::open(&host, &reference, &io).await?;
        Self::verify_owner(&owner, &io).await?;
        io.retain_owned(workspace.name().as_str().len() as u64)?;
        let generation = bounded_io::generation(&host, &workspace, &authorization.generation, &io).await?;
        Self::verify_owner(&owner, &io).await?;
        Ok(Self { host, owner, handle, generation, limits, io,
            serial: tokio::sync::Mutex::new(()) })
    }

    pub(super) fn authorization(&self) -> &ProjectSourceAuthorization { self.handle.authorization() }
    pub(super) fn work(&self) -> WorkCounters { self.io.receipt() }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn require_native_project_selection_with_native_work(
        &self, source: TaskId, project: &VolumeRef, generation: &GenerationRef,
        frozen: Option<LazySnapshotRef>, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let original = self.io.original_native_work().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native source loan requires an originally funded selected reader",
        })?;
        work.remaining()?;
        if !original.shares_ledger(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native source selection belongs to another original ledger" });
        }
        original.remaining()?;
        self.io.cancellation().check()
            .map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let selected = self.authorization();
        self.handle.require_selection(selected.operation_id, source, project, generation, selected.native_snapshot.as_ref(), Some(work))?;
        self.owner.verify_project_source_read(&self.handle, Some(work)).await?;
        let io = PreparationIo::from_native_work(work)?;
        self.host.require_private_directory(&self.handle.authorization().project, self.handle.grant(), "", Some(&io))?;
        if let Some(frozen) = frozen {
            if frozen.workspace_id != self.generation.workspace_id() || frozen.authored_generation != self.generation.id() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native source snapshot differs from the protected immutable project" });
            }
            if let Some(native) = &selected.native_snapshot {
                let mut history = io.history_io();
                let mut observed: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
                let expected = crate::contract::canonical_json_digest(
                    native, selected.maximum_bytes, &mut observed,
                )?;
                let actual = crate::contract::canonical_json_digest(
                    &frozen, selected.maximum_bytes, &mut observed,
                )?;
                if actual != expected {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                        reason: "native source snapshot changed its sealed operation selection" });
                }
            }
        }
        self.owner.verify_project_source_read(&self.handle, Some(work)).await?;
        original.remaining()?;
        self.io.cancellation().check()
            .map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        work.remaining()?;
        Ok(())
    }

    async fn verify_owner(owner: &TaskJournalOwner<P>, io: &PreparationIo) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            let cancellation = owner.invocation_stop.as_ref().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "selected source has no original invocation token",
            })?;
            if cancellation.is_cancelled() || io.cancellation().is_cancelled() {
                return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
            }
            work.remaining()?;
            owner.verify(false, &mut Some(&mut crate::store::HistoryWork::new(
                        crate::filesystem::PreparationIo::from_native_work(work)?,
                    ))).await?;
            if cancellation.is_cancelled() || io.cancellation().is_cancelled() {
                return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
            }
            return Ok(());
        }
        io.cancellation().check().map_err(|_| Error::Unauthorized("selected source invocation cancelled".into()))?;
        owner.verify(false, &mut None).await?;
        io.cancellation().check().map_err(|_| Error::Unauthorized("selected source invocation cancelled".into()))
    }

    fn path(&self, volume: &VolumeRef, prefix: &str, path: &str, generation: &GenerationRef) -> Result<()> {
        let selected = self.authorization();
        if volume != &selected.project || generation != &selected.generation {
            return Err(Error::Unauthorized("selected reader names another project or generation".into()));
        }
        if prefix.len() > self.limits.path_bytes || path.len() > self.limits.path_bytes {
            return Err(Error::Invalid("selected path exceeds original caller limits".into()));
        }
        self.host.require_private_directory(volume, self.handle.grant(), prefix, Some(&self.io))?;
        self.host.require_private_directory(volume, self.handle.grant(), path, Some(&self.io))?;
        if !prefix.is_empty() && path != prefix
            && !path.strip_prefix(prefix).is_some_and(|rest| rest.starts_with('/')) {
            return Err(Error::Unauthorized("selected path leaves its requested directory boundary".into()));
        }
        Ok(())
    }

    fn file(&self, file: &FileRef) -> Result<()> {
        crate::executor::with_history_io(Some(&self.io), |work| self.limits.validate_file(file, work))?;
        self.handle.grant().require_file_read(file, Some(&self.io))?;
        self.path(file.volume(), "", file.path(), &self.authorization().generation)?;
        let mut generation = [0_u8; 32];
        hex::decode_to_slice(file.version(), &mut generation)
            .map_err(|_| Error::Unauthorized("selected file has no exact immutable generation".into()))?;
        if generation.as_slice() != self.authorization().generation.as_resource().key() {
            return Err(Error::Unauthorized("selected file names another generation".into()));
        }
        Ok(())
    }

    async fn file_bytes(&self, file: &FileRef) -> Result<acyclic_stream::OwnedBytes> {
        Self::verify_owner(&self.owner, &self.io).await?;
        let result = self.host.read_pinned_io(file, self.limits.file_bytes, Some(&self.io)).await;
        Self::verify_owner(&self.owner, &self.io).await?;
        result
    }

    pub(super) async fn read_file(&self, file: &FileRef) -> Result<acyclic_stream::OwnedBytes> {
        self.file(file)?;
        Self::verify_owner(&self.owner, &self.io).await?;
        let _serial = self.serial.lock().await;
        self.file_bytes(file).await
    }

    pub(super) async fn diff(&self, before: &FileRef, after: &FileRef) -> Result<bool> {
        self.file(before)?;
        self.file(after)?;
        Self::verify_owner(&self.owner, &self.io).await?;
        let _serial = self.serial.lock().await;
        let _before_hold;
        let before_bytes = self.file_bytes(before).await?;
        _before_hold = self.io.guard_bytes(&before_bytes)?;
        let after_bytes = self.file_bytes(after).await?;
        Ok(before_bytes != after_bytes)
    }

    pub(super) async fn read_path(&self, volume: &VolumeRef, prefix: &str, path: &str,
        generation: &GenerationRef) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        self.path(volume, prefix, path, generation)?;
        crate::conversation::validate_content_path(path, &mut None)?;
        if self.authorization().native_snapshot.is_some() {
            return Err(Error::Invalid("native selection requires its actual lazy snapshot reader".into()));
        }
        Self::verify_owner(&self.owner, &self.io).await?;
        let _serial = self.serial.lock().await;
        self.io.retain_owned(path.len() as u64 + 1)?;
        self.io.allocation_attempt(path.len() as u64 + 1)?;
        let requested = format!("/{path}");
        let _requested_hold = self.io.guard_owned(requested.len() as u64)?;
        Self::verify_owner(&self.owner, &self.io).await?;
        let result = self.generation.read(&requested, self.limits.file_bytes,
            self.io.remaining()?, self.io.cancellation()).await;
        let bytes = self.io.receive(result);
        Self::verify_owner(&self.owner, &self.io).await?;
        let bytes = bytes?;
        self.io.retain_owned(bytes.retained_capacity().unwrap_or(0) as u64)?;
        let metadata_path = crate::filesystem::content_metadata_path_io(path, true, Some(&self.io))?;
        let _metadata_path_hold = self.io.guard_owned(metadata_path.len() as u64)?;
        Self::verify_owner(&self.owner, &self.io).await?;
        let metadata = self.generation.read(&metadata_path, self.io.remaining()?.source_bytes_read,
            self.io.remaining()?, self.io.cancellation()).await;
        let metadata = self.io.receive(metadata);
        Self::verify_owner(&self.owner, &self.io).await?;
        let display_name = path.rsplit('/').next().unwrap_or(path);
        let staged = match metadata {
            Ok(metadata) => Some(self.io.decode_json::<FileRef>(&metadata)?),
            Err(Error::NotFound(_)) => None,
            Err(error) => return Err(error),
        };
        self.io.charge(WorkCounters { bytes_hashed: bytes.len() as u64, ..WorkCounters::default() })?;
        let raw_descriptor;
        let _descriptor_hold;
        let (descriptor, display_name) = if let Some(staged) = &staged {
            if staged.volume() != volume || staged.path() != path || staged.version() != "pending" {
                return Err(Error::Storage("selected file metadata does not match immutable data".into()));
            }
            staged.descriptor().verify(&bytes, &mut None)?;
            _descriptor_hold = None;
            (staged.descriptor(), staged.display_name())
        } else {
            const MEDIA_TYPE: &str = "application/octet-stream";
            self.io.retain_owned(MEDIA_TYPE.len() as u64)?;
            self.io.allocation_attempt(MEDIA_TYPE.len() as u64)?;
            _descriptor_hold = Some(self.io.guard_owned(MEDIA_TYPE.len() as u64)?);
            raw_descriptor = FileDescriptor::from_bytes(&bytes, MEDIA_TYPE, &mut None)?;
            (&raw_descriptor, display_name)
        };
        self.io.retain_owned(64)?;
        self.io.allocation_attempt(64)?;
        let version = hex::encode(generation.as_resource().key());
        let _version_hold = self.io.guard_owned(version.len() as u64)?;
        let reference = crate::executor::with_history_io(Some(&self.io), |work| FileRef::new(
            volume.copy(work)?, stock_string_copy(path, Some(&self.io))?,
            stock_string_copy(&version, Some(&self.io))?,
            FileDescriptor::new(*descriptor.sha256(), descriptor.byte_length(),
                stock_string_copy(descriptor.media_type(), Some(&self.io))?)?,
            stock_string_copy(display_name, Some(&self.io))?, work))?;
        self.file(&reference)?;
        Ok((reference, bytes))
    }

    pub(super) async fn list(&self, volume: &VolumeRef, prefix: &str, path: &str,
        generation: &GenerationRef, after: Option<&str>, maximum: u32) -> Result<PrivateDirectoryPage> {
        self.path(volume, prefix, path, generation)?;
        if self.authorization().native_snapshot.is_some() {
            return Err(Error::Invalid("native selection requires its actual lazy directory reader".into()));
        }
        if maximum == 0 { return Err(Error::Invalid("selected directory page must be finite and positive".into())); }
        Self::verify_owner(&self.owner, &self.io).await?;
        let _serial = self.serial.lock().await;
        let after = after.map(|name| {
            if name.len() > self.limits.path_bytes { return Err(Error::Invalid("selected cursor exceeds original path limits".into())); }
            self.io.retain_owned(name.len() as u64)?;
            self.io.allocation_attempt(name.len() as u64)?;
            LogicalName::new(NameEncoding::Utf8, name.as_bytes().to_vec(), u32::MAX)
                .map_err(|error| Error::Invalid(error.to_string()))
        }).transpose()?;
        self.io.retain_owned(path.len() as u64 + 1)?;
        self.io.allocation_attempt(path.len() as u64 + 1)?;
        let requested = if path.is_empty() { "/".to_owned() } else { format!("/{path}") };
        let _requested_hold = self.io.guard_owned(requested.len() as u64)?;
        let engine_maximum = maximum.checked_add(u32::from(path.is_empty()))
            .ok_or_else(|| Error::Invalid("selected root page limit overflows".into()))?;
        Self::verify_owner(&self.owner, &self.io).await?;
        let result = self.generation.list_directory(&requested, after.as_ref(), engine_maximum,
            self.io.remaining()?, self.io.cancellation()).await;
        let page = self.io.receive(result).map(|(page, _)| page);
        Self::verify_owner(&self.owner, &self.io).await?;
        let page = page?;
        let slots = (page.entries.len() as u64).checked_mul(std::mem::size_of::<PrivateDirectoryEntry>() as u64)
            .ok_or_else(|| Error::Invalid("selected directory ownership overflows".into()))?;
        self.io.retain_owned(slots)?;
        self.io.allocation_attempt(slots)?;
        let mut entries = Vec::with_capacity(page.entries.len());
        let mut has_more = page.has_more;
        for entry in page.entries {
            if entry.name.encoding() != NameEncoding::Utf8 {
                return Err(Error::Unsupported("portable source directory requires UTF-8 names; use the native reader".into()));
            }
            let name = std::str::from_utf8(entry.name.as_bytes())
                .map_err(|_| Error::Storage("selected portable directory name is invalid UTF-8".into()))?;
            if path.is_empty() && name == ".system" { continue; }
            if entries.len() == maximum as usize { has_more = true; break; }
            self.io.retain_owned(name.len() as u64)?;
            self.io.allocation_attempt(name.len() as u64)?;
            self.io.charge(WorkCounters { bytes_copied: name.len() as u64, ..WorkCounters::default() })?;
            let kind = match entry.kind {
                FileKind::Regular => PrivateDirectoryEntryKind::File,
                FileKind::Directory => PrivateDirectoryEntryKind::Directory,
                _ => return Err(Error::Unsupported("selected directory contains a non-file entry".into())),
            };
            entries.push(PrivateDirectoryEntry { name: name.to_owned(), kind });
        }
        let result = PrivateDirectoryPage { generation: generation.clone(), entries, has_more };
        result.validate()?;
        Ok(result)
    }

}

use crate::executor::{stock_string_copy};
