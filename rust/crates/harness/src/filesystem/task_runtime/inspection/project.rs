use super::*;
use crate::conversation::{FileDescriptor, Limits, PrivateDirectoryEntry, PrivateDirectoryEntryKind};
use crate::durable_host::TaskJournalOwner;
use crate::filesystem::{FilesystemHost, PreparationIo};
use crate::filesystem::fork_preparer::bounded_io;
use crate::source_authorization::{ProjectSourceAuthorization, ProjectSourceReadHandle};
use acyclic_fs::{Generation, LazySnapshotRef, LazyWorkspace, LazyWorkspaceStore, OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use acyclic_fs::demand::DemandSource;
use acyclic_fs::kernel::{FileKind, LogicalName, NameEncoding};
use serde::Deserialize;

/// One transient reader over an existing owner and immutable allocation. It is
/// neither a task/source registry nor an executable foreign task context.
pub(super) struct ProjectSourceReader<P, A, O> {
    host: Arc<FilesystemHost<A, O>>,
    owner: Arc<TaskJournalOwner<P>>,
    handle: ProjectSourceReadHandle,
    generation: Generation<A, O>,
    workspace_id: acyclic_fs::WorkspaceId,
    limits: Limits,
    pub(super) io: PreparationIo,
    // Serialize use of the same finite allowance, rather than racing snapshots
    // of its remaining budget in simultaneous SDK calls.
    pub(super) serial: tokio::sync::Mutex<()>,
}

impl<P, A, O> ProjectSourceReader<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    pub(super) async fn open(
        host: Arc<FilesystemHost<A, O>>, owner: Arc<TaskJournalOwner<P>>,
        handle: ProjectSourceReadHandle, limits: Limits, budget: WorkBudget,
    ) -> Result<Self> {
        let cancellation = owner.invocation_cancellation().ok_or_else(|| {
            Error::Unauthorized("selected source has no original invocation token".into())
        })?;
        let io = PreparationIo::from_budget(budget, cancellation)?;
        Self::open_with_io(host, owner, handle, limits, io).await
    }

    pub(super) async fn open_with_io(
        host: Arc<FilesystemHost<A, O>>, owner: Arc<TaskJournalOwner<P>>,
        handle: ProjectSourceReadHandle, limits: Limits, io: PreparationIo,
    ) -> Result<Self> {
        limits.validate()?;
        let authorization = handle.authorization();
        Self::require_directory_with_io(&host, &handle, "", &io)?;
        let reference = bounded_io::content_workspace(&host.provider, &authorization.project, Some(&io))?;
        Self::verify_owner(&owner, &io).await?;
        let workspace = bounded_io::open(&host, &reference, &io).await?;
        Self::verify_owner(&owner, &io).await?;
        // Both handles share the opened workspace name payload.
        io.retain_owned(workspace.name().as_str().len() as u64)?;
        let generation = bounded_io::generation(&host, &workspace, &authorization.generation, &io).await?;
        Self::verify_owner(&owner, &io).await?;
        Ok(Self { host, owner, handle, generation, workspace_id: workspace.id(), limits, io,
            serial: tokio::sync::Mutex::new(()) })
    }

    fn require_directory_with_io(
        host: &FilesystemHost<A, O>, handle: &ProjectSourceReadHandle,
        path: &str, io: &PreparationIo,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            work.remaining()?;
            let project = &handle.authorization().project;
            if project.provider() != &host.provider {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "private directory belongs to another provider or class",
                });
            }
            if !path.is_empty() {
                crate::conversation::validate_content_path_with_native_work(path, work)?;
            }
            return handle.grant().require_directory_path_with_native_work(project, path, work);
        }
        host.require_private_directory(&handle.authorization().project, handle.grant(), path)
    }

    pub(super) fn authorization(&self) -> &ProjectSourceAuthorization { self.handle.authorization() }
    pub(super) fn work(&self) -> WorkCounters { self.io.receipt() }

    pub(super) async fn guard(&self) -> Result<()> {
        Self::verify_owner(&self.owner, &self.io).await
    }

    async fn verify_owner(owner: &TaskJournalOwner<P>, io: &PreparationIo) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            let cancellation = owner.invocation_cancellation().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "selected source has no original invocation token",
            })?;
            if cancellation.is_cancelled() || io.cancellation.is_cancelled() {
                return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
            }
            work.remaining()?;
            owner.verify_with_native_work(false, work).await?;
            if cancellation.is_cancelled() || io.cancellation.is_cancelled() {
                return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
            }
            return Ok(());
        }
        io.cancellation.check().map_err(|_| Error::Unauthorized("selected source invocation cancelled".into()))?;
        owner.verify(false).await?;
        io.cancellation.check().map_err(|_| Error::Unauthorized("selected source invocation cancelled".into()))
    }

    fn path(&self, volume: &VolumeRef, prefix: &str, path: &str, generation: &GenerationRef) -> Result<()> {
        let selected = self.authorization();
        if volume != &selected.project || generation != &selected.generation {
            return Err(Error::Unauthorized("selected reader names another project or generation".into()));
        }
        if prefix.len() > self.limits.path_bytes || path.len() > self.limits.path_bytes {
            return Err(Error::Invalid("selected path exceeds original caller limits".into()));
        }
        self.host.require_private_directory(volume, self.handle.grant(), prefix)?;
        self.host.require_private_directory(volume, self.handle.grant(), path)?;
        if !prefix.is_empty() && path != prefix
            && !path.strip_prefix(prefix).is_some_and(|rest| rest.starts_with('/')) {
            return Err(Error::Unauthorized("selected path leaves its requested directory boundary".into()));
        }
        Ok(())
    }

    fn file(&self, file: &FileRef) -> Result<()> {
        self.limits.validate_file(file)?;
        self.handle.grant().require_file_read(file)?;
        self.path(file.volume(), "", file.path(), &self.authorization().generation)?;
        let mut generation = [0_u8; 32];
        hex::decode_to_slice(file.version(), &mut generation)
            .map_err(|_| Error::Unauthorized("selected file has no exact immutable generation".into()))?;
        if generation.as_slice() != self.authorization().generation.as_resource().key() {
            return Err(Error::Unauthorized("selected file names another generation".into()));
        }
        Ok(())
    }

    async fn file_bytes(&self, file: &FileRef) -> Result<Vec<u8>> {
        self.file(file)?;
        self.guard().await?;
        let result = self.host.read_content_measured(file, self.handle.grant(), self.limits.file_bytes,
            self.io.remaining()?, &self.io.cancellation).await;
        // Charge real backend work even if the original owner became stale.
        let result = self.io.receive_content(result);
        self.guard().await?;
        result
    }

    pub(super) async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>> {
        self.file(file)?;
        self.guard().await?;
        let _serial = self.serial.lock().await;
        self.file_bytes(file).await
    }

    pub(super) async fn diff(&self, before: &FileRef, after: &FileRef) -> Result<bool> {
        self.file(before)?;
        self.file(after)?;
        self.guard().await?;
        let _serial = self.serial.lock().await;
        let before_bytes = self.file_bytes(before).await?;
        self.io.retain_owned(before_bytes.len() as u64)?;
        let _before_hold = self.io.guard_owned(before_bytes.len() as u64)?;
        let after_bytes = self.file_bytes(after).await?;
        Ok(before_bytes != after_bytes)
    }

    pub(super) async fn read_path(&self, volume: &VolumeRef, prefix: &str, path: &str,
        generation: &GenerationRef) -> Result<(FileRef, Vec<u8>)> {
        self.path(volume, prefix, path, generation)?;
        crate::conversation::validate_content_path(path)?;
        if self.authorization().native_snapshot.is_some() {
            return Err(Error::Invalid("native selection requires its actual lazy snapshot reader".into()));
        }
        self.guard().await?;
        let _serial = self.serial.lock().await;
        self.io.claim(path.len() as u64 + 1)?;
        self.io.allocation_attempt(path.len() as u64 + 1)?;
        let requested = format!("/{path}");
        let _requested_hold = self.io.guard_owned(requested.len() as u64)?;
        self.guard().await?;
        let result = self.generation.read_measured(&requested, self.limits.file_bytes,
            self.io.remaining()?, &self.io.cancellation).await;
        let bytes = self.io.receive(result);
        self.guard().await?;
        let bytes = bytes?;
        self.io.retain_owned(bytes.len() as u64)?;
        let _bytes_hold = self.io.guard_owned(bytes.len() as u64)?;
        let metadata_path = bounded_io::content_metadata_path(path, &self.io)?;
        let _metadata_path_hold = self.io.guard_owned(metadata_path.len() as u64)?;
        self.guard().await?;
        let metadata = self.generation.read_measured(&metadata_path, self.io.remaining()?.source_bytes_read,
            self.io.remaining()?, &self.io.cancellation).await;
        let metadata = self.io.receive(metadata);
        self.guard().await?;
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
            staged.descriptor().verify(&bytes)?;
            _descriptor_hold = None;
            (staged.descriptor(), staged.display_name())
        } else {
            const MEDIA_TYPE: &str = "application/octet-stream";
            self.io.claim(MEDIA_TYPE.len() as u64)?;
            self.io.allocation_attempt(MEDIA_TYPE.len() as u64)?;
            _descriptor_hold = Some(self.io.guard_owned(MEDIA_TYPE.len() as u64)?);
            raw_descriptor = FileDescriptor::from_bytes(&bytes, MEDIA_TYPE)?;
            (&raw_descriptor, display_name)
        };
        self.io.claim(64)?;
        self.io.allocation_attempt(64)?;
        let version = hex::encode(generation.as_resource().key());
        let _version_hold = self.io.guard_owned(version.len() as u64)?;
        let reference = bounded_io::file_ref(volume, path, &version, descriptor, display_name, Some(&self.io))?;
        self.file(&reference)?;
        self.io.claim(bytes.len() as u64)?;
        self.io.allocation_attempt(bytes.len() as u64)?;
        self.io.charge(WorkCounters { bytes_copied: bytes.len() as u64, ..WorkCounters::default() })?;
        Ok((reference, bytes.to_vec()))
    }

    pub(super) async fn list(&self, volume: &VolumeRef, prefix: &str, path: &str,
        generation: &GenerationRef, after: Option<&str>, maximum: u32) -> Result<PrivateDirectoryPage> {
        self.path(volume, prefix, path, generation)?;
        if self.authorization().native_snapshot.is_some() {
            return Err(Error::Invalid("native selection requires its actual lazy directory reader".into()));
        }
        if maximum == 0 { return Err(Error::Invalid("selected directory page must be finite and positive".into())); }
        self.guard().await?;
        let _serial = self.serial.lock().await;
        let after = after.map(|name| {
            if name.len() > self.limits.path_bytes { return Err(Error::Invalid("selected cursor exceeds original path limits".into())); }
            self.io.claim(name.len() as u64)?;
            self.io.allocation_attempt(name.len() as u64)?;
            LogicalName::new(NameEncoding::Utf8, name.as_bytes().to_vec(), u32::MAX)
                .map_err(|error| Error::Invalid(error.to_string()))
        }).transpose()?;
        self.io.claim(path.len() as u64 + 1)?;
        self.io.allocation_attempt(path.len() as u64 + 1)?;
        let requested = if path.is_empty() { "/".to_owned() } else { format!("/{path}") };
        let _requested_hold = self.io.guard_owned(requested.len() as u64)?;
        let engine_maximum = maximum.checked_add(u32::from(path.is_empty()))
            .ok_or_else(|| Error::Invalid("selected root page limit overflows".into()))?;
        self.guard().await?;
        let result = self.generation.list_directory_measured(&requested, after.as_ref(), engine_maximum,
            self.io.remaining()?, &self.io.cancellation).await;
        let page = self.io.receive(result);
        self.guard().await?;
        let page = page?;
        let slots = (page.entries.len() as u64).checked_mul(std::mem::size_of::<PrivateDirectoryEntry>() as u64)
            .ok_or_else(|| Error::Invalid("selected directory ownership overflows".into()))?;
        self.io.claim(slots)?;
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
            self.io.claim(name.len() as u64)?;
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

    /// The backend argument is the trusted host's actual configured lazy binding,
    /// never a workspace reconstructed from public selector metadata. Identity is
    /// checked against the same workspace opened under the sealed source grant.
    pub(super) fn native_binding<D, S>(&self, lazy: &LazyWorkspace<A, O, D, S>, frozen: LazySnapshotRef,
        path: &str) -> Result<()>
    where D: DemandSource + 'static, S: LazyWorkspaceStore {
        let selected = self.authorization();
        let native = selected.native_snapshot.as_ref().ok_or_else(|| Error::Unauthorized("selection has no native source proof".into()))?;
        let expected = LazySnapshotRef::deserialize(native)
            .map_err(|_| Error::Unauthorized("selected native proof is not an actual SDK snapshot".into()))?;
        if frozen != expected || lazy.workspace().id() != self.workspace_id
            || frozen.workspace_id != self.workspace_id
            || frozen.authored_generation != self.generation.id() {
            return Err(Error::Unauthorized("native backend or frozen source selection differs".into()));
        }
        let relative = path.strip_prefix('/').ok_or_else(|| Error::Invalid("native selected path must be absolute".into()))?;
        self.path(&selected.project, "", relative, &selected.generation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_binding_with_io<D, S>(
        &self, lazy: &LazyWorkspace<A, O, D, S>, frozen: LazySnapshotRef,
        path: &str, io: &PreparationIo,
    ) -> Result<()>
    where D: DemandSource + 'static, S: LazyWorkspaceStore {
        let selected = self.authorization();
        let native = selected.native_snapshot.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "selection has no native source proof",
        })?;
        // Compare exact canonical SDK proof bytes under the original observer.
        // No unobserved serde decoder or temporary hydrated Value is needed.
        let mut observer = io.history_io();
        let mut observed: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut observer);
        let expected = crate::contract::canonical_json_digest_with_work(
            native, selected.maximum_bytes, &mut observed,
        )?;
        let actual = crate::contract::canonical_json_digest_with_work(
            &frozen, selected.maximum_bytes, &mut observed,
        )?;
        if actual != expected || lazy.workspace().id() != self.workspace_id
            || frozen.workspace_id != self.workspace_id
            || frozen.authored_generation != self.generation.id()
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native backend or frozen source selection differs",
            });
        }
        let relative = path.strip_prefix('/').ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "native selected path must be absolute",
        })?;
        if relative.len() > self.limits.path_bytes {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "selected path exceeds original caller limits",
            });
        }
        Self::require_directory_with_io(&self.host, &self.handle, "", io)?;
        Self::require_directory_with_io(&self.host, &self.handle, relative, io)
    }

    pub(super) async fn finish_native<T>(&self,
        result: acyclic_fs::MeasuredResult<OperationReceipt<T>, acyclic_fs::LazyWorkspaceError>,
    ) -> acyclic_fs::MeasuredResult<OperationReceipt<T>, Error> {
        let work = match &result { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        let accounted = match &result { Ok(_) => self.io.charge(work), Err(_) => self.io.spent(work) };
        if let Err(error) = accounted { return Err(OperationFailure::new(error, work)); }
        if let Err(error) = self.guard().await { return Err(OperationFailure::new(error, work)); }
        result.map_err(|failure| {
            let error = match failure.error.semantic_error() {
                acyclic_fs::LazyWorkspaceError::NotFound => Error::NotFound("selected native path".into()),
                _ => Error::Storage(failure.error.to_string()),
            };
            OperationFailure::new(error, *failure.work)
        })
    }

    fn native_budget(&self, budget: WorkBudget) -> Result<WorkBudget> {
        if !budget.has_finite_allowances() {
            return Err(Error::Invalid("native selected caller budget must be finite".into()));
        }
        Ok(self.io.remaining()?.intersect_budget(budget))
    }
}

impl<P, A, O> TaskSourceInspection<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    fn selected_reader(&self) -> Result<&ProjectSourceReader<P, A, O>> {
        match &self.reader {
            SourceReader::Project(reader) => Ok(reader),
            SourceReader::Caller(_) => Err(Error::Unauthorized("native source inspection requires operation-bound authorization".into())),
        }
    }

    /// Reads the immutable selected native source under the supplied original
    /// ledger. Bytes retain the SDK's intrinsic source-data owner; the returned
    /// guard retains separate SDK metadata/body custody. Preserve both through
    /// their actual payload lifetimes without charging the data a second time.
    /// This does not derive a new allowance from a measured receipt.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read_native_path_with_native_work<D, S>(
        &self, lazy: &LazyWorkspace<A, O, D, S>, frozen: LazySnapshotRef,
        path: &str, maximum_bytes: u64, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(bytes::Bytes, acyclic_fs::NativeAllocation)>
    where D: DemandSource + 'static, S: LazyWorkspaceStore {
        work.remaining()?;
        let reader = match &self.reader {
            SourceReader::Project(reader) => reader,
            SourceReader::Caller(_) => return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native source inspection requires operation-bound authorization",
            }),
        };
        if reader.io.cancellation.is_cancelled() {
            return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
        }
        reader.owner.verify_project_source_read_with_native_work(&reader.handle, work).await?;
        if maximum_bytes == 0 || maximum_bytes > reader.limits.file_bytes
            || maximum_bytes > reader.authorization().maximum_bytes
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native selected read exceeds original caller limits",
            });
        }
        let _serial = reader.serial.lock().await;
        reader.owner.verify_project_source_read_with_native_work(&reader.handle, work).await?;
        if reader.io.cancellation.is_cancelled() {
            return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
        }
        let io = PreparationIo::from_native_work(work)?;
        reader.native_binding_with_io(lazy, frozen, path, &io)?;
        let cancellation = reader.owner.invocation_cancellation().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "selected source has no original invocation token",
        })?;
        let result = lazy.read_at_snapshot_with_work(
            frozen, path, maximum_bytes, work, cancellation,
        ).await;
        match result {
            Ok(owned) => {
                // Keep bytes and custody together if the final fence rejects.
                reader.owner.verify_project_source_read_with_native_work(&reader.handle, work).await?;
                if reader.io.cancellation.is_cancelled() {
                    return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
                }
                Ok(owned)
            }
            Err((acyclic_fs::LazyWorkspaceError::NativeWork(cause), custody)) =>
                Err(Error::NativeWork { cause, custody }),
            Err((acyclic_fs::LazyWorkspaceError::Cancelled, custody)) =>
                Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody }),
            Err((source, custody)) => Err(Error::NativeSource { source, custody }),
        }
    }

    /// Looks up one exact immutable native path under the original caller's
    /// operation-bound selection and original ledger. The returned lookup
    /// must physically drop before its separate metadata custody.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn lookup_native_path_with_native_work<D, S>(
        &self, lazy: &LazyWorkspace<A, O, D, S>, frozen: LazySnapshotRef,
        path: &str, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(acyclic_fs::LazyLookup, acyclic_fs::NativeAllocation)>
    where D: DemandSource + 'static, S: LazyWorkspaceStore {
        work.remaining()?;
        let reader = match &self.reader {
            SourceReader::Project(reader) => reader,
            SourceReader::Caller(_) => return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native source inspection requires operation-bound authorization",
            }),
        };
        if reader.io.cancellation.is_cancelled() {
            return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
        }
        reader.owner.verify_project_source_read_with_native_work(&reader.handle, work).await?;
        let _serial = reader.serial.lock().await;
        reader.owner.verify_project_source_read_with_native_work(&reader.handle, work).await?;
        if reader.io.cancellation.is_cancelled() {
            return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
        }
        let io = PreparationIo::from_native_work(work)?;
        reader.native_binding_with_io(lazy, frozen, path, &io)?;
        let cancellation = reader.owner.invocation_cancellation().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "selected source has no original invocation token",
        })?;
        let result = lazy.lookup_at_snapshot_with_work(frozen, path, work, cancellation).await;
        match result {
            Ok(owned) => {
                // Preserve the actual lookup and guard together on final rejection.
                reader.owner.verify_project_source_read_with_native_work(&reader.handle, work).await?;
                if reader.io.cancellation.is_cancelled() {
                    return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
                }
                Ok(owned)
            }
            Err((acyclic_fs::LazyWorkspaceError::NativeWork(cause), custody)) =>
                Err(Error::NativeWork { cause, custody }),
            Err((acyclic_fs::LazyWorkspaceError::Cancelled, custody)) =>
                Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody }),
            Err((source, custody)) => Err(Error::NativeSource { source, custody }),
        }
    }

    /// Reads an opaque selected symbolic-link target, never following it.
    /// The original caller's file ceiling and sealed source byte ceiling are
    /// forwarded to the same SDK body before its payload IO.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read_native_link_with_native_work<D, S>(
        &self, lazy: &LazyWorkspace<A, O, D, S>, frozen: LazySnapshotRef,
        path: &str, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(bytes::Bytes, acyclic_fs::NativeAllocation)>
    where D: DemandSource + 'static, S: LazyWorkspaceStore {
        work.remaining()?;
        let reader = match &self.reader {
            SourceReader::Project(reader) => reader,
            SourceReader::Caller(_) => return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native source inspection requires operation-bound authorization",
            }),
        };
        if reader.io.cancellation.is_cancelled() {
            return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
        }
        reader.owner.verify_project_source_read_with_native_work(&reader.handle, work).await?;
        let maximum_bytes = reader.limits.file_bytes.min(reader.authorization().maximum_bytes);
        if maximum_bytes == 0 {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native selected link has no original caller byte allowance",
            });
        }
        let _serial = reader.serial.lock().await;
        reader.owner.verify_project_source_read_with_native_work(&reader.handle, work).await?;
        if reader.io.cancellation.is_cancelled() {
            return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
        }
        let io = PreparationIo::from_native_work(work)?;
        reader.native_binding_with_io(lazy, frozen, path, &io)?;
        let cancellation = reader.owner.invocation_cancellation().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "selected source has no original invocation token",
        })?;
        let result = lazy.read_link_at_snapshot_with_work(
            frozen, path, maximum_bytes, work, cancellation,
        ).await;
        match result {
            Ok(owned) => {
                // Bytes keep intrinsic payload custody; do not charge them twice.
                reader.owner.verify_project_source_read_with_native_work(&reader.handle, work).await?;
                if reader.io.cancellation.is_cancelled() {
                    return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into());
                }
                Ok(owned)
            }
            Err((acyclic_fs::LazyWorkspaceError::NativeWork(cause), custody)) =>
                Err(Error::NativeWork { cause, custody }),
            Err((acyclic_fs::LazyWorkspaceError::Cancelled, custody)) =>
                Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody }),
            Err((source, custody)) => Err(Error::NativeSource { source, custody }),
        }
    }

    /// Reads unresolved native content through the trusted host's ACTUAL lazy
    /// binding and the original frozen snapshot, not its authored-only generation.
    /// Receipts retain the actual SDK work, including failed/stale-source reads.
    /// The reader's cumulative allowance and original invocation token are reused.
    pub async fn read_native_path<D, S>(
        &self, lazy: &LazyWorkspace<A, O, D, S>, frozen: LazySnapshotRef,
        path: &str, maximum_bytes: u64, budget: WorkBudget,
    ) -> acyclic_fs::MeasuredResult<OperationReceipt<bytes::Bytes>, Error>
    where D: DemandSource + 'static, S: LazyWorkspaceStore {
        let reader = self.selected_reader().map_err(OperationFailure::before_work)?;
        reader.native_binding(lazy, frozen, path).map_err(OperationFailure::before_work)?;
        if maximum_bytes == 0 || maximum_bytes > reader.limits.file_bytes {
            return Err(OperationFailure::before_work(Error::Invalid("native selected read exceeds original file limits".into())));
        }
        reader.guard().await.map_err(OperationFailure::before_work)?;
        let _serial = reader.serial.lock().await;
        reader.guard().await.map_err(OperationFailure::before_work)?;
        let budget = reader.native_budget(budget).map_err(OperationFailure::before_work)?;
        let result = lazy.read_at_snapshot_measured(frozen, path, maximum_bytes, budget, &reader.io.cancellation).await;
        reader.finish_native(result).await
    }

    /// Reads an exact frozen link target without following it or widening source
    /// authority. The caller's remaining allowance intersects the retained meter.
    pub async fn read_native_link<D, S>(
        &self, lazy: &LazyWorkspace<A, O, D, S>, frozen: LazySnapshotRef,
        path: &str, budget: WorkBudget,
    ) -> acyclic_fs::MeasuredResult<OperationReceipt<bytes::Bytes>, Error>
    where D: DemandSource + 'static, S: LazyWorkspaceStore {
        let reader = self.selected_reader().map_err(OperationFailure::before_work)?;
        reader.native_binding(lazy, frozen, path).map_err(OperationFailure::before_work)?;
        reader.guard().await.map_err(OperationFailure::before_work)?;
        let _serial = reader.serial.lock().await;
        reader.guard().await.map_err(OperationFailure::before_work)?;
        let mut budget = reader.native_budget(budget).map_err(OperationFailure::before_work)?;
        budget.output_bytes = budget.output_bytes.min(reader.limits.file_bytes);
        budget.source_bytes_read = budget.source_bytes_read.min(reader.limits.file_bytes);
        let result = lazy.read_link_at_snapshot_measured(frozen, path, budget, &reader.io.cancellation).await;
        reader.finish_native(result).await
    }

    /// Resolves one no-follow path through the same authorized frozen native view.
    pub async fn lookup_native_path<D, S>(
        &self, lazy: &LazyWorkspace<A, O, D, S>, frozen: LazySnapshotRef, path: &str, budget: WorkBudget,
    ) -> acyclic_fs::MeasuredResult<OperationReceipt<acyclic_fs::LazyLookup>, Error>
    where D: DemandSource + 'static, S: LazyWorkspaceStore {
        let reader = self.selected_reader().map_err(OperationFailure::before_work)?;
        reader.native_binding(lazy, frozen, path).map_err(OperationFailure::before_work)?;
        reader.guard().await.map_err(OperationFailure::before_work)?;
        let _serial = reader.serial.lock().await;
        reader.guard().await.map_err(OperationFailure::before_work)?;
        let budget = reader.native_budget(budget).map_err(OperationFailure::before_work)?;
        let result = lazy.lookup_at_snapshot_measured(frozen, path, budget, &reader.io.cancellation).await;
        reader.finish_native(result).await
    }

    /// Pages only the selected frozen native directory using its exact SDK
    /// continuation token. This never enumerates or hydrates a task/swarm tree.
    pub async fn list_native_directory<D, S>(
        &self, lazy: &LazyWorkspace<A, O, D, S>, frozen: LazySnapshotRef, path: &str,
        cursor: Option<acyclic_fs::LazyDirectoryCursor>, maximum_entries: u32, budget: WorkBudget,
    ) -> acyclic_fs::MeasuredResult<OperationReceipt<acyclic_fs::LazyDirectoryPage>, Error>
    where D: DemandSource + 'static, S: LazyWorkspaceStore {
        let reader = self.selected_reader().map_err(OperationFailure::before_work)?;
        reader.native_binding(lazy, frozen, path).map_err(OperationFailure::before_work)?;
        if maximum_entries == 0 {
            return Err(OperationFailure::before_work(Error::Invalid("native directory page must be positive".into())));
        }
        reader.guard().await.map_err(OperationFailure::before_work)?;
        let _serial = reader.serial.lock().await;
        reader.guard().await.map_err(OperationFailure::before_work)?;
        let budget = reader.native_budget(budget).map_err(OperationFailure::before_work)?;
        let result = lazy.list_directory_at_snapshot_measured(frozen, path, cursor, maximum_entries, budget, &reader.io.cancellation).await;
        reader.finish_native(result).await
    }

    /// Produces the SDK's real bounded selected-import plan. It neither commits
    /// into a destination nor changes the source head. Destination authority,
    /// exact-root approval and conflict protection remain the caller's workflow.
    pub async fn prepare_native_selected_import<D, S>(
        &self, lazy: &LazyWorkspace<A, O, D, S>, frozen: LazySnapshotRef,
        requested: &[acyclic_fs::kernel::NamespacePath], budget: WorkBudget,
    ) -> acyclic_fs::MeasuredResult<OperationReceipt<acyclic_fs::SelectedImportPlan>, Error>
    where D: DemandSource + 'static, S: LazyWorkspaceStore {
        let reader = self.selected_reader().map_err(OperationFailure::before_work)?;
        reader.native_binding(lazy, frozen, "/").map_err(OperationFailure::before_work)?;
        let accepted_budget = reader.native_budget(budget).map_err(OperationFailure::before_work)?;
        if requested.is_empty() || requested.len() as u64 > accepted_budget.items_examined {
            return Err(OperationFailure::before_work(Error::Invalid("selected import roots exceed the finite allowance".into())));
        }
        for path in requested {
            if path.encoded_bytes() as usize > reader.limits.path_bytes
                || path.components().first().is_some_and(|name| name.unicode_text().as_deref() == Some(".system")) {
                return Err(OperationFailure::before_work(Error::Unauthorized("selected import path leaves its public project boundary".into())));
            }
        }
        reader.guard().await.map_err(OperationFailure::before_work)?;
        let _serial = reader.serial.lock().await;
        reader.guard().await.map_err(OperationFailure::before_work)?;
        let budget = reader.native_budget(budget).map_err(OperationFailure::before_work)?;
        let result = lazy.prepare_selected_import_measured(frozen, requested, budget, &reader.io.cancellation).await;
        reader.finish_native(result).await
    }
}
