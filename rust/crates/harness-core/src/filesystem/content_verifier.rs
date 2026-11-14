use super::*;
use crate::conversation::{PrivateDirectoryEntry, PrivateDirectoryEntryKind};

/// Filesystem-backed admission barrier for version-pinned conversation content.
pub struct FilesystemContentVerifier<A: 'static, O: 'static> {
    pub(in crate::filesystem) host: Arc<FilesystemHost<A, O>>,
    pub(in crate::filesystem) authority: Arc<crate::core::VerifiedScope>,
    pub(in crate::filesystem) exact_file: Option<FileRef>,
    pub(in crate::filesystem) maximum_bytes: Option<u64>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) _native_metadata: Option<Arc<acyclic_fs::NativeAllocation>>,
}

impl<A, O> FilesystemContentVerifier<A, O> {
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) fn require_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        if !self
            ._native_metadata
            .as_ref()
            .is_some_and(|guard| guard.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "Filesystem verifier belongs to another original resource",
            });
        }
        let io = PreparationIo::from_native_work(work)?;
        self.authority.recheck(&mut Some(&mut io.history_io()))
    }

    /// Binds authenticated read authority with optional preparation accounting.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        maximum_bytes: impl Into<Option<u64>>,
        io: Option<&PreparationIo>,
    ) -> Result<Arc<Self>> {
        let maximum_bytes = maximum_bytes.into();
        if maximum_bytes == Some(0) {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "content admission limit must be positive",
            ));
        }
        crate::executor::with_history_io(io, |work| -> Result<_> {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let authority = verifier.authenticate_scope(scope, quota.work())?;
            quota.adopt(authority.logical_owned_bytes()?)?;
            if quota.work().is_some() {
                quota.retain(std::mem::size_of::<Self>() as u64)?;
                quota
                    .observer()?
                    .allocation_attempt(std::mem::size_of::<Self>() as u64)?;
                let authority_bytes = (std::mem::size_of::<crate::core::VerifiedScope>()
                    + 2 * std::mem::size_of::<usize>())
                    as u64;
                quota.retain(authority_bytes)?;
                quota.observer()?.allocation_attempt(1)?;
            }
            #[cfg(not(target_arch = "wasm32"))]
            if quota
                .work()
                .as_deref()
                .is_some_and(|io| io.original_native_work().is_some())
            {
                quota.retain(
                    (std::mem::size_of::<acyclic_fs::NativeAllocation>()
                        + 2 * std::mem::size_of::<usize>()) as u64,
                )?;
                quota.observer()?.allocation_attempt(1)?;
            }
            let _native_metadata = quota.into_custody()?.map(Arc::new);
            Ok(Arc::new(Self {
                host,
                authority: Arc::new(authority),
                exact_file: None,
                maximum_bytes,
                #[cfg(not(target_arch = "wasm32"))]
                _native_metadata,
            }))
        })
    }
}

impl<A, O> ContentResidencyVerifier for FilesystemContentVerifier<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn capture_private_path<'a>(
        &'a self,
        capture: crate::conversation::ContentCapture<'a>,
        maximum_bytes: Option<u64>,
        access: crate::conversation::ContentAccess<'a, 'a>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<'a, crate::conversation::CapturedContent<'a>>,
    > {
        crate::conversation::BoundedContentFuture::new(access.io, || async move {
            self.verify_exact_file(None, access.io)?;
            let mut history = access.io.map(PreparationIo::history_io);
            let grant = ContentGrant::verify_directory_read(
                self.authority.verifier(),
                self.authority.scope(),
                capture.source.volume,
                capture.source.granted_prefix,
                &mut history
                    .as_mut()
                    .map(|observer| observer as &mut dyn crate::store::history::HistoryIo),
            )?;
            let _grant_hold = access
                .io
                .map(|io| io.guard_owned(grant.logical_owned_bytes()?))
                .transpose()?;
            self.host
                .capture_private_path(
                    &grant,
                    capture,
                    maximum_bytes.into_iter().chain(self.maximum_bytes).min(),
                    access,
                )
                .await
        })
    }

    fn supports_read(&self, volume: &VolumeRef, io: Option<&PreparationIo>) -> Result<bool> {
        if self
            .exact_file
            .as_ref()
            .is_some_and(|file| file.volume() != volume)
        {
            return Ok(false);
        }
        crate::model::charge_items(io, 1)?;
        let mut history = io.map(PreparationIo::history_io);
        volume.validate(
            &mut history
                .as_mut()
                .map(|observer| observer as &mut dyn crate::store::history::HistoryIo),
        )?;
        Ok(volume.provider() == &self.host.provider)
    }

    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        ceiling: Option<&'a crate::Capabilities>,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
        acyclic_differential::record(("content read", maximum_bytes));
        crate::conversation::BoundedContentFuture::new(io, || {
            self.read_file(reference, maximum_bytes, ceiling, io)
        })
    }

    fn read_range<'a>(
        &'a self,
        reference: &'a FileRef,
        range: acyclic_fs::ByteRange,
        ceiling: Option<&'a crate::Capabilities>,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.verify_exact_file(Some(reference), io)?;
            if self
                .maximum_bytes
                .is_some_and(|maximum| range.length > maximum)
                || range
                    .offset
                    .checked_add(range.length)
                    .is_none_or(|end| end > reference.descriptor().byte_length())
            {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "range exceeds source or provider credit",
                ));
            }
            let owned;
            let authority_io = if let Some(io) = io {
                io
            } else {
                self.authority.recheck(&mut None)?;
                owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                &owned
            };
            crate::executor::with_history_io(io, |work| self.authority.recheck(work))?;
            let bytes = self
                .host
                .read_selection_with_access(
                    self.authority.scope(),
                    reference,
                    self.exact_file.is_some(),
                    ceiling,
                    authority_io,
                )
                .await?
                .read_range(range)
                .await?;
            crate::executor::with_history_io(io, |work| self.authority.recheck(work))?;
            Ok(bytes)
        })
    }

    fn verify<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            drop(self.read_file(reference, maximum_bytes, None, io).await?);
            if let Some(io) = io {
                io.remaining()?;
            }
            Ok(())
        })
    }

    fn verify_publication<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if reference.descriptor().byte_length()
                > self
                    .maximum_bytes
                    .map_or(maximum_bytes, |maximum| maximum.min(maximum_bytes))
            {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "publication exceeds its byte credit",
                ));
            }
            if !self.host.published_here(reference, io)? {
                return self.verify(reference, maximum_bytes, io)?.await;
            }
            let temporary = PreparationIo::temporary(io)?;
            let owned;
            let io = if let Some(io) = temporary.as_ref().or(io) {
                io
            } else {
                owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                &owned
            };
            self.verify_exact_file(Some(reference), Some(io))?;
            self.authority.recheck(&mut Some(&mut io.history_io()))?;
            self.host
                .read_selection_with_access(
                    self.authority.scope(),
                    reference,
                    self.exact_file.is_some(),
                    None,
                    io,
                )
                .await?
                .confirm_publication()
                .await?;
            self.authority.recheck(&mut Some(&mut io.history_io()))?;
            io.remaining()?;
            Ok(())
        })
    }

    fn read_private_prefix<'a>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        generation: &'a GenerationRef,
        maximum_bytes: u64,
        access: crate::conversation::ContentAccess<'a, 'a>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        let crate::conversation::ContentAccess { ceiling, io } = access;

        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.verify_exact_file(None, io)?;
            if maximum_bytes == 0
                || self
                    .maximum_bytes
                    .is_some_and(|maximum| maximum_bytes > maximum)
            {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "prefix read exceeds provider limit",
                ));
            }
            let grant = crate::executor::with_history_io(io, |work| {
                ContentGrant::verify_directory_read(
                    self.authority.verifier(),
                    self.authority.scope(),
                    volume,
                    granted_prefix,
                    work,
                )
            })?;
            let grant_hold = io
                .map(|io| io.guard_owned(grant.logical_owned_bytes()?))
                .transpose()?;
            let bytes = self
                .host
                .read_private_prefix(
                    volume,
                    &grant,
                    path,
                    generation,
                    maximum_bytes,
                    crate::conversation::ContentAccess { ceiling, io },
                )
                .await?;
            drop(grant);
            drop(grant_hold);
            Ok(bytes)
        })
    }

    fn list_private_directory<'a, 'io: 'a>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
        after: Option<&'a [u8]>,
        maximum_entries: u32,
        ceiling: Option<&'a crate::Capabilities>,
        io: Option<&'io PreparationIo>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            Option<(
                PrivateDirectoryPage,
                Option<crate::conversation::ContentHold<'io>>,
            )>,
        >,
    > {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let unmeasured;
            let ledger = match io {
                Some(io) => io,
                None => {
                    unmeasured =
                        PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                    &unmeasured
                }
            };
            self.verify_exact_file(None, Some(ledger))?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = ledger.original_native_work() {
                self.supports_native_directory_list(volume, work)?;
            }
            let grant = crate::executor::with_history_io(Some(ledger), |history| {
                ContentGrant::verify_directory_read(
                    self.authority.verifier(),
                    self.authority.scope(),
                    volume,
                    granted_prefix,
                    history,
                )
            })?;
            let _grant_hold = ledger.guard_owned(grant.logical_owned_bytes()?)?;
            let Some((page, held)) = self
                .host
                .list_directory_page(
                    host::DirectoryListing {
                        volume,
                        grant: &grant,
                        path,
                        generation: expected_generation,
                        after,
                        maximum_entries,
                        ceiling,
                    },
                    ledger,
                    None,
                    &mut || std::future::ready(Ok(())),
                )
                .await?
            else {
                return Ok(None);
            };
            self.authority
                .recheck(&mut Some(&mut ledger.history_io()))?;
            // Only caller-funded results transfer custody out of this future.
            match io {
                Some(io) => {
                    let bytes = held.bytes();
                    held.into_retained()?;
                    Ok(Some((page, Some(io.guard_owned(bytes)?))))
                }
                None => Ok(Some((page, None))),
            }
        })
    }

    fn diff_private_paths<'a>(
        &'a self,
        roots: &'a [&'a crate::context::ContextRoot],
        generations: (&'a GenerationRef, &'a GenerationRef),
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<crate::context::DiffPublication>>>
    {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.verify_exact_file(None, io)?;
            let unmeasured;
            let io = if let Some(io) = io {
                io
            } else {
                unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                &unmeasured
            };
            let first = roots
                .first()
                .ok_or_else(|| io.invalid("comparison has no roots"))?;
            let mut grants = Vec::new();
            let mut holds = Vec::new();
            for root in roots {
                if root.volume != first.volume {
                    return Err(io.invalid("comparison roots belong to different volumes"));
                }
                let grant = ContentGrant::verify_directory_read(
                    self.authority.verifier(),
                    self.authority.scope(),
                    &root.volume,
                    &root.directory,
                    &mut Some(&mut io.history_io()),
                )?;
                holds.push(io.guard_owned(grant.logical_owned_bytes()?)?);
                grants.push(grant);
            }
            let selected: Vec<_> = roots.iter().copied().zip(&grants).collect();
            let publication = self
                .host
                .diff_private_publications(&selected, generations, io)
                .await?;
            drop(grants);
            drop(holds);
            Ok(publication)
        })
    }

    fn select_private_generation<'a, 'io>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
        access: crate::conversation::ContentAccess<'a, 'io>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (GenerationRef, Option<crate::conversation::ContentHold<'io>>),
        >,
    >
    where
        'io: 'a,
    {
        let crate::conversation::ContentAccess { ceiling, io } = access;
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.verify_exact_file(None, io)?;
            let mut history = io.map(PreparationIo::history_io);
            let grant = ContentGrant::verify_directory_read(
                self.authority.verifier(),
                self.authority.scope(),
                volume,
                granted_prefix,
                &mut history
                    .as_mut()
                    .map(|observer| observer as &mut dyn crate::store::history::HistoryIo),
            )?;
            let grant_hold = io
                .map(|io| io.guard_owned(grant.logical_owned_bytes()?))
                .transpose()?;
            let generation = self
                .host
                .select_private_generation(
                    volume,
                    &grant,
                    path,
                    expected_generation,
                    crate::conversation::ContentAccess { ceiling, io },
                )
                .await?;
            let hold = io
                .map(|io| io.guard_owned(generation.logical_owned_bytes()?))
                .transpose()?;
            drop(grant);
            drop(grant_hold);
            Ok((generation, hold))
        })
    }

    fn wait_private_path<'a, 'io>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        after: Option<&'a GenerationRef>,
        access: crate::conversation::ContentAccess<'a, 'io>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>>
    where
        'io: 'a,
    {
        use acyclic_stream::StreamProviderExt as _;
        use futures::TryStreamExt as _;
        let streams = self.host.changes.as_ref().ok_or_else(|| {
            stock_boundary(
                access.io,
                crate::wire::ErrorCode::Unsupported,
                "content provider has no path change event",
            )
        })?;
        crate::conversation::BoundedContentFuture::new(access.io, || async move {
            let exists = || async {
                match self
                    .read_private_range(
                        volume,
                        granted_prefix,
                        path,
                        None,
                        acyclic_fs::ByteRange {
                            offset: 0,
                            length: 0,
                        },
                        access,
                    )?
                    .await
                {
                    Ok((selected, _, _)) => Ok(after != Some(&selected.generation)),
                    Err(error) if error.code() == crate::wire::ErrorCode::NotFound => {
                        Ok(after.is_some())
                    }
                    Err(error) => Err(error),
                }
            };
            if exists().await? {
                return Ok(());
            }
            let reference =
                super::bounded_io::content_workspace(&self.host.provider, volume, access.io)?;
            let workspace = self.host.open(&reference, access.io).await?;
            let changes = acyclic_fs::session::changes_path(
                &self.host.filesystem,
                workspace.id().volume_id(),
            )
            .map_err(Error::from)?;
            let tail = streams.tail(changes.clone()).await.map_err(Error::from)?;
            let mut updates = acyclic_stream::records_only(
                streams
                    .follow(acyclic_stream::FollowRequest::new(changes, tail))
                    .await
                    .map_err(Error::from)?,
            );
            loop {
                if exists().await? {
                    return Ok(());
                }
                updates
                    .try_next()
                    .await
                    .map_err(Error::from)?
                    .ok_or_else(|| {
                        Error::Storage("content change stream ended before path publication".into())
                    })?;
            }
        })
    }

    fn read_private_range<'a, 'io>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
        range: acyclic_fs::ByteRange,
        access: crate::conversation::ContentAccess<'a, 'io>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (
                crate::conversation::FilePathSelection,
                acyclic_stream::OwnedBytes,
                Option<crate::conversation::ContentHold<'io>>,
            ),
        >,
    >
    where
        'io: 'a,
    {
        let crate::conversation::ContentAccess { ceiling, io } = access;
        #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
        acyclic_differential::record(("generation-bound range", range.offset, range.length));
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.verify_exact_file(None, io)?;
            let mut history = io.map(PreparationIo::history_io);
            let grant = ContentGrant::verify_directory_read(
                self.authority.verifier(),
                self.authority.scope(),
                volume,
                granted_prefix,
                &mut history
                    .as_mut()
                    .map(|observer| observer as &mut dyn crate::store::history::HistoryIo),
            )?;
            let grant_hold = io
                .map(|io| io.guard_owned(grant.logical_owned_bytes()?))
                .transpose()?;
            if self
                .maximum_bytes
                .is_some_and(|maximum| range.length > maximum)
            {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "path range exceeds the bound reader allowance",
                )
                .tool_input());
            }
            let (bytes, generation, byte_length) = self
                .host
                .read_private_range(
                    volume,
                    &grant,
                    path,
                    expected_generation,
                    range,
                    crate::conversation::ContentAccess { ceiling, io },
                )
                .await?;
            let selection = crate::executor::with_history_io(io, |work| {
                let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
                let selected = crate::conversation::FilePathSelection {
                    volume: volume.copy_in_quota(&mut quota)?,
                    path: crate::core::copy_text(path, &mut quota)?,
                    generation,
                    byte_length,
                };
                quota.transfer();
                Ok::<_, Error>(selected)
            })?;
            let hold = io
                .map(|io| io.guard_owned(selection.logical_owned_bytes()?))
                .transpose()?;
            drop(grant);
            drop(grant_hold);
            Ok((selection, bytes, hold))
        })
    }

    fn read_private_path<'a, 'io>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
        maximum_bytes: Option<u64>,
        access: crate::conversation::ContentAccess<'a, 'io>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            Option<(
                FileRef,
                acyclic_stream::OwnedBytes,
                Option<crate::conversation::ContentHold<'io>>,
            )>,
        >,
    >
    where
        'io: 'a,
    {
        let crate::conversation::ContentAccess { ceiling, io } = access;

        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.verify_exact_file(None, io)?;
            let mut history = io.map(PreparationIo::history_io);
            let grant = ContentGrant::verify_directory_read(
                self.authority.verifier(),
                self.authority.scope(),
                volume,
                granted_prefix,
                &mut history
                    .as_mut()
                    .map(|observer| observer as &mut dyn crate::store::history::HistoryIo),
            )?;
            let grant_hold = io
                .map(|io| io.guard_owned(grant.logical_owned_bytes()?))
                .transpose()?;
            let selected = self
                .host
                .read_private_path_indexed(
                    volume,
                    &grant,
                    path,
                    expected_generation,
                    maximum_bytes.into_iter().chain(self.maximum_bytes).min(),
                    crate::conversation::ContentAccess { ceiling, io },
                )
                .await?;
            drop(grant);
            drop(grant_hold);
            let Some((file, bytes, _)) = selected else {
                return Ok(None);
            };
            let hold = io
                .map(|io| io.guard_owned(file.logical_owned_bytes()?))
                .transpose()?;
            Ok(Some((file, bytes, hold)))
        })
    }
}

impl<A, O> FilesystemContentVerifier<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(in crate::filesystem) fn verify_exact_file(
        &self,
        file: Option<&FileRef>,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
        acyclic_differential::record(("content creation verify", 0u64));
        if let Some(bound) = &self.exact_file
            && file != Some(bound)
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "created reference handle names another immutable file",
            ));
        }
        Ok(())
    }

    pub(in crate::filesystem) async fn read_file(
        &self,
        reference: &FileRef,
        maximum_bytes: u64,
        ceiling: Option<&crate::Capabilities>,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let temporary = PreparationIo::temporary(io)?;
        let io = temporary.as_ref().or(io);
        self.verify_exact_file(Some(reference), io)?;
        let unmeasured;
        let authority_io = if let Some(io) = io {
            io
        } else {
            unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &unmeasured
        };
        crate::executor::with_history_io(io, |work| self.authority.recheck(work))?;
        let bytes = self
            .host
            .read_file_with_access(
                self.authority.scope(),
                reference,
                self.maximum_bytes
                    .map_or(maximum_bytes, |maximum| maximum.min(maximum_bytes)),
                self.exact_file.is_some(),
                ceiling,
                authority_io,
            )
            .await?;
        crate::executor::with_history_io(io, |work| self.authority.recheck(work))?;
        #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
        acyclic_differential::record(("resolved content body", bytes.len() as u64));
        Ok(bytes)
    }

    /// Lazily lists another agent's private directory under this reader's signed, owner-delegated subtree grant.
    /// Only an absent authorized directory returns `None`; source failures remain errors.
    pub async fn list_private_directory(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
        options: crate::filesystem::DirectoryOptions<'_>,
    ) -> Result<Option<PrivateDirectoryPage>> {
        let crate::filesystem::DirectoryOptions {
            generation: expected_generation,
            after,
            maximum_entries,
            ceiling,
        } = options;

        self.verify_exact_file(None, None)?;
        let grant = ContentGrant::verify_directory_read(
            self.authority.verifier(),
            self.authority.scope(),
            volume,
            granted_prefix,
            &mut None,
        )?;
        self.host
            .list_private_directory(
                volume,
                &grant,
                path,
                crate::filesystem::DirectoryOptions {
                    generation: expected_generation,
                    after,
                    maximum_entries,
                    ceiling,
                },
            )
            .await
    }

    /// The exact-path fork diff of `volume` from `base` to `current`, as far
    /// as this authority may list it (see [`FilesystemHost::diff_publication`]).
    pub async fn diff_publication(
        &self,
        volume: &VolumeRef,
        base: &GenerationRef,
        current: &GenerationRef,
    ) -> Result<crate::context::DiffPublication> {
        self.verify_exact_file(None, None)?;
        let grant = ContentGrant::verify_directory_read(
            self.authority.verifier(),
            self.authority.scope(),
            volume,
            "",
            &mut None,
        )?;
        self.host
            .diff_publication(volume, &grant, base, current)
            .await
    }

    /// Demand-driven exact fork changes under the original read authority.
    /// Empty steps report filtered source work; only stream exhaustion means
    /// completion. No hidden changed-path suffix is collected or replayed.
    pub fn diff_steps<'a>(
        &'a self,
        volume: &'a VolumeRef,
        base: &'a GenerationRef,
        current: &'a GenerationRef,
    ) -> impl futures::Stream<Item = Result<Option<crate::context::DiffPath>>> + 'a {
        async_stream::try_stream! {
            use futures::TryStreamExt as _;
            self.verify_exact_file(None, None)?;
            let grant = ContentGrant::verify_directory_read(
                self.authority.verifier(), self.authority.scope(), volume, "", &mut None,
            )?;
            let steps = self.host.diff_steps(volume, &grant, base, current);
            futures::pin_mut!(steps);
            while let Some(step) = steps.try_next().await? {
                yield step;
            }
        }
    }

    /// Whether this authority may still list each of `paths` of a
    /// [`Self::diff_publication`] it was given (see
    /// [`FilesystemHost::diff_disclosure`]).
    pub async fn diff_disclosure<'path>(
        &self,
        volume: &VolumeRef,
        (base, current): (&GenerationRef, &GenerationRef),
        paths: impl IntoIterator<Item = (&'path str, crate::context::DiffKind)>,
    ) -> Result<bool> {
        self.verify_exact_file(None, None)?;
        let grant = ContentGrant::verify_directory_read(
            self.authority.verifier(),
            self.authority.scope(),
            volume,
            "",
            &mut None,
        )?;
        self.host
            .diff_disclosure(volume, &grant, (base, current), paths)
            .await
    }

    /// Requires authentic provider metadata funded by this original resource.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn supports_native_directory_list(
        &self,
        volume: &VolumeRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.require_native_work(work)?;
        volume.validate(&mut Some(&mut crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        )))?;
        if volume.provider() != &self.host.provider {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "private directory belongs to another provider",
            });
        }
        Ok(())
    }

    /// Lists under both the signed provider subtree and the current dispatch owner.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(
        clippy::too_many_arguments,
        reason = "exact owner, subtree and immutable pagination"
    )]
    pub fn list_private_directory_with_native_work<'a>(
        &'a self,
        context: &'a crate::runtime::ToolContext,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
        after: Option<&'a [u8]>,
        maximum_entries: u32,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        acyclic_fs::NativeFutureBundle<
            'a,
            (PrivateDirectoryPage, acyclic_fs::NativeAllocation),
            crate::Error,
        >,
    > {
        self.supports_native_directory_list(volume, work)?;
        acyclic_fs::NativeFutureBundle::new(work, || async move {
            self.verify_exact_file(None, Some(&PreparationIo::from_native_work(work)?))?;
            let mut observer = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(work)?,
            );
            let grant = ContentGrant::verify_directory_read(
                self.authority.verifier(),
                self.authority.scope(),
                volume,
                granted_prefix,
                &mut Some(&mut observer),
            )?;
            let grant_custody = observer.into_custody()?;
            if context
                .effective_scope()
                .limits()
                .path_bytes
                .is_some_and(|maximum| path.len() > maximum.get())
                || (after.is_some() && expected_generation.is_none())
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "directory cursor or path exceeds its issued task boundary",
                });
            }
            let result = self
                .host
                .list_private_directory_guarded_with_native_work(
                    host::DirectoryListing {
                        volume,
                        grant: &grant,
                        path,
                        generation: expected_generation,
                        after,
                        maximum_entries,
                        ceiling: Some(context.effective_scope().grants()),
                    },
                    work,
                    || async {
                        self.supports_native_directory_list(volume, work)?;
                        let io = PreparationIo::from_native_work(work)?;
                        if !crate::core::directory_read_granted_with_work(
                            context.effective_scope().grants(),
                            volume,
                            granted_prefix,
                            &mut Some(&mut io.history_io()),
                        )? {
                            return Err(Error::NativeBoundary {
                                code: crate::wire::ErrorCode::Unauthorized,
                                reason: "issued task scope does not retain this directory read",
                            });
                        }
                        Ok(())
                    },
                )
                .await;
            drop(grant);
            drop(grant_custody);
            result
        })
    }
}

pub(in crate::filesystem) fn require_file_read(
    scope: &Scope,
    reference: &FileRef,
    io: &PreparationIo,
) -> Result<()> {
    reference.validate(&mut Some(&mut io.history_io()))?;
    // The one shared read decision; persisted volume policy governs its files.
    match crate::core::file_read_grant(
        scope.capabilities(),
        reference,
        &mut Some(&mut io.history_io()),
    )? {
        Some(
            crate::core::ReadGrant::Volume
            | crate::core::ReadGrant::Exact
            | crate::core::ReadGrant::Structured,
        ) => return Ok(()),
        Some(crate::core::ReadGrant::Directory(_)) if scope.agent().is_some() => return Ok(()),
        Some(crate::core::ReadGrant::Directory(_)) | None => {}
    }
    Err(stock_boundary(
        Some(io),
        crate::wire::ErrorCode::Unauthorized,
        "file read is not granted by its owner",
    ))
}

use crate::executor::stock_boundary;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

impl PrivateDirectoryEntry {
    /// The one Unicode projection for authored and computed directory metadata.
    pub(in crate::filesystem) fn from_filesystem(
        entry: acyclic_fs::LazyDirectoryEntry,
        ledger: &crate::filesystem::PreparationIo,
    ) -> Result<Self> {
        let (name, allocation) = ledger.account(|ctx| {
            entry
                .name
                .unicode_text_with_accounting(ctx, ledger.cancellation())
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        if let acyclic_fs::kernel::KernelOwnedAllocation::NativeOriginal(allocation) = allocation {
            ledger.absorb_native_custody(allocation)?;
        }
        #[cfg(target_arch = "wasm32")]
        let _ = allocation;
        use PrivateDirectoryEntryKind as Kind;
        use acyclic_fs::{LazyDirectoryNode, demand::SourceNodeKind, kernel::FileKind};
        let kind = match entry.node {
            LazyDirectoryNode::Authored { kind, .. } => match kind {
                FileKind::Regular => Kind::File,
                FileKind::Directory => Kind::Directory,
                FileKind::SymbolicLink => Kind::SymbolicLink,
                FileKind::Fifo => Kind::Fifo,
                FileKind::Socket => Kind::Socket,
                FileKind::CharacterDevice => Kind::CharacterDevice,
                FileKind::BlockDevice => Kind::BlockDevice,
                FileKind::ReparsePoint => Kind::ReparsePoint,
                FileKind::MountBoundary => Kind::MountBoundary,
            },
            LazyDirectoryNode::Source(node) => match node.kind {
                SourceNodeKind::RegularFile => Kind::File,
                SourceNodeKind::Directory => Kind::Directory,
                SourceNodeKind::SymbolicLink => Kind::SymbolicLink,
                SourceNodeKind::Fifo => Kind::Fifo,
                SourceNodeKind::Socket => Kind::Socket,
                SourceNodeKind::CharacterDevice => Kind::CharacterDevice,
                SourceNodeKind::BlockDevice => Kind::BlockDevice,
                SourceNodeKind::Unsupported => Kind::Unsupported,
            },
        };
        Ok(Self { name, kind })
    }
}
