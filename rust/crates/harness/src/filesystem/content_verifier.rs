use super::*;

/// Filesystem-backed admission barrier for version-pinned conversation content.
pub struct FilesystemContentVerifier<A, O> {
    pub(in crate::filesystem) host: Arc<FilesystemHost<A, O>>,
    pub(in crate::filesystem) verifier: Arc<AuthorityVerifier>,
    pub(in crate::filesystem) scope: Arc<Scope>,
    pub(in crate::filesystem) creation: Option<Arc<fork_preparer::CreatedProvenance>>,
    pub(in crate::filesystem) exact_file: Option<FileRef>,
    pub(in crate::filesystem) maximum_bytes: u64,
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
        self.verifier
            .verify(&self.scope, &mut Some(&mut io.history_io()))
    }

    /// Binds authenticated read authority with optional preparation accounting.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Arc<Self>> {
        if maximum_bytes == 0 {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "content admission limit must be positive",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let (verifier, scope) = crate::executor::with_history_io(io, |work| -> Result<_> {
            verifier.verify(scope, work)?;
            Ok((verifier.copy(work)?, scope.copy(work)?))
        })?;
        if let Some(io) = io {
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
            io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
            let authority_bytes = (std::mem::size_of::<AuthorityVerifier>()
                + std::mem::size_of::<Scope>()
                + 4 * std::mem::size_of::<usize>()) as u64;
            io.retain_owned(authority_bytes)?;
            io.allocation_attempt(1)?;
            io.allocation_attempt(1)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        let _native_metadata = match io.filter(|io| io.original_native_work().is_some()) {
            Some(io) => {
                io.retain_owned(
                    (std::mem::size_of::<acyclic_fs::NativeAllocation>()
                        + 2 * std::mem::size_of::<usize>()) as u64,
                )?;
                io.allocation_attempt(1)?;
                Some(Arc::new(io.take_native_custody(io.owned_since(before)?)?))
            }
            None => None,
        };
        Ok(Arc::new(Self {
            host,
            verifier: Arc::new(verifier),
            scope: Arc::new(scope),
            creation: None,
            exact_file: None,
            maximum_bytes,
            #[cfg(not(target_arch = "wasm32"))]
            _native_metadata,
        }))
    }
}

impl<A, O> ContentResidencyVerifier for FilesystemContentVerifier<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn supports_read(&self, volume: &VolumeRef, io: Option<&PreparationIo>) -> Result<bool> {
        if self
            .exact_file
            .as_ref()
            .is_some_and(|file| file.volume() != volume)
            || self.exact_file.is_none()
                && self
                    .creation
                    .as_ref()
                    .is_some_and(|creation| creation.volume() != volume)
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
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.read_file(reference, maximum_bytes, io)
        })
    }

    fn read_range<'a>(
        &'a self,
        reference: &'a FileRef,
        range: acyclic_fs::ByteRange,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.verify_creation(reference.volume(), Some(reference), io)
                .await?;
            if reference.descriptor().byte_length() > self.maximum_bytes {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "range source exceeds provider limit",
                ));
            }
            if self.exact_file.is_none() {
                let owned;
                let authority_io = if let Some(io) = io {
                    io
                } else {
                    self.verifier.verify(&self.scope, &mut None)?;
                    owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                    &owned
                };
                require_file_read(&self.scope, reference, authority_io)?;
            }
            self.host.read_pinned_range(reference, range, io).await
        })
    }

    fn verify<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            drop(self.read_file(reference, maximum_bytes, io).await?);
            if let Some(io) = io {
                io.remaining()?;
            }
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
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.verify_creation(volume, None, io).await?;
            if maximum_bytes == 0 || maximum_bytes > self.maximum_bytes {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "prefix read exceeds provider limit",
                ));
            }
            let grant = crate::executor::with_history_io(io, |work| {
                ContentGrant::verify_directory_read(
                    &self.verifier,
                    &self.scope,
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
                .read_private_prefix(volume, &grant, path, generation, maximum_bytes, io)
                .await?;
            drop(grant);
            drop(grant_hold);
            Ok(bytes)
        })
    }

    fn list_private_directory<'a>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
        after: Option<&'a str>,
        maximum_entries: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, PrivateDirectoryPage>> {
        if io.is_some() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "finite private directory discovery is unavailable",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.verify_creation(volume, None, None).await?;
            let after = after
                .map(|name| {
                    LogicalName::new(NameEncoding::Utf8, name.as_bytes().to_vec(), u32::MAX)
                        .map_err(|error| Error::Invalid(error.to_string()))
                })
                .transpose()?;
            let (generation, page) = FilesystemContentVerifier::list_private_directory(
                self,
                volume,
                granted_prefix,
                path,
                expected_generation,
                after.as_ref(),
                maximum_entries,
            )
            .await?;
            let entries = page
                .entries
                .into_iter()
                .map(|entry| {
                    let name = entry
                        .name
                        .unicode_text(
                            acyclic_fs::WorkBudget::UNBOUNDED,
                            &acyclic_fs::CancellationToken::new(),
                        )
                        .ok()
                        .map(|receipt| receipt.value)
                        .ok_or_else(|| {
                            Error::Unsupported("private directory name is not Unicode".into())
                        })?;
                    let kind = match entry.kind {
                        FileKind::Regular => PrivateDirectoryEntryKind::File,
                        FileKind::Directory => PrivateDirectoryEntryKind::Directory,
                        _ => {
                            return Err(Error::Unsupported(
                                "private directory contains an unsupported entry kind".into(),
                            ));
                        }
                    };
                    Ok(PrivateDirectoryEntry { name, kind })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(PrivateDirectoryPage {
                generation,
                entries,
                has_more: page.has_more,
            })
        })
    }

    fn read_private_path<'a, 'io>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
        maximum_bytes: u64,
        io: Option<&'io PreparationIo>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (
                FileRef,
                acyclic_stream::OwnedBytes,
                Option<crate::conversation::ContentHold<'io>>,
            ),
        >,
    >
    where
        'io: 'a,
    {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.verify_creation(volume, None, io).await?;
            let mut history = io.map(PreparationIo::history_io);
            let grant = ContentGrant::verify_directory_read(
                &self.verifier,
                &self.scope,
                volume,
                granted_prefix,
                &mut history
                    .as_mut()
                    .map(|observer| observer as &mut dyn crate::store::history::HistoryIo),
            )?;
            let grant_hold = io
                .map(|io| io.guard_owned(grant.logical_owned_bytes()?))
                .transpose()?;
            let (file, bytes) = self
                .host
                .read_private_path(
                    volume,
                    &grant,
                    path,
                    expected_generation,
                    maximum_bytes.min(self.maximum_bytes),
                    io,
                )
                .await?;
            drop(grant);
            drop(grant_hold);
            let hold = io
                .map(|io| io.guard_owned(file.logical_owned_bytes()?))
                .transpose()?;
            Ok((file, bytes, hold))
        })
    }
}

impl<A, O> FilesystemContentVerifier<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(in crate::filesystem) async fn verify_creation(
        &self,
        volume: &VolumeRef,
        file: Option<&FileRef>,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if let Some(bound) = &self.exact_file
            && file != Some(bound)
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "created reference handle names another immutable file",
            ));
        }
        if let Some(creation) = &self.creation {
            creation
                .verify(
                    &self.host,
                    self.verifier.audience(),
                    volume,
                    self.exact_file.as_ref(),
                    io,
                )?
                .await?;
        }
        Ok(())
    }

    pub(in crate::filesystem) fn reference_reader(
        self: &Arc<Self>,
        file: &FileRef,
        io: &PreparationIo,
    ) -> Result<Arc<Self>> {
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
        Ok(Arc::new(Self {
            host: self.host.clone(),
            verifier: self.verifier.clone(),
            scope: self.scope.clone(),
            maximum_bytes: self.maximum_bytes,
            creation: self.creation.clone(),
            exact_file: Some(file.copy(&mut Some(&mut io.history_io()))?),
            #[cfg(not(target_arch = "wasm32"))]
            _native_metadata: self._native_metadata.clone(),
        }))
    }

    pub(in crate::filesystem) async fn read_file(
        &self,
        reference: &FileRef,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let temporary = PreparationIo::temporary(io)?;
        let io = temporary.as_ref().or(io);
        self.verify_creation(reference.volume(), Some(reference), io)
            .await?;
        if self.exact_file.is_some() {
            return self
                .host
                .read_pinned_io(reference, maximum_bytes.min(self.maximum_bytes), io)
                .await;
        }
        if io.is_none() {
            self.verifier.verify(&self.scope, &mut None)?;
        }
        let unmeasured;
        let authority_io = if let Some(io) = io {
            io
        } else {
            unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &unmeasured
        };
        require_file_read(&self.scope, reference, authority_io)?;
        let bytes = self
            .host
            .read_pinned_io(reference, maximum_bytes.min(self.maximum_bytes), io)
            .await?;
        if io.is_none() {
            self.host.retain_file_generation(reference).await?;
        }
        Ok(bytes)
    }

    /// Lazily lists another agent's private directory under this reader's signed, owner-delegated subtree grant.
    pub async fn list_private_directory(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<(GenerationRef, DirectoryPage)> {
        self.verify_creation(volume, None, None).await?;
        let grant = ContentGrant::verify_directory_read(
            &self.verifier,
            &self.scope,
            volume,
            granted_prefix,
            &mut None,
        )?;
        self.host
            .list_private_directory(
                volume,
                &grant,
                path,
                expected_generation,
                after,
                maximum_entries,
            )
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
        context: &'a crate::runtime::TaskContext,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
        after: Option<&'a str>,
        maximum_entries: u32,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            ((GenerationRef, DirectoryPage), acyclic_fs::NativeAllocation),
        >,
    > {
        self.supports_native_directory_list(volume, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.verify_creation(volume, None, Some(&PreparationIo::from_native_work(work)?))
                .await?;
            context.verify_original_execution_owner(work).await?;
            let mut observer = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(work)?,
            );
            let grant = ContentGrant::verify_directory_read(
                &self.verifier,
                &self.scope,
                volume,
                granted_prefix,
                &mut Some(&mut observer),
            )?;
            let grant_custody = observer.into_custody()?;
            if path.len() > context.scope().limits().path_bytes
                || after.is_some_and(|name| name.len() > context.scope().limits().path_bytes)
                || (after.is_some() && expected_generation.is_none())
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "directory cursor or path exceeds its issued task boundary",
                });
            }
            let cursor_header = work
                .claim(
                    if after.is_some() {
                        std::mem::size_of::<LogicalName>() as u64
                    } else {
                        0
                    },
                    0,
                    acyclic_fs::WorkCounters::UNCHARGED,
                )
                .map_err(Error::from)?;
            let cursor_custody;
            let cursor = if let Some(name) = after {
                work.charge(acyclic_fs::WorkCounters {
                    items_examined: name.len() as u64 + 1,
                    ..Default::default()
                })
                .map_err(Error::from)?;
                let (bytes, custody) = work.copy_bytes(name.as_bytes()).map_err(Error::from)?;
                cursor_custody = Some(custody);
                Some(
                    LogicalName::new(NameEncoding::Utf8, bytes, u32::MAX).map_err(|_| {
                        Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Invalid,
                            reason: "private directory cursor is not a logical child name",
                        }
                    })?,
                )
            } else {
                cursor_custody = None;
                None
            };
            let result = self
                .host
                .list_private_directory_guarded_with_native_work(
                    volume,
                    &grant,
                    path,
                    expected_generation,
                    cursor.as_ref(),
                    maximum_entries,
                    work,
                    || async {
                        self.supports_native_directory_list(volume, work)?;
                        let io = PreparationIo::from_native_work(work)?;
                        let directory = io.contains_capability(
                            context.scope().grants(),
                            &(volume, granted_prefix),
                            acyclic_fs::access::DIRECTORY_READ_PREFIX,
                        )?;
                        let whole_volume = io.contains_capability(
                            context.scope().grants(),
                            volume,
                            acyclic_fs::access::VolumeOperation::Read.capability_prefix(),
                        )?;
                        if !directory && !whole_volume {
                            return Err(Error::NativeBoundary {
                                code: crate::wire::ErrorCode::Unauthorized,
                                reason: "issued task scope does not retain this directory read",
                            });
                        }
                        context.verify_original_execution_owner(work).await
                    },
                )
                .await;
            drop(cursor);
            drop(cursor_custody);
            drop(cursor_header);
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
    let internal = crate::conversation::is_internal_path(reference.path());
    let inherited = crate::conversation::is_inherited_context_path(reference.path());
    let internal_owner = reference.path().starts_with(".system/") && !inherited;
    if internal && !inherited && !internal_owner {
        return Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Unauthorized,
            "internal storage file is not public content",
        ));
    }
    if io.contains_capability(
        scope.capabilities(),
        reference.volume(),
        acyclic_fs::access::VolumeOperation::Read.capability_prefix(),
    )? {
        return Ok(());
    }
    if internal_owner {
        return Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Unauthorized,
            "volume operation is not granted",
        ));
    }
    if io.contains_capability(
        scope.capabilities(),
        reference,
        acyclic_fs::access::FILE_READ_PREFIX,
    )? {
        return Ok(());
    }
    if !internal
        && reference.volume().class() == VolumeClass::AgentPrivate
        && scope.agent().is_some()
    {
        if io.contains_capability(
            scope.capabilities(),
            &(reference.volume(), ""),
            acyclic_fs::access::DIRECTORY_READ_PREFIX,
        )? {
            return Ok(());
        }
        for prefix in reference
            .path()
            .match_indices('/')
            .filter_map(|(end, _)| reference.path().get(..end))
        {
            if io.contains_capability(
                scope.capabilities(),
                &(reference.volume(), prefix),
                acyclic_fs::access::DIRECTORY_READ_PREFIX,
            )? {
                return Ok(());
            }
        }
    }
    Err(stock_boundary(
        Some(io),
        crate::wire::ErrorCode::Unauthorized,
        "file read is not granted by its owner",
    ))
}

use crate::executor::stock_boundary;
