use super::*;

/// Filesystem-backed admission barrier for version-pinned conversation content.
pub struct FilesystemContentVerifier<A, O> {
    pub(in crate::filesystem) host: Arc<FilesystemHost<A, O>>,
    pub(in crate::filesystem) verifier: AuthorityVerifier,
    pub(in crate::filesystem) scope: Scope,
    pub(in crate::filesystem) maximum_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) _native_metadata: Option<acyclic_fs::NativeAllocation>,
}

impl<A, O> FilesystemContentVerifier<A, O> {
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
        }
        #[cfg(not(target_arch = "wasm32"))]
        let _native_metadata = match io.filter(|io| io.original_native_work().is_some()) {
            Some(io) => Some(
                io.take_native_custody(
                    io.owned_bytes()?
                        .checked_sub(before)
                        .ok_or_else(|| io.invalid("content verifier custody underflows"))?,
                )?,
            ),
            None => None,
        };
        Ok(Arc::new(Self {
            host,
            verifier,
            scope,
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
                        .unicode_text()
                        .ok_or_else(|| {
                            Error::Unsupported("private directory name is not Unicode".into())
                        })?
                        .into_owned();
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
    pub(in crate::filesystem) async fn read_file(
        &self,
        reference: &FileRef,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        if io.is_none() {
            self.verifier.verify(&self.scope, &mut None)?;
        }
        let unmeasured;
        let authority_io = match io {
            Some(io) => io,
            None => {
                unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                &unmeasured
            }
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
        if !self
            ._native_metadata
            .as_ref()
            .is_some_and(|guard| guard.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "directory verifier lacks original native metadata custody",
            });
        }
        self.verifier.verify(
            &self.scope,
            &mut Some(&mut crate::store::history::native_work_io(work)?),
        )?;
        volume.validate(&mut Some(&mut crate::store::history::native_work_io(work)?))?;
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
            context
                .verify_original_execution_owner_with_native_work(work)?
                .await?;
            let mut observer = crate::store::history::native_work_io(work)?;
            let grant = ContentGrant::verify_directory_read(
                &self.verifier,
                &self.scope,
                volume,
                granted_prefix,
                &mut Some(&mut observer),
            )?;
            let grant_custody = observer.into_custody();
            grant.require_directory_path(
                volume,
                path,
                Some(&PreparationIo::from_native_work(work)?),
            )?;
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
                        grant.require_directory_path(
                            volume,
                            path,
                            Some(&PreparationIo::from_native_work(work)?),
                        )?;
                        let io = PreparationIo::from_native_work(work)?;
                        let directory = io.contains_capability(
                            context.scope().grants(),
                            &(volume, granted_prefix),
                            "directory:read:",
                        )?;
                        let whole_volume = io.contains_capability(
                            context.scope().grants(),
                            volume,
                            "volume:read:",
                        )?;
                        if !directory && !whole_volume {
                            return Err(Error::NativeBoundary {
                                code: crate::wire::ErrorCode::Unauthorized,
                                reason: "issued task scope does not retain this directory read",
                            });
                        }
                        context
                            .verify_original_execution_owner_with_native_work(work)?
                            .await
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
    if io.contains_capability(scope.capabilities(), reference.volume(), "volume:read:")? {
        return Ok(());
    }
    if internal_owner {
        return Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Unauthorized,
            "volume operation is not granted",
        ));
    }
    if io.contains_capability(scope.capabilities(), reference, "file:read:")? {
        return Ok(());
    }
    if !internal
        && reference.volume().class() == VolumeClass::AgentPrivate
        && scope.agent().is_some()
    {
        if io.contains_capability(
            scope.capabilities(),
            &(reference.volume(), ""),
            "directory:read:",
        )? {
            return Ok(());
        }
        for (end, _) in reference.path().match_indices('/') {
            let prefix = &reference.path()[..end];
            if io.contains_capability(
                scope.capabilities(),
                &(reference.volume(), prefix),
                "directory:read:",
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

use crate::executor::{stock_boundary};
