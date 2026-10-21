use super::*;

/// Optional content-operation accounting.
#[cfg(feature = "filesystem")]
pub type ContentIo = crate::filesystem::PreparationIo;
/// Optional content-operation accounting.
#[cfg(not(feature = "filesystem"))]
pub type ContentIo = ();
/// Retained content metadata ownership.
#[cfg(feature = "filesystem")]
pub type ContentHold<'a> = crate::filesystem::DecodeReservation<'a>;
/// Retained content metadata ownership.
#[cfg(not(feature = "filesystem"))]
pub type ContentHold<'a> = std::marker::PhantomData<&'a ()>;

/// An operation future whose allocation remains owned until its physical drop.
pub struct BoundedContentFuture<'a, T> {
    pub(super) future: BoxFuture<'a, Result<T>>,
    #[cfg(feature = "filesystem")]
    pub(super) _hold: Option<ContentHold<'a>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) _native: Option<acyclic_fs::NativeAllocation>,
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
thread_local! {
    static PAD_FRAME: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl<'a, T> BoundedContentFuture<'a, T> {
    #[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn with_padded_frame<R>(enabled: bool, operation: impl FnOnce() -> R) -> R {
        struct Reset(bool);
        impl Drop for Reset {
            fn drop(&mut self) {
                PAD_FRAME.set(self.0);
            }
        }
        let _reset = Reset(PAD_FRAME.replace(enabled));
        operation()
    }

    /// Admits the concrete future before invoking its builder.
    pub fn new<F>(io: Option<&'a ContentIo>, build: impl FnOnce() -> F) -> Result<Self>
    where
        F: std::future::Future<Output = Result<T>> + acyclic_stream::ProviderTask + 'a,
    {
        #[cfg(feature = "filesystem")]
        let mut hold = None;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.check_cancellation()?;
            let bytes = std::mem::size_of::<F>() as u64;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.original_native_work() {
                let native = if bytes == 0 {
                    work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?
                } else {
                    work.claim(
                        bytes,
                        u64::from((bytes) != 0),
                        acyclic_fs::WorkCounters::UNCHARGED,
                    )?
                };
                return Ok(Self {
                    future: Box::pin(build()),
                    _hold: None,
                    _native: Some(native),
                });
            }
            io.retain_owned(bytes)?;
            hold = Some(io.guard_owned(bytes)?);
            if bytes != 0 {
                io.allocation_attempt(bytes)?;
            }
        }
        let _ = io;
        #[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
        if PAD_FRAME.get() {
            // Size-only mutation: keep the shipping future inline in its one
            // allocation, with extra retained state across the real await.
            let future = build();
            let padding = std::hint::black_box([0u64; 1]);
            return Ok(Self {
                future: Box::pin(async move {
                    let result = future.await;
                    std::hint::black_box(padding);
                    result
                }),
                _hold: hold,
                _native: None,
            });
        }
        Ok(Self {
            future: Box::pin(build()),
            #[cfg(feature = "filesystem")]
            _hold: hold,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native: None,
        })
    }
}

impl<T> std::future::Future for BoundedContentFuture<'_, T> {
    type Output = Result<T>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}

pub(crate) fn content_error(
    io: Option<&ContentIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.is_some_and(|io| io.original_native_work().is_some()) {
        return Error::NativeBoundary { code, reason };
    }
    let _ = io;
    match code {
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        _ => Error::Unsupported(reason.into()),
    }
}

/// Authenticated, bounded immutable-content operations.
pub trait ContentResidencyVerifier: acyclic_stream::ProviderPlatform {
    /// Capability availability; conveys no read authority.
    fn supports_read(&self, _volume: &VolumeRef, io: Option<&ContentIo>) -> Result<bool> {
        if io.is_some() {
            return Err(content_error(
                io,
                crate::wire::ErrorCode::Unsupported,
                "finite content reader capability is unavailable",
            ));
        }
        Ok(true)
    }

    /// Reads an exact authorized file on the caller's existing operation ledger.
    fn read<'a>(
        &'a self,
        _reference: &'a FileRef,
        _maximum_bytes: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        Err(content_error(
            io,
            crate::wire::ErrorCode::Unsupported,
            "content byte resolution is unavailable",
        ))
    }

    /// Reads an authenticated interval without loading the remaining body.
    fn read_range<'a>(
        &'a self,
        _reference: &'a FileRef,
        _range: acyclic_fs::ByteRange,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        Err(content_error(
            io,
            crate::wire::ErrorCode::Unsupported,
            "authenticated range resolution is unavailable",
        ))
    }

    /// Checks exact content length and digest under the selected byte allowance.
    fn verify<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, ()>> {
        BoundedContentFuture::new(io, || async move {
            verified_content_bytes(self, reference, maximum_bytes, io).await?;
            Ok(())
        })
    }

    /// Loads the manifest and verifies every member through its owning provider.
    fn verify_manifest<'a>(
        &'a self,
        reference: &'a FileRef,
        item_count: u32,
        limits: &'a Limits,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, ()>> {
        BoundedContentFuture::new(io, || async move {
            verified_attachment_manifest(self, reference, item_count, limits, io).await?;
            Ok(())
        })
    }

    /// Reads an authenticated bounded prefix without fetching the remaining body.
    fn read_private_prefix<'a>(
        &'a self,
        _volume: &'a VolumeRef,
        _granted_prefix: &'a str,
        _path: &'a str,
        _generation: &'a GenerationRef,
        _maximum_bytes: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        Err(content_error(
            io,
            crate::wire::ErrorCode::Unsupported,
            "bounded prefix reads are unavailable",
        ))
    }

    /// Discovers a private subtree at a stable generation.
    #[allow(
        clippy::too_many_arguments,
        reason = "The provider receives the grant, generation and cursor as separate borrowed fields."
    )]
    fn list_private_directory<'a>(
        &'a self,
        _volume: &'a VolumeRef,
        _granted_prefix: &'a str,
        _path: &'a str,
        _expected_generation: Option<&'a GenerationRef>,
        _after: Option<&'a str>,
        _maximum_entries: u32,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, PrivateDirectoryPage>> {
        Err(content_error(
            io,
            crate::wire::ErrorCode::Unsupported,
            "private directory discovery is unavailable",
        ))
    }

    /// Resolves the named private file at the caller's pinned generation.
    fn read_private_path<'a, 'io>(
        &'a self,
        _volume: &'a VolumeRef,
        _granted_prefix: &'a str,
        _path: &'a str,
        _expected_generation: Option<&'a GenerationRef>,
        _maximum_bytes: u64,
        io: Option<&'io ContentIo>,
    ) -> Result<
        BoundedContentFuture<
            'a,
            (
                FileRef,
                acyclic_stream::OwnedBytes,
                Option<ContentHold<'io>>,
            ),
        >,
    >
    where
        'io: 'a,
    {
        Err(content_error(
            io,
            crate::wire::ErrorCode::Unsupported,
            "private path resolution is unavailable",
        ))
    }
}

/// Owner-bound, bounded publication on the caller's existing ledger.
pub trait ContentPublisher: acyclic_stream::ProviderPlatform {
    /// Resolves the exact destination before the caller checks write authority.
    /// Paths remain relative to the selected volume in returned file references.
    fn destination<'a>(
        &'a self,
        _volume: Option<&VolumeRef>,
        path: &'a str,
        _io: Option<&ContentIo>,
    ) -> Result<(&'a VolumeRef, std::borrow::Cow<'a, str>)> {
        Ok((self.volume(), std::borrow::Cow::Borrowed(path)))
    }
    /// Exact writable volume.
    fn volume(&self) -> &VolumeRef;
    /// Rejects unavailable bounded publication before external side effects.
    fn supports_stage(&self, io: Option<&ContentIo>) -> Result<()> {
        if io.is_some() {
            return Err(content_error(
                io,
                crate::wire::ErrorCode::Unsupported,
                "finite publication is unavailable",
            ));
        }
        Ok(())
    }
    /// Stages immutable bytes under one stable operation identity.
    fn stage<'a>(
        &'a self,
        _operation_id: OperationId,
        _path: &'a str,
        _bytes: &'a [u8],
        _media_type: &'a str,
        _display_name: &'a str,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        Err(content_error(
            io,
            crate::wire::ErrorCode::Unsupported,
            "file publication is unavailable",
        ))
    }
    /// Publishes one complete file contract, including its path, per operation.
    fn stage_once<'a>(
        &'a self,
        _operation_id: OperationId,
        _path: &'a str,
        _bytes: &'a [u8],
        _media_type: &'a str,
        _display_name: &'a str,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        Err(content_error(
            io,
            crate::wire::ErrorCode::Unsupported,
            "operation-bound file publication is unavailable",
        ))
    }
    /// Observes the original once-publication without substituting a mutable head.
    fn observe_staged_once<'a>(
        &'a self,
        _operation_id: OperationId,
        _maximum_bytes: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, (Option<FileRef>, Option<ContentHold<'a>>)>> {
        Err(content_error(
            io,
            crate::wire::ErrorCode::Unsupported,
            "publication observation is unavailable",
        ))
    }
    /// Replaces bytes only at the immutable source generation.
    fn stage_at<'a>(
        &'a self,
        _operation_id: OperationId,
        _source: &'a FileRef,
        _bytes: &'a [u8],
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        Err(content_error(
            io,
            crate::wire::ErrorCode::Unsupported,
            "generation-checked publication is unavailable",
        ))
    }
}

/// Resolves an owner-mediated reader when a volume has not been explicitly bound.
pub trait ContentMountResolver: acyclic_stream::ProviderPlatform {
    /// Lazily opens the owner for one exact ref and returns its independently verified caller grant.
    fn mount<'a>(
        &'a self,
        reference: &'a FileRef,
    ) -> BoxFuture<'a, Result<(ContentGrant, Arc<dyn ContentResidencyVerifier>)>>;
}

/// Routes admission reads by exact owner volume, including several agents on one provider and members stored separately from their manifests.
pub struct CompositeContentVerifier {
    pub(super) volumes: BTreeMap<VolumeRef, Arc<dyn ContentResidencyVerifier>>,
    pub(super) mount: Option<Arc<dyn ContentMountResolver>>,
}

impl CompositeContentVerifier {
    fn bound_reader(
        &self,
        volume: &VolumeRef,
        io: Option<&ContentIo>,
    ) -> Result<&dyn ContentResidencyVerifier> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            volume.validate(&mut Some(&mut io.history_io()))?;
            return self.volumes.get(volume).map(AsRef::as_ref).ok_or_else(|| content_error(
                Some(io), crate::wire::ErrorCode::Unsupported,
                "finite content routing requires an explicitly bound owner; lazy mount lacks finite IO"));
        }
        let _ = io;
        volume.validate(&mut None)?;
        self.volumes.get(volume).map(AsRef::as_ref).ok_or_else(|| {
            Error::Unsupported(
                if self.mount.is_some() {
                    "private directory mount is unavailable"
                } else {
                    "content volume is not registered"
                }
                .into(),
            )
        })
    }

    /// Rejects duplicate exact-volume bindings instead of silently replacing one.
    pub fn new(
        bindings: Vec<(VolumeRef, Arc<dyn ContentResidencyVerifier>)>,
        mount: Option<Arc<dyn ContentMountResolver>>,
    ) -> Result<Self> {
        let mut volumes = BTreeMap::new();
        for (volume, resolver) in bindings {
            volume.validate(&mut None)?;
            if volumes.insert(volume, resolver).is_some() {
                return Err(Error::Invalid("content volume is registered twice".into()));
            }
        }
        Ok(Self { volumes, mount })
    }
}

impl ContentResidencyVerifier for CompositeContentVerifier {
    fn supports_read(&self, volume: &VolumeRef, _io: Option<&ContentIo>) -> Result<bool> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = _io {
            return self
                .bound_reader(volume, Some(io))?
                .supports_read(volume, Some(io));
        }
        volume.validate(&mut None)?;
        Ok(self.volumes.contains_key(volume) || self.mount.is_some())
    }
    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            return self.bound_reader(reference.volume(), Some(io))?.read(
                reference,
                maximum_bytes,
                Some(io),
            );
        }
        reference.validate(&mut None)?;
        if let Some(reader) = self.volumes.get(reference.volume()) {
            return reader.read(reference, maximum_bytes, io);
        }
        let mount = self
            .mount
            .as_ref()
            .ok_or_else(|| Error::Unsupported("content volume is not registered".into()))?;
        BoundedContentFuture::new(io, || async move {
            let (grant, reader) = mount.mount(reference).await?;
            grant.require_file_read(reference, None)?;
            let bytes = reader.read(reference, maximum_bytes, io)?.await?;
            Ok(bytes)
        })
    }
    fn read_range<'a>(
        &'a self,
        reference: &'a FileRef,
        range: acyclic_fs::ByteRange,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        if io.is_some() || self.volumes.contains_key(reference.volume()) {
            return self
                .bound_reader(reference.volume(), io)?
                .read_range(reference, range, io);
        }
        reference.validate(&mut None)?;
        let mount = self
            .mount
            .as_ref()
            .ok_or_else(|| Error::Unsupported("content volume is not registered".into()))?;
        BoundedContentFuture::new(io, || async move {
            let (grant, reader) = mount.mount(reference).await?;
            grant.require_file_read(reference, None)?;
            reader.read_range(reference, range, io)?.await
        })
    }

    fn read_private_prefix<'a>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        generation: &'a GenerationRef,
        maximum_bytes: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.bound_reader(volume, io)?.read_private_prefix(
            volume,
            granted_prefix,
            path,
            generation,
            maximum_bytes,
            io,
        )
    }
    fn list_private_directory<'a>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
        after: Option<&'a str>,
        maximum_entries: u32,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, PrivateDirectoryPage>> {
        self.bound_reader(volume, io)?.list_private_directory(
            volume,
            granted_prefix,
            path,
            expected_generation,
            after,
            maximum_entries,
            io,
        )
    }
    fn read_private_path<'a, 'io>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
        maximum_bytes: u64,
        io: Option<&'io ContentIo>,
    ) -> Result<
        BoundedContentFuture<
            'a,
            (
                FileRef,
                acyclic_stream::OwnedBytes,
                Option<ContentHold<'io>>,
            ),
        >,
    >
    where
        'io: 'a,
    {
        self.bound_reader(volume, io)?.read_private_path(
            volume,
            granted_prefix,
            path,
            expected_generation,
            maximum_bytes,
            io,
        )
    }
}
