//! provider fork protocol.
use super::*;
/// One provider's admission barrier for prepared fork state.
pub trait ForkSeedVerifier: acyclic_stream::ProviderPlatform {
    /// Identifies the provider proved by this verifier.
    fn provider(&self) -> &ProviderRef;
    /// Bounds all descriptors verified during publication.
    fn maximum_reference_bytes(&self) -> u64 { MAX_FORK_REFERENCE_BYTES }
    /// Checks every selected revision owned by this provider.
    fn verify<'a, 'w: 'a>(&'a self, seed: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>>;
    /// Performs the provider's acquire private fence operation.
    fn acquire_private_fence<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ObservedForkPublicationGuard>> {
        admit_fork_future(work, |work| async move {
            Err(boundary_error(work, crate::wire::ErrorCode::Unsupported, "fork private publication fence is not bound"))
        })
    }
    /// Performs the provider's read manifest operation.
    fn read_manifest<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, _manifest: &'a FileRef,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>> {
        admit_fork_future(work, |work| async move {
            Err(boundary_error(work, crate::wire::ErrorCode::Unsupported, "fork attachment manifest reader is not bound"))
        })
    }
    /// Performs the provider's verify file operation.
    fn verify_file<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, _file: &'a FileRef,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
        admit_fork_future(work, |work| async move {
            Err(boundary_error(work, crate::wire::ErrorCode::Unsupported, "fork file verifier is not bound"))
        })
    }
    /// Performs the provider's verify boundary operation.
    fn verify_boundary<'a, 'w: 'a>(&'a self, _boundary: &'a AttestedBoundary,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
        admit_fork_future(work, |work| async move {
            Err(boundary_error(work, crate::wire::ErrorCode::Unsupported, "provider does not attest fork boundaries"))
        })
    }
}

/// A provider-owned fence retained through publication or reconciliation.
pub trait ForkPublicationGuard: acyclic_stream::ProviderPlatform {
    /// Idempotently releases the exact fence after publication.
    fn release<'a, 'w: 'a>(&'a self, work: &'a mut Option<&'w mut dyn HistoryIo>)
        -> Result<HistoryContentFuture<'a, ()>>;
}

pub(crate) fn admit_fork_future<'a, 'w: 'a, T: 'a, F, B>(
    work: &'a mut Option<&'w mut dyn HistoryIo>, build: B,
) -> Result<HistoryContentFuture<'a, T>>
where F: std::future::Future<Output = Result<T>> + acyclic_stream::ProviderTask + 'a,
    B: FnOnce(&'a mut Option<&'w mut dyn HistoryIo>) -> F,
{
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = work.as_ref().and_then(|io| io.original_native_work()).cloned() {
        return crate::NativeFutureBundle::new(&native, || build(work))
            .map(HistoryContentFuture::Native);
    }
    let frame_bytes = if let Some(io) = work.as_deref_mut() {
        let bytes = std::mem::size_of::<F>() as u64;
        io.check_cancellation()?;
        io.admit_owned(bytes)?;
        io.retain_owned(bytes)?;
        bytes
    } else { 0 };
    Ok(HistoryContentFuture::Ordinary { future: Box::pin(build(work)), frame_bytes })
}

pub(crate) fn boundary_error(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode, reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_ref().is_some_and(|work| work.original_native_work().is_some()) {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        _ => Error::Unsupported(reason.into()),
    }
}

pub(in crate::fork) fn frame_bytes<T>(future: &HistoryContentFuture<'_, T>) -> u64 {
    match future {
        HistoryContentFuture::Ordinary { frame_bytes, .. } => *frame_bytes,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        HistoryContentFuture::Native(_) => 0,
    }
}

pub(in crate::fork) fn release_frame<T>(result: Result<T>, bytes: u64,
    work: &mut Option<&mut dyn HistoryIo>) -> Result<T> {
    let released = if bytes == 0 { Ok(()) }
        else { work.as_deref_mut().expect("funded fork frame").release_owned(bytes) };
    match result { Err(error) => Err(error), Ok(value) => released.map(|()| value) }
}

pub(crate) async fn release_fork_guard_with_work(
    guard: &dyn ForkPublicationGuard, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let future = guard.release(work)?;
    let bytes = frame_bytes(&future);
    let result = future.await;
    release_frame(result, bytes, work)
}

macro_rules! fork_provider_dispatcher {
    ($name:ident, $operation:ident, $input:ident : $input_type:ty, $output:ty $(, $context:ident : $context_type:ty)?) => {
        pub(in crate::fork) async fn $name(verifier: &dyn ForkSeedVerifier, $($context: &$context_type,)?
            $input: &$input_type, work: &mut Option<&mut dyn HistoryIo>) -> Result<$output> {
            let future = verifier.$operation($($context,)? $input, work)?;
            let bytes = frame_bytes(&future);
            let result = future.await;
            release_frame(result, bytes, work)
        }
    };
}
pub(in crate::fork) use fork_provider_dispatcher;

pub(crate) struct ObservedForkPublicationGuard {
    pub(crate) guard: Box<dyn ForkPublicationGuard>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) _allocation: Option<acyclic_fs::NativeAllocation>,
}

pub(in crate::fork) async fn acquire_fork_fence(verifier: &dyn ForkSeedVerifier, seed: &ForkSeed,
    work: &mut Option<&mut dyn HistoryIo>) -> Result<ObservedForkPublicationGuard> {
    let future = verifier.acquire_private_fence(seed, work)?;
    let bytes = frame_bytes(&future);
    let result = future.await;
    let guard = release_frame(result, bytes, work)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = work.as_ref().and_then(|io| io.original_native_work()) {
        if !guard._allocation.as_ref().is_some_and(|allocation| allocation.work().shares_ledger(native)) {
            let ObservedForkPublicationGuard { guard, _allocation } = guard;
            drop(guard);
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: _allocation,
            });
        }
    }
    Ok(guard)
}

fork_provider_dispatcher!(verify_fork_provider_with_work, verify, seed: ForkSeed, ());

fork_provider_dispatcher!(verify_fork_file_with_work, verify_file, file: FileRef, (), seed: ForkSeed);

fork_provider_dispatcher!(verify_fork_boundary_with_work, verify_boundary, boundary: AttestedBoundary, ());

pub(in crate::fork) async fn read_fork_manifest(verifier: &dyn ForkSeedVerifier, seed: &ForkSeed,
    manifest: &FileRef, work: &mut Option<&mut dyn HistoryIo>) -> Result<(acyclic_stream::OwnedBytes, u64)> {
    let future = verifier.read_manifest(seed, manifest, work)?;
    let frame = frame_bytes(&future);
    let result = future.await;
    let retained = result.as_ref().map_or(0, |(_, retained)| *retained);
    let result = release_frame(result, frame, work);
    if result.is_err() && retained != 0 {
        let _ = work.as_deref_mut().expect("retained manifest observer").release_owned(retained);
    }
    result
}
