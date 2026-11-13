//! Explicit original-meter capability over the same blob engine.
#[cfg(not(target_arch = "wasm32"))]
use crate::{
    ByteRange, Digest, NativeObjectsError, Object, ObjectsBatch, ObjectsNativeFuture,
    ObjectsNativeRequest, ObjectsPutReceipt, Retained, RootId, Sealed,
};

/// Blob operations retaining the caller's original native allocation and work
/// owners. Providers advertise this capability through [`crate::Blobs::native`].
/// The capability exposes no native operations on WebAssembly.
pub trait NativeBlobs: acyclic_stream::ProviderPlatform {
    /// Checks complete blob presence under the caller's original meter.
    /// Every physical read is admitted before dispatch; cancellation and
    /// physical failures remain errors. Only absence returns `false`.
    /// Temporary head metadata retires within the call.
    #[cfg(not(target_arch = "wasm32"))]
    fn contains_native(
        &self,
        request: ObjectsNativeRequest<Digest>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, bool>, NativeObjectsError>;

    /// Reads under the caller's original allocation owner.
    /// Decoded metadata is admitted before allocation and retains its original
    /// credit through the returned object's lifetime. Body aliases retain
    /// their independent original backing credit.
    #[cfg(not(target_arch = "wasm32"))]
    fn get_native(
        &self,
        request: ObjectsNativeRequest<(Digest, ByteRange, u64)>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, Object>, NativeObjectsError>;

    /// Reads a batch under the caller's original allocation owner.
    /// Each returned object retains its decoded metadata as in [`NativeBlobs::get_native`].
    #[cfg(not(target_arch = "wasm32"))]
    fn get_batch_native(
        &self,
        requests: ObjectsNativeRequest<Vec<(Digest, ByteRange, u64)>>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<Object>>, NativeObjectsError>;

    /// Stores under the caller's original allocation owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_native(
        &self,
        request: ObjectsNativeRequest<(Sealed, Vec<Digest>)>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsPutReceipt>, NativeObjectsError>;

    /// Stores a batch under the caller's original allocation owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_batch_native(
        &self,
        requests: ObjectsNativeRequest<Vec<(Sealed, Vec<Digest>)>>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<ObjectsPutReceipt>>, NativeObjectsError>;

    /// Retains a batch under `root` within the caller's original work: every
    /// medium operation is admitted to, and every buffer charged on, its
    /// meter, and the call observes its cancellation.
    #[cfg(not(target_arch = "wasm32"))]
    fn retain_native(
        &self,
        request: ObjectsNativeRequest<(RootId, Vec<Digest>)>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, Retained>, NativeObjectsError>;
}
