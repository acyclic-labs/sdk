//! Logical bucket/key Objects contract replacing public version history and snapshots.
pub mod conformance;
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub mod http;
#[cfg(feature = "json")]
pub mod json;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub mod local;
mod memory;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) use memory::persistence::Publication;
pub mod request;
pub mod response;
#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
mod upload;

/// Canonical HTTP route, input message, and output message inventory, relative to `/v1/objects/`.
pub const HTTP_ROUTES: &[(&str, &str, &str)] = &[
    ("buckets/create", "CreateBucketRequest", "Bucket"),
    ("buckets/head", "HeadBucketRequest", "Bucket"),
    (
        "buckets/delete",
        "DeleteBucketRequest",
        "DeleteBucketResponse",
    ),
    ("objects/put", "PutObjectRequest", "ObjectInfo"),
    ("objects/get", "GetObjectRequest", "GetObjectResponse"),
    ("objects/head", "HeadObjectRequest", "HeadObjectResponse"),
    (
        "objects/delete",
        "DeleteObjectRequest",
        "DeleteObjectResponse",
    ),
    ("objects/list", "ListObjectsRequest", "ListObjectsResponse"),
    (
        "multipart/create",
        "CreateMultipartRequest",
        "MultipartUpload",
    ),
    ("multipart/upload-part", "UploadPartRequest", "UploadedPart"),
    (
        "multipart/list-parts",
        "ListPartsRequest",
        "ListPartsResponse",
    ),
    (
        "multipart/complete",
        "CompleteMultipartRequest",
        "ObjectInfo",
    ),
    (
        "multipart/abort",
        "AbortMultipartRequest",
        "AbortMultipartResponse",
    ),
];
pub use memory::{MemoryObjects, MemoryOptions};

/// Native composition capability for filesystem storage over logical Objects.
#[async_trait::async_trait]
#[doc(hidden)]
pub trait NativeBatchObjects: ObjectsProvider {
    /// Publishes ordered, individually atomic mutations in one native admission.
    async fn put_batch(
        &self,
        requests: Vec<(wire::PutObjectHeader, acyclic_stream::OwnedBytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>>;

    /// Reads ordered, individually bounded selections under one native admission.
    async fn get_batch(
        &self,
        requests: Vec<(wire::GetObjectRequest, u64)>,
    ) -> Vec<Result<Object, Error>>;

    /// Reads a genuine batch using the caller's original allocation owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn get_batch_native(
        &self,
        _requests: ObjectsNativeRequest<Vec<(wire::GetObjectRequest, u64)>>,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<Object>>, NativeObjectsError> {
        drop(_requests);
        Err(NativeObjectsError::Unsupported)
    }

    /// Publishes a genuine batch with the current caller's original admissions.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_batch_native(
        &self,
        _requests: ObjectsNativeRequest<Vec<(wire::PutObjectHeader, acyclic_stream::OwnedBytes)>>,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<ObjectsPutReceipt>>, NativeObjectsError> {
        drop(_requests);
        Err(NativeObjectsError::Unsupported)
    }
}
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
pub mod grpc;
#[cfg(all(test, feature = "grpc", not(target_arch = "wasm32")))]
mod grpc_tests;
#[cfg(all(test, feature = "http", not(target_arch = "wasm32")))]
mod http_tests;
#[cfg(test)]
mod tests;

/// Generated Objects v1 public request, response and transport bindings.
#[allow(
    missing_docs,
    clippy::all,
    clippy::pedantic,
    clippy::allow_attributes_without_reason,
    reason = "generated prost and tonic bindings"
)]
pub mod wire {
    include!("../generated/acyclic.objects.v1.rs");
}

/// Largest caller-supplied PEM CA, client certificate, or private key.
#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
const MAX_PEM_BYTES: usize = 64 * 1024;
/// Largest bearer credential, in bytes, that an SDK client accepts.
pub const MAX_BEARER_TOKEN_BYTES: usize = 12 * 1024;

/// Canonical Objects v1 descriptor, including streaming service definitions.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../generated/acyclic-objects-v1.bin");

/// Canonical semantic failure; infrastructure diagnostics remain operator-only.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("Objects operation failed ({code:?})")]
pub struct Error {
    /// Public semantic category, suitable for programmatic handling.
    pub code: wire::ErrorCode,
}
impl From<wire::ErrorCode> for Error {
    fn from(code: wire::ErrorCode) -> Self {
        Self { code }
    }
}

/// An original native admission failure or the service's semantic category.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, thiserror::Error)]
pub enum NativeObjectsError {
    /// The unchanged logical service failure.
    #[error(transparent)]
    Service(#[from] Error),
    /// The original meter's typed admission, cancellation or I/O failure.
    #[error(transparent)]
    Admission(#[from] std::io::Error),
    /// A custom provider has no original-meter immutable read producer.
    #[error("provider has no native immutable read producer")]
    Unsupported,
    /// A failed real read retaining its completed bytes and original metadata custody.
    #[error(transparent)]
    ReadFailure(NativeObjectsFailure),
    /// One actual grouped publication failure shared by its ordered result slots.
    #[error(transparent)]
    SharedFailure(std::sync::Arc<NativeObjectsSharedFailure>),
}

/// The exact native failure category, independent of retained physical payloads.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, thiserror::Error)]
pub enum NativeObjectsCause {
    /// The unchanged logical service failure.
    #[error(transparent)]
    Service(Error),
    /// The original native admission, cancellation or I/O error.
    #[error(transparent)]
    Admission(std::io::Error),
    /// The custom provider does not implement the original-meter producer.
    #[error("provider has no native immutable read producer")]
    Unsupported,
    /// The unchanged actual failure shared by a grouped publication.
    #[error(transparent)]
    SharedFailure(std::sync::Arc<NativeObjectsSharedFailure>),
}

/// One preadmitted actual grouped failure, retaining its original cause to last alias.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct NativeObjectsSharedFailure {
    error: std::sync::OnceLock<NativeObjectsError>,
    _allocation: acyclic_native_runtime::ReadAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeObjectsSharedFailure {
    pub(crate) fn prepare(
        meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<std::sync::Arc<Self>, NativeObjectsError> {
        let allocation = acyclic_native_runtime::ReadAllocation::new(meter, std::mem::size_of::<Self>())?;
        Ok(std::sync::Arc::new(Self { error: std::sync::OnceLock::new(), _allocation: allocation }))
    }

    pub(crate) fn set(&self, error: NativeObjectsError) -> Result<(), NativeObjectsError> {
        self.error.set(error)
    }

    /// Borrows the same original typed cause; published aliases are always initialized.
    pub fn error(&self) -> &NativeObjectsError {
        self.error.get().expect("only an initialized original failure is published")
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Display for NativeObjectsSharedFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.error(), formatter)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl std::error::Error for NativeObjectsSharedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { Some(self.error()) }
}

/// A failed actual read, with physical data retiring before its original claims.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, thiserror::Error)]
#[error("{cause}")]
pub struct NativeObjectsFailure {
    /// The primary unflattened error category.
    pub cause: NativeObjectsCause,
    /// The actual decoded object, intrinsically retaining its header custody.
    pub object: Option<Object>,
    /// An additional actual decoded object retained by a nested transport failure.
    pub secondary_object: Option<Object>,
    /// The prepared buffer's completed prefix when no decoded object owns it.
    pub partial: Option<acyclic_stream::OwnedBytes>,
    /// An actual decoded acknowledgement retained by a failed publication.
    pub receipt: Option<ObjectsPutReceipt>,
    // An actual malformed SDK wire buffer, inseparable from its original claim.
    wire: Option<(Vec<u8>, acyclic_native_runtime::ReadAllocation)>,
    /// An opaque transport chunk retained on a failed bounded collection.
    pub transport_chunk: Option<bytes::Bytes>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeObjectsFailure {
    /// Borrows retained malformed wire data without detaching its original custody.
    pub fn wire_bytes(&self) -> Option<&[u8]> {
        self.wire.as_ref().map(|(bytes, _)| bytes.as_slice())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeObjectsError {
    /// Destroys retained physical data before returning its original typed cause.
    pub fn into_cause(self) -> NativeObjectsCause {
        match self {
            Self::Service(error) => NativeObjectsCause::Service(error),
            Self::Admission(error) => NativeObjectsCause::Admission(error),
            Self::Unsupported => NativeObjectsCause::Unsupported,
            Self::SharedFailure(failure) => NativeObjectsCause::SharedFailure(failure),
            Self::ReadFailure(failure) => {
                let NativeObjectsFailure { cause, object, secondary_object, partial, receipt, wire, transport_chunk } = failure;
                drop(object);
                drop(secondary_object);
                drop(partial);
                drop(receipt);
                if let Some((bytes, allocation)) = wire { drop(bytes); drop(allocation); }
                drop(transport_chunk);
                cause
            }
        }
    }

    /// Retains the actual malformed wire buffer without copying or adopting it.
    pub fn with_wire(self, bytes: Vec<u8>, allocation: acyclic_native_runtime::ReadAllocation) -> Self {
        match self {
            Self::ReadFailure(mut failure) => {
                if let Some((old_bytes, old_allocation)) = failure.wire.take() {
                    drop(old_bytes);
                    drop(old_allocation);
                }
                failure.wire = Some((bytes, allocation));
                Self::ReadFailure(failure)
            }
            error => Self::ReadFailure(NativeObjectsFailure {
                cause: error.into_cause(), object: None, secondary_object: None, partial: None, receipt: None,
                wire: Some((bytes, allocation)), transport_chunk: None,
            }),
        }
    }

    /// Retains the actual decoded header behind its preadmitted original claim.
    pub fn with_header(self, header: wire::GetObjectHeader, allocation: acyclic_native_runtime::ReadAllocation) -> Self {
        let object = Object::new_native(header, acyclic_stream::OwnedBytes::new(), Some(allocation));
        match self {
            Self::ReadFailure(mut failure) => {
                failure.secondary_object = failure.object.take();
                failure.object = Some(object);
                Self::ReadFailure(failure)
            }
            error => Self::ReadFailure(NativeObjectsFailure {
                cause: error.into_cause(), object: Some(object), secondary_object: None,
                partial: None, receipt: None, wire: None, transport_chunk: None,
            }),
        }
    }

    /// Retains a decoded acknowledgement behind its preadmitted original claim.
    pub fn with_receipt(self, info: wire::ObjectInfo, allocation: acyclic_native_runtime::ReadAllocation) -> Self {
        let receipt = ObjectsPutReceipt::new(info, Some(allocation));
        match self {
            Self::ReadFailure(mut failure) => {
                failure.receipt = Some(receipt);
                Self::ReadFailure(failure)
            }
            error => Self::ReadFailure(NativeObjectsFailure {
                cause: error.into_cause(), object: None, secondary_object: None, partial: None,
                receipt: Some(receipt), wire: None, transport_chunk: None,
            }),
        }
    }
}

/// A preallocated error slot bridging existing semantic transport body streams.
#[cfg(not(target_arch = "wasm32"))]
pub struct NativeTransportErrors {
    error: std::sync::Mutex<Option<NativeObjectsError>>,
    #[cfg(feature = "grpc")]
    codec_status: std::sync::Mutex<Option<tonic::Status>>,
    wire_allocation: std::sync::Mutex<Option<acyclic_native_runtime::ReadAllocation>>,
    _allocation: acyclic_native_runtime::ReadAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeTransportErrors {
    /// Preserves the first original typed failure without allocating.
    pub fn record(&self, error: NativeObjectsError) {
        let mut slot = self.error.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_none() { *slot = Some(error); }
    }
    /// Moves the original failure without formatting, copying or allocating.
    pub fn take(&self) -> Option<NativeObjectsError> {
        self.error.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take()
    }

    /// Holds the original total known wire quota admitted before the whole RPC.
    pub(crate) fn retain_wire(
        &self, allocation: acyclic_native_runtime::ReadAllocation,
    ) -> Result<(), NativeObjectsError> {
        if !std::sync::Arc::ptr_eq(self._allocation.meter(), allocation.meter()) {
            return Err(NativeObjectsError::Admission(std::io::ErrorKind::InvalidInput.into()));
        }
        let mut slot = self.wire_allocation.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match slot.as_mut() {
            Some(current) => current.absorb(&mut { allocation })?,
            None => *slot = Some(allocation),
        }
        Ok(())
    }

    /// Transfers the same preadmitted wire quota to an actual immutable frame.
    #[cfg(feature = "grpc")]
    pub(crate) fn take_wire(
        &self, bytes: usize,
    ) -> Result<acyclic_native_runtime::ReadAllocation, NativeObjectsError> {
        let mut slot = self.wire_allocation.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        slot.as_mut().ok_or_else(|| NativeObjectsError::Admission(
            std::io::ErrorKind::InvalidData.into(),
        ))?.split(bytes).map_err(NativeObjectsError::Admission)
    }

    #[cfg(feature = "grpc")]
    pub(crate) fn prepare_codec_failure(
        &self, meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<(), NativeObjectsError> {
        meter.check()?;
        let owner = meter.allocation_owner().unwrap_or_else(|| std::sync::Arc::clone(meter));
        if !std::sync::Arc::ptr_eq(self._allocation.meter(), &owner) {
            return Err(NativeObjectsError::Admission(std::io::ErrorKind::InvalidInput.into()));
        }
        let mut slot = self.codec_status.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_none() {
            // Status' private allocation layout is opaque; its empty logical
            // payload and one real box attempt are admitted before codec I/O.
            meter.allocate_refcount_header()?;
            *slot = Some(tonic::Status::new(tonic::Code::Cancelled, ""));
        }
        Ok(())
    }

    #[cfg(feature = "grpc")]
    pub(crate) fn codec_failure(&self) -> Option<tonic::Status> {
        self.codec_status.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take()
    }
}


#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_preparation_error(error: acyclic_stream::PreparedOwnedBytesError) -> NativeObjectsError {
    match error {
        acyclic_stream::PreparedOwnedBytesError::Admission(error) => error.into(),
        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
        =>
            NativeObjectsError::Admission(std::io::ErrorKind::InvalidData.into()),
    }
}

/// One buffered selection for callers that explicitly bound allocation.
#[derive(Debug)]
pub struct Object {
    header: wire::GetObjectHeader,
    body: acyclic_stream::OwnedBytes,
    #[cfg(not(target_arch = "wasm32"))]
    header_custody: Option<acyclic_native_runtime::ReadAllocation>,
}

impl Object {
    /// Constructs an ordinary object from its existing canonical immutable body.
    pub fn new(header: wire::GetObjectHeader, body: acyclic_stream::OwnedBytes) -> Self {
        Self {
            header, body,
            #[cfg(not(target_arch = "wasm32"))]
            header_custody: None,
        }
    }

    /// Transfers metadata custody funded by the actual native producer before I/O.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_native(
        header: wire::GetObjectHeader, body: acyclic_stream::OwnedBytes,
        header_custody: Option<acyclic_native_runtime::ReadAllocation>,
    ) -> Self { Self { header, body, header_custody } }

    /// Borrows the exact response metadata without detaching its original claim.
    pub const fn header(&self) -> &wire::GetObjectHeader { &self.header }

    /// Borrows the canonical byte carrier; clones retain intrinsic byte custody.
    pub const fn body(&self) -> &acyclic_stream::OwnedBytes { &self.body }

    /// Moves the same body after destroying metadata before its original claim.
    pub fn into_body(self) -> acyclic_stream::OwnedBytes {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let Self { header, body, header_custody } = self;
            drop(header);
            drop(header_custody);
            body
        }
        #[cfg(target_arch = "wasm32")]
        {
            let Self { header, body } = self;
            drop(header);
            body
        }
    }
}

impl PartialEq for Object {
    fn eq(&self, other: &Self) -> bool { self.header == other.header && self.body == other.body }
}

/// A decoded acknowledgement inseparable from its original metadata custody.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct ObjectsPutReceipt {
    info: wire::ObjectInfo,
    _metadata_custody: Option<acyclic_native_runtime::ReadAllocation>,
}

#[cfg(not(target_arch = "wasm32"))]
impl ObjectsPutReceipt {
    /// Transfers actual metadata and the claim funded before its native producer.
    pub fn new(info: wire::ObjectInfo, metadata_custody: Option<acyclic_native_runtime::ReadAllocation>) -> Self {
        Self { info, _metadata_custody: metadata_custody }
    }
    /// Borrows the real acknowledgement without separating its metadata claim.
    pub const fn info(&self) -> &wire::ObjectInfo { &self.info }
}

/// A genuine ordered batch owning its original result-vector custody.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct ObjectsBatch<T> {
    results: Vec<Result<T, NativeObjectsError>>,
    allocation: acyclic_native_runtime::ReadAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> ObjectsBatch<T> {
    /// Transfers the already prepared result vector and its exact original claim.
    pub fn new(
        results: Vec<Result<T, NativeObjectsError>>,
        allocation: acyclic_native_runtime::ReadAllocation,
    ) -> Result<Self, NativeObjectsError> {
        let bytes = results.capacity().checked_mul(std::mem::size_of::<Result<T, NativeObjectsError>>());
        if bytes.and_then(|bytes| u64::try_from(bytes).ok()) != Some(allocation.bytes()) {
            drop(results);
            drop(allocation);
            return Err(NativeObjectsError::Admission(std::io::ErrorKind::InvalidData.into()));
        }
        Ok(Self { results, allocation })
    }

    /// Borrows the actual ordered results without separating their original claim.
    pub fn results(&self) -> &[Result<T, NativeObjectsError>] { &self.results }

    /// Iterates owned results while retaining the actual vector's allocation.
    pub fn into_results(self) -> impl ExactSizeIterator<Item = Result<T, NativeObjectsError>> {
        ObjectsBatchIterator { results: self.results.into_iter(), _allocation: self.allocation }
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct ObjectsBatchIterator<T> {
    results: std::vec::IntoIter<Result<T, NativeObjectsError>>,
    _allocation: acyclic_native_runtime::ReadAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> Iterator for ObjectsBatchIterator<T> {
    type Item = Result<T, NativeObjectsError>;
    fn next(&mut self) -> Option<Self::Item> { self.results.next() }
    fn size_hint(&self) -> (usize, Option<usize>) { self.results.size_hint() }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> ExactSizeIterator for ObjectsBatchIterator<T> {}

/// A preadmitted concrete native future whose physical box retires before custody.
#[cfg(not(target_arch = "wasm32"))]
pub struct ObjectsNativeFuture<'a, T> {
    future: std::pin::Pin<Box<dyn Future<Output = Result<T, NativeObjectsError>> + Send + 'a>>,
    _allocation: acyclic_native_runtime::ReadAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> Future for ObjectsNativeFuture<'_, T> {
    type Output = Result<T, NativeObjectsError>;
    fn poll(
        self: std::pin::Pin<&mut Self>, context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> { self.get_mut().future.as_mut().poll(context) }
}

/// Admits the actual concrete native future before its physical box allocation.
#[cfg(not(target_arch = "wasm32"))]
pub fn native_future<'a, T: 'a, F>(
    meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>, future: F,
) -> Result<ObjectsNativeFuture<'a, T>, NativeObjectsError>
where F: Future<Output = Result<T, NativeObjectsError>> + Send + 'a {
    meter.check()?;
    let allocation = acyclic_native_runtime::ReadAllocation::new(&meter, std::mem::size_of::<F>())?;
    Ok(ObjectsNativeFuture { future: Box::pin(future), _allocation: allocation })
}

/// Checks only existing input custody identity, never namespace authority.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_input_meter<T>(
    meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    input: &ObjectsNativeRequest<T>,
) -> Result<(), NativeObjectsError> {
    if !input.matches_meter(meter) {
        return Err(NativeObjectsError::Admission(std::io::ErrorKind::InvalidInput.into()));
    }
    meter.check()?;
    Ok(())
}

/// Actual caller request data and its original, unmodified ownership guard.
#[cfg(not(target_arch = "wasm32"))]
pub struct ObjectsNativeRequest<T> {
    pub(crate) data: T,
    guard: Box<dyn std::any::Any + Send + Sync>,
    allocation: acyclic_native_runtime::ReadAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> ObjectsNativeRequest<T> {
    /// Boxes the same original guard only after admission of that actual box.
    pub fn new<G>(
        data: T, guard: G,
        meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<Self, NativeObjectsError>
    where G: std::any::Any + Send + Sync {
        let request = (data, guard);
        meter.check()?;
        let allocation = acyclic_native_runtime::ReadAllocation::new(meter, std::mem::size_of::<G>())?;
        let (data, guard) = request;
        Ok(Self { data, guard: Box::new(guard), allocation })
    }

    /// Borrows request data without detaching its original ownership.
    pub const fn data(&self) -> &T { &self.data }


    pub(crate) fn matches_meter(
        &self, meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> bool {
        let owner = meter.allocation_owner().unwrap_or_else(|| std::sync::Arc::clone(meter));
        std::sync::Arc::ptr_eq(self.allocation.meter(), &owner)
    }

    pub(crate) fn map<U>(self, map: impl FnOnce(T) -> U) -> ObjectsNativeRequest<U> {
        let Self { data, guard, allocation } = self;
        ObjectsNativeRequest { data: map(data), guard, allocation }
    }
}

/// A validated header followed by bounded decoded chunks. Dropping it cancels the read.
pub struct Download {
    /// Metadata for the complete selected representation.
    pub header: wire::GetObjectHeader,
    /// Decoded body chunks; a truncated body or terminal service error is an error item.
    pub body: BodyStream,
}
pub use crate::obs::BodyStream;
use acyclic_stream::OwnedBytes;
/// Caller-owned upload chunks retaining the same funded stream box through destruction.
pub struct UploadBody {
    stream: futures::stream::BoxStream<'static, Result<OwnedBytes, Error>>,
    #[cfg(not(target_arch = "wasm32"))]
    _allocation: Option<acyclic_native_runtime::ReadAllocation>,
}
impl From<futures::stream::BoxStream<'static, Result<OwnedBytes, Error>>> for UploadBody {
    fn from(stream: futures::stream::BoxStream<'static, Result<OwnedBytes, Error>>) -> Self {
        Self { stream, #[cfg(not(target_arch = "wasm32"))] _allocation: None }
    }
}

impl UploadBody {
    /// Boxes an ordinary already initialized input stream at its existing boundary.
    pub fn from_stream<S>(stream: S) -> Self
    where S: futures::Stream<Item = Result<OwnedBytes, Error>> + Send + 'static {
        futures::StreamExt::boxed(stream).into()
    }

}
impl futures::Stream for UploadBody {
    type Item = Result<OwnedBytes, Error>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().stream.as_mut().poll_next(cx)
    }
}

/// Exclusive native download storage prepared before the provider starts I/O.
#[cfg(not(target_arch = "wasm32"))]
pub struct PreparedNativeDownload {
    bytes: Option<acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>>,
    maximum_bytes: usize,
    meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    header_allocation: Option<acyclic_native_runtime::ReadAllocation>,
    errors: std::sync::Arc<NativeTransportErrors>,
}

#[cfg(not(target_arch = "wasm32"))]
impl PreparedNativeDownload {
    /// Shares the already funded original-error bridge without another control.
    pub fn errors(&self) -> std::sync::Arc<NativeTransportErrors> { std::sync::Arc::clone(&self.errors) }

    /// Transfers actual header custody funded before the transport's I/O.
    pub fn retain_header_allocation(&mut self, allocation: acyclic_native_runtime::ReadAllocation) -> Result<(), NativeObjectsError> {
        match &mut self.header_allocation {
            Some(current) => current.absorb(&mut { allocation })?,
            None => self.header_allocation = Some(allocation),
        }
        Ok(())
    }

    /// Moves a recorded original transport failure, if one exists.
    pub fn take_error(&self) -> Option<NativeObjectsError> { self.errors.take() }

    /// Returns a failed real read with its existing data and original custody.
    pub fn fail(
        self, error: NativeObjectsError, header: Option<wire::GetObjectHeader>, filled: usize,
        transport_chunk: Option<bytes::Bytes>,
    ) -> NativeObjectsError {
        let body = self.bytes.map_or_else(acyclic_stream::OwnedBytes::new, |bytes| {
            bytes.freeze(filled).expect("completed prefix remains within its prepared storage")
        });
        let (object, partial) = match header {
            Some(header) => (Some(Object::new_native(header, body, self.header_allocation)), None),
            None => (None, Some(body)),
        };
        match error {
            NativeObjectsError::ReadFailure(mut failure) => {
                if object.is_some() {
                    failure.secondary_object = failure.object.take();
                    failure.object = object;
                }
                if partial.is_some() { failure.partial = partial; }
                if transport_chunk.is_some() { failure.transport_chunk = transport_chunk; }
                NativeObjectsError::ReadFailure(failure)
            }
            error => NativeObjectsError::ReadFailure(NativeObjectsFailure {
                cause: error.into_cause(), object, secondary_object: None, partial, receipt: None,
                wire: None, transport_chunk,
            }),
        }
    }
}
impl Download {
    /// Collects chunks within an explicit allocation bound, preserving terminal failures.
    pub async fn collect(self, maximum_bytes: u64) -> Result<Object, Error> {
        use futures::StreamExt;
        let mut body = bytes::BytesMut::new();
        let mut stream = self.body;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            let size = (body.len() as u64)
                .checked_add(chunk.len() as u64)
                .ok_or(Error::from(wire::ErrorCode::QuotaExceeded))?;
            if size > maximum_bytes || usize::try_from(size).is_err() {
                return Err(wire::ErrorCode::QuotaExceeded.into());
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Object::new(self.header, body.freeze().into()))
    }

    /// Creates the actual fixed body storage and every byte control before I/O.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn prepare_native(
        maximum_bytes: u64,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<PreparedNativeDownload, NativeObjectsError> {
        meter.check()?;
        let maximum_bytes = usize::try_from(maximum_bytes)
            .map_err(|_| Error::from(wire::ErrorCode::QuotaExceeded))?;
        let bytes = if maximum_bytes == 0 { None } else {
            Some(crate::body::prepare_native_bytes(maximum_bytes, &meter)?)
        };
        let allocation = acyclic_native_runtime::ReadAllocation::new(&meter, std::mem::size_of::<NativeTransportErrors>())?;
        let errors = std::sync::Arc::new(NativeTransportErrors {
            error: std::sync::Mutex::new(None),
            #[cfg(feature = "grpc")]
            codec_status: std::sync::Mutex::new(None),
            wire_allocation: std::sync::Mutex::new(None),
            _allocation: allocation,
        });
        Ok(PreparedNativeDownload { bytes, maximum_bytes, meter, header_allocation: None, errors })
    }

    /// Collects into storage prepared before this download's physical request.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn collect_native(
        self, mut prepared: PreparedNativeDownload,
    ) -> Result<Object, NativeObjectsError> {
        use futures::StreamExt;
        let mut stream = self.body;
        let mut filled = 0_usize;
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    let error = prepared.take_error().unwrap_or_else(|| error.into());
                    return Err(prepared.fail(error, Some(self.header), filled, None));
                }
            };
            let Some(end) = filled.checked_add(chunk.len()).filter(|end| *end <= prepared.maximum_bytes) else {
                return Err(prepared.fail(Error::from(wire::ErrorCode::QuotaExceeded).into(), Some(self.header), filled, Some(chunk)));
            };
            if let Err(error) = prepared.meter.check() {
                return Err(prepared.fail(error.into(), Some(self.header), filled, Some(chunk)));
            }
            if !chunk.is_empty() {
                if let Err(error) = prepared.meter.admit_codec(0, chunk.len() as u64, 0) {
                    return Err(prepared.fail(error.into(), Some(self.header), filled, Some(chunk)));
                }
                prepared.bytes.as_mut().expect("nonempty bounded chunk has prepared storage")
                    .writable()[filled..end].copy_from_slice(&chunk);
                filled = end;
                if let Err(error) = prepared.meter.record_codec(0, chunk.len() as u64, 0) {
                    return Err(prepared.fail(error.into(), Some(self.header), filled, Some(chunk)));
                }
            }
            filled = end;
        }
        let body = prepared.bytes.map_or_else(acyclic_stream::OwnedBytes::new, |bytes| {
            bytes.freeze(filled).expect("bounded collection fits its original prepared storage")
        });
        Ok(Object::new_native(self.header, body, prepared.header_allocation))
    }
}

/// Transport-independent logical Objects interface.
#[async_trait::async_trait]
pub trait ObjectsProvider: Send + Sync {
    /// Creates a tenant-scoped logical bucket.
    async fn create_bucket(
        &self,
        request: wire::CreateBucketRequest,
    ) -> Result<wire::Bucket, Error>;
    /// Inspects an existing logical bucket.
    async fn head_bucket(&self, request: wire::HeadBucketRequest) -> Result<wire::Bucket, Error>;
    /// Deletes an empty logical bucket.
    async fn delete_bucket(
        &self,
        request: wire::DeleteBucketRequest,
    ) -> Result<wire::DeleteBucketResponse, Error>;
    /// Publishes one complete value and atomically evaluates its precondition.
    async fn put(
        &self,
        header: wire::PutObjectHeader,
        body: acyclic_stream::OwnedBytes,
    ) -> Result<wire::ObjectInfo, Error>;

    /// Publishes through the same engine with the current original native meter.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_native(
        &self,
        _request: ObjectsNativeRequest<(wire::PutObjectHeader, acyclic_stream::OwnedBytes)>,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsPutReceipt>, NativeObjectsError> {
        drop(_request);
        Err(NativeObjectsError::Unsupported)
    }
    /// Reads one complete selected representation within the caller's allocation bound.
    async fn get(
        &self,
        request: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Object, Error>;

    /// Reads through the same provider using its original-meter body producer.
    #[cfg(not(target_arch = "wasm32"))]
    fn get_native(
        &self,
        _request: ObjectsNativeRequest<wire::GetObjectRequest>,
        _maximum_bytes: u64,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, Object>, NativeObjectsError> {
        drop(_request);
        Err(NativeObjectsError::Unsupported)
    }
    /// Reads metadata without allocating body bytes.
    async fn head(
        &self,
        request: wire::HeadObjectRequest,
    ) -> Result<wire::HeadObjectResponse, Error>;
    /// Deletes the current logical value with an optional atomic precondition.
    async fn delete(
        &self,
        request: wire::DeleteObjectRequest,
    ) -> Result<wire::DeleteObjectResponse, Error>;
    /// Traverses a live eventual listing in lexical key order.
    async fn list(
        &self,
        request: wire::ListObjectsRequest,
    ) -> Result<wire::ListObjectsResponse, Error>;
    /// Starts independently staged multipart work.
    async fn create_multipart(
        &self,
        request: wire::CreateMultipartRequest,
    ) -> Result<wire::MultipartUpload, Error>;
    /// Replaces one staged part and returns its exact receipt.
    async fn upload_part(
        &self,
        header: wire::UploadPartHeader,
        body: acyclic_stream::OwnedBytes,
    ) -> Result<wire::UploadedPart, Error>;
    /// Paginates staged parts by part number.
    async fn list_parts(
        &self,
        request: wire::ListPartsRequest,
    ) -> Result<wire::ListPartsResponse, Error>;
    /// Atomically publishes the selected parts and evaluates current-value conditions.
    async fn complete_multipart(
        &self,
        request: wire::CompleteMultipartRequest,
    ) -> Result<wire::ObjectInfo, Error>;
    /// Aborts staged multipart work.
    async fn abort_multipart(
        &self,
        request: wire::AbortMultipartRequest,
    ) -> Result<wire::AbortMultipartResponse, Error>;
}
