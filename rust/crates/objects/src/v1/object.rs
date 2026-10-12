use super::*;

/// One buffered selection for callers that explicitly bound allocation.
#[derive(Debug)]
pub struct Object {
    pub(super) header: wire::GetObjectHeader,
    pub(super) body: acyclic_stream::OwnedBytes,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) _header_custody: Option<acyclic_native_runtime::ReadAllocation>,
}

impl Object {
    /// Constructs an ordinary object from its existing canonical immutable body.
    pub fn new(header: wire::GetObjectHeader, body: acyclic_stream::OwnedBytes) -> Self {
        Self {
            header,
            body,
            #[cfg(not(target_arch = "wasm32"))]
            _header_custody: None,
        }
    }

    /// Transfers metadata custody funded by the actual native producer before I/O.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_native(
        header: wire::GetObjectHeader,
        body: acyclic_stream::OwnedBytes,
        header_custody: Option<acyclic_native_runtime::ReadAllocation>,
    ) -> Self {
        Self {
            header,
            body,
            _header_custody: header_custody,
        }
    }

    /// Borrows the exact response metadata without detaching its original claim.
    pub const fn header(&self) -> &wire::GetObjectHeader {
        &self.header
    }

    /// Borrows the canonical byte carrier; clones retain intrinsic byte custody.
    pub const fn body(&self) -> &acyclic_stream::OwnedBytes {
        &self.body
    }

    /// Moves the same body after destroying metadata before its original claim.
    pub fn into_body(self) -> acyclic_stream::OwnedBytes {
        self.body
    }
}

impl PartialEq for Object {
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header && self.body == other.body
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
pub type UploadBody = Guarded<futures::stream::BoxStream<'static, Result<OwnedBytes, Error>>>;
impl From<futures::stream::BoxStream<'static, Result<OwnedBytes, Error>>> for UploadBody {
    fn from(value: futures::stream::BoxStream<'static, Result<OwnedBytes, Error>>) -> Self {
        Self {
            value,
            _allocation: Default::default(),
        }
    }
}

/// Exclusive native download storage prepared before the provider starts I/O.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct PreparedNativeDownload {
    pub(super) bytes:
        Option<acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>>,
    pub(super) maximum_bytes: usize,
    pub(super) header_allocation: Option<acyclic_native_runtime::ReadAllocation>,
    pub(super) errors: std::sync::Arc<NativeTransportErrors>,
}

#[cfg(not(target_arch = "wasm32"))]
impl PreparedNativeDownload {
    /// Returns a failed real read with its existing data and original custody.
    fn fail(
        self,
        error: NativeObjectsError,
        header: wire::GetObjectHeader,
        filled: usize,
        transport_chunk: Option<bytes::Bytes>,
    ) -> NativeObjectsError {
        let body = self
            .bytes
            .map_or_else(acyclic_stream::OwnedBytes::new, |bytes| {
                bytes
                    .freeze(filled)
                    .expect("completed prefix remains within its prepared storage")
            });
        let mut failure = error;
        failure.secondary_object = failure.object.take();
        failure.object = Some(Object::new_native(header, body, self.header_allocation));
        if transport_chunk.is_some() {
            failure.transport_chunk = transport_chunk;
        }
        failure
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
    pub(crate) fn prepare_native(
        maximum_bytes: u64,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<PreparedNativeDownload, NativeObjectsError> {
        meter.check()?;
        let maximum_bytes = usize::try_from(maximum_bytes)
            .map_err(|_| Error::from(wire::ErrorCode::QuotaExceeded))?;
        let bytes = if maximum_bytes == 0 {
            None
        } else {
            Some(crate::body::prepare_native_bytes(maximum_bytes, &meter)?)
        };
        let errors = NativeTransportErrors::new(&meter)?;
        Ok(PreparedNativeDownload {
            bytes,
            maximum_bytes,
            header_allocation: None,
            errors,
        })
    }

    /// Collects into storage prepared before this download's physical request.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn collect_native(
        self,
        mut prepared: PreparedNativeDownload,
    ) -> Result<Object, NativeObjectsError> {
        use futures::StreamExt;
        let mut stream = self.body;
        let mut filled = 0_usize;
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    let error = prepared.errors.take().unwrap_or_else(|| error.into());
                    return Err(prepared.fail(error, self.header, filled, None));
                }
            };
            let Some(end) = filled
                .checked_add(chunk.len())
                .filter(|end| *end <= prepared.maximum_bytes)
            else {
                return Err(prepared.fail(
                    Error::from(wire::ErrorCode::QuotaExceeded).into(),
                    self.header,
                    filled,
                    Some(chunk),
                ));
            };
            if let Err(error) = prepared.errors.meter.check() {
                return Err(prepared.fail(error.into(), self.header, filled, Some(chunk)));
            }
            if !chunk.is_empty() {
                if let Err(error) = prepared.errors.meter.admit_codec(0, chunk.len() as u64, 0) {
                    return Err(prepared.fail(error.into(), self.header, filled, Some(chunk)));
                }
                prepared
                    .bytes
                    .as_mut()
                    .expect("nonempty bounded chunk has prepared storage")
                    .writable()[filled..end]
                    .copy_from_slice(&chunk);
                filled = end;
                if let Err(error) = prepared.errors.meter.record_codec(0, chunk.len() as u64, 0) {
                    return Err(prepared.fail(error.into(), self.header, filled, Some(chunk)));
                }
            }
            filled = end;
        }
        let body = prepared
            .bytes
            .map_or_else(acyclic_stream::OwnedBytes::new, |bytes| {
                bytes
                    .freeze(filled)
                    .expect("bounded collection fits its original prepared storage")
            });
        Ok(Object::new_native(
            self.header,
            body,
            prepared.header_allocation,
        ))
    }
}
