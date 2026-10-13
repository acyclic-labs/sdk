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
}
