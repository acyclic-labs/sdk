//! The account-scoped blob contract every provider implements.
use crate::{ByteRange, Digest, Error, Kind, Manifest, wire};
#[cfg(not(target_arch = "wasm32"))]
use crate::{NativeObjectsCause, NativeObjectsError, ObjectsBatch, ObjectsNativeFuture};
#[cfg(not(target_arch = "wasm32"))]
use crate::{ObjectsNativeRequest, ObjectsPutReceipt};
use acyclic_stream::OwnedBytes;

/// A digest and length compared with the stored envelope in the caller's own
/// account, under the grant that admitted the call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Verified {
    /// The stored digest.
    pub digest: Digest,
    /// Its logical length.
    pub length: u64,
}

/// Outcome of a put.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Put {
    /// This call stored the blob.
    Created {
        /// The new blob.
        verified: Verified,
    },
    /// The account already held identical bytes with identical references.
    AlreadyExists {
        /// The existing blob.
        verified: Verified,
    },
    /// The outcome is unknown. Repeating the identical put is always safe.
    Unknown,
}

impl Put {
    /// The durable blob, unless the outcome is unknown.
    #[must_use]
    pub const fn verified(&self) -> Option<&Verified> {
        match self {
            Self::Created { verified } | Self::AlreadyExists { verified } => Some(verified),
            Self::Unknown => None,
        }
    }
}

/// One blob's envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Head {
    /// Its digest.
    pub digest: Digest,
    /// Plain blob or manifest.
    pub kind: Kind,
    /// Logical length.
    pub length: u64,
    /// Outgoing references recorded outside the payload.
    pub references: Vec<Digest>,
}

/// Outcome of a delete. A referenced blob is never deleted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Delete {
    /// This call removed the blob.
    Deleted,
    /// The account holds no such blob.
    Absent,
    /// A retained root or another blob references it; nothing changed.
    Referenced,
}

/// One page of the account's digests in ascending order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Page {
    /// Digests strictly after the request's cursor.
    pub digests: Vec<Digest>,
    /// The cursor for the next page; `None` only at the end.
    pub next: Option<Digest>,
}

/// A retained root's identity, typically one stream's retained records.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RootId(Vec<u8>);

impl RootId {
    /// Largest root identity in bytes.
    pub const MAX_BYTES: usize = 1024;

    /// Wraps a nonempty identity of at most [`Self::MAX_BYTES`].
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, Error> {
        let bytes = bytes.into();
        if bytes.is_empty() || bytes.len() > Self::MAX_BYTES {
            return Err(wire::ErrorCode::InvalidArgument.into());
        }
        Ok(Self(bytes))
    }

    /// The identity bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// An atomically appended reference batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Retained {
    /// Distinct references in the batch.
    pub references: u64,
}

/// One completed mark/sweep pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Collection {
    /// The collection epoch this pass began.
    pub epoch: u64,
    /// Blobs reachable from retained roots or admitted after the previous cut.
    pub marked: u64,
    /// Blobs removed.
    pub swept: u64,
    /// Stored bytes removed.
    pub swept_bytes: u64,
}

/// The account's unique stored bytes; every digest counts once.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Usage {
    /// Distinct blobs.
    pub blobs: u64,
    /// Their stored bytes.
    pub unique_bytes: u64,
}

/// One selected logical range and the envelope it was read under.
#[derive(Debug)]
pub struct Object {
    head: Head,
    start: u64,
    body: OwnedBytes,
    #[cfg(not(target_arch = "wasm32"))]
    _custody: Option<acyclic_native_runtime::ReadAllocation>,
}

impl Object {
    /// Pairs selected bytes with their envelope.
    #[must_use]
    pub fn new(head: Head, start: u64, body: OwnedBytes) -> Self {
        Self {
            head,
            start,
            body,
            #[cfg(not(target_arch = "wasm32"))]
            _custody: None,
        }
    }

    /// Retains the metadata claim a native producer funded.
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn new_native(
        head: Head,
        start: u64,
        body: OwnedBytes,
        custody: Option<acyclic_native_runtime::ReadAllocation>,
    ) -> Self {
        Self {
            head,
            start,
            body,
            _custody: custody,
        }
    }

    /// The envelope.
    #[must_use]
    pub const fn head(&self) -> &Head {
        &self.head
    }

    /// Logical offset of the first selected byte.
    #[must_use]
    pub const fn start(&self) -> u64 {
        self.start
    }

    /// The selected bytes.
    #[must_use]
    pub const fn body(&self) -> &OwnedBytes {
        &self.body
    }

    /// Moves the selected bytes out.
    #[must_use]
    pub fn into_body(self) -> OwnedBytes {
        self.body
    }
}

impl PartialEq for Object {
    fn eq(&self, other: &Self) -> bool {
        self.head == other.head && self.start == other.start && self.body == other.body
    }
}

/// Account-scoped content-addressed blobs.
///
/// The provider's account is the entire namespace; nothing here can name,
/// dedupe against or disclose another account's blobs.
#[async_trait::async_trait]
pub trait Blobs: Send + Sync {
    /// Stores `body` with envelope `references`, each of which must already exist.
    async fn put(&self, body: OwnedBytes, references: Vec<Digest>) -> Result<Put, Error>;

    /// Stores a manifest after checking every part's length and every edge's existence.
    async fn put_manifest(&self, manifest: Manifest) -> Result<Put, Error>;

    /// Reads one logical range of at most `maximum_bytes`. A manifest's range
    /// selects only the intersecting parts.
    async fn get(
        &self,
        digest: Digest,
        range: ByteRange,
        maximum_bytes: u64,
    ) -> Result<Object, Error>;

    /// Reads the envelope without payload bytes.
    async fn head(&self, digest: Digest) -> Result<Head, Error>;

    /// Deletes an unreferenced blob; a referenced one is refused, never broken.
    async fn delete(&self, digest: Digest) -> Result<Delete, Error>;

    /// Lists digests strictly after `after` in ascending order. A blob present
    /// for the whole traversal appears exactly once.
    async fn list(&self, after: Option<Digest>, limit: u32) -> Result<Page, Error>;

    /// Atomically appends one reference batch to `root` after verifying every
    /// reference exists. Repeating the same batch is a no-op.
    async fn retain(&self, root: &RootId, references: Vec<Digest>) -> Result<Retained, Error>;

    /// Drops every batch of `root`. Repeating it is a no-op.
    async fn release(&self, root: &RootId) -> Result<bool, Error>;

    /// Marks from every retained root and every blob admitted since the
    /// previous cut, then sweeps the rest. An incomplete root scan sweeps nothing.
    async fn collect(&self) -> Result<Collection, Error>;

    /// Unique stored bytes in this account.
    async fn usage(&self) -> Result<Usage, Error>;

    /// Ordered, individually atomic puts.
    async fn put_batch(&self, requests: Vec<(OwnedBytes, Vec<Digest>)>) -> Vec<Result<Put, Error>> {
        let mut results = Vec::with_capacity(requests.len());
        for (body, references) in requests {
            results.push(self.put(body, references).await);
        }
        results
    }

    /// Ordered, individually bounded reads.
    async fn get_batch(
        &self,
        requests: Vec<(Digest, ByteRange, u64)>,
    ) -> Vec<Result<Object, Error>> {
        let mut results = Vec::with_capacity(requests.len());
        for (digest, range, maximum) in requests {
            results.push(self.get(digest, range, maximum).await);
        }
        results
    }

    /// Reads under the caller's original allocation owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn get_native(
        &self,
        _request: ObjectsNativeRequest<(Digest, ByteRange, u64)>,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, Object>, NativeObjectsError> {
        Err(NativeObjectsError::from(NativeObjectsCause::Unsupported))
    }

    /// Reads a batch under the caller's original allocation owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn get_batch_native(
        &self,
        _requests: ObjectsNativeRequest<Vec<(Digest, ByteRange, u64)>>,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<Object>>, NativeObjectsError> {
        Err(NativeObjectsError::from(NativeObjectsCause::Unsupported))
    }

    /// Stores under the caller's original allocation owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_native(
        &self,
        _request: ObjectsNativeRequest<(OwnedBytes, Vec<Digest>)>,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsPutReceipt>, NativeObjectsError> {
        Err(NativeObjectsError::from(NativeObjectsCause::Unsupported))
    }

    /// Stores a batch under the caller's original allocation owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_batch_native(
        &self,
        _requests: ObjectsNativeRequest<Vec<(OwnedBytes, Vec<Digest>)>>,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<ObjectsPutReceipt>>, NativeObjectsError> {
        Err(NativeObjectsError::from(NativeObjectsCause::Unsupported))
    }
}

#[async_trait::async_trait]
impl<T: Blobs + ?Sized> Blobs for std::sync::Arc<T> {
    async fn put(&self, body: OwnedBytes, references: Vec<Digest>) -> Result<Put, Error> {
        (**self).put(body, references).await
    }
    async fn put_manifest(&self, manifest: Manifest) -> Result<Put, Error> {
        (**self).put_manifest(manifest).await
    }
    async fn get(
        &self,
        digest: Digest,
        range: ByteRange,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
        (**self).get(digest, range, maximum_bytes).await
    }
    async fn head(&self, digest: Digest) -> Result<Head, Error> {
        (**self).head(digest).await
    }
    async fn delete(&self, digest: Digest) -> Result<Delete, Error> {
        (**self).delete(digest).await
    }
    async fn list(&self, after: Option<Digest>, limit: u32) -> Result<Page, Error> {
        (**self).list(after, limit).await
    }
    async fn retain(&self, root: &RootId, references: Vec<Digest>) -> Result<Retained, Error> {
        (**self).retain(root, references).await
    }
    async fn release(&self, root: &RootId) -> Result<bool, Error> {
        (**self).release(root).await
    }
    async fn collect(&self) -> Result<Collection, Error> {
        (**self).collect().await
    }
    async fn usage(&self) -> Result<Usage, Error> {
        (**self).usage().await
    }
    async fn put_batch(&self, requests: Vec<(OwnedBytes, Vec<Digest>)>) -> Vec<Result<Put, Error>> {
        (**self).put_batch(requests).await
    }
    async fn get_batch(
        &self,
        requests: Vec<(Digest, ByteRange, u64)>,
    ) -> Vec<Result<Object, Error>> {
        (**self).get_batch(requests).await
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn get_native(
        &self,
        request: ObjectsNativeRequest<(Digest, ByteRange, u64)>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, Object>, NativeObjectsError> {
        (**self).get_native(request, meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn get_batch_native(
        &self,
        requests: ObjectsNativeRequest<Vec<(Digest, ByteRange, u64)>>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<Object>>, NativeObjectsError> {
        (**self).get_batch_native(requests, meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn put_native(
        &self,
        request: ObjectsNativeRequest<(OwnedBytes, Vec<Digest>)>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsPutReceipt>, NativeObjectsError> {
        (**self).put_native(request, meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn put_batch_native(
        &self,
        requests: ObjectsNativeRequest<Vec<(OwnedBytes, Vec<Digest>)>>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<ObjectsPutReceipt>>, NativeObjectsError> {
        (**self).put_batch_native(requests, meter)
    }
}
