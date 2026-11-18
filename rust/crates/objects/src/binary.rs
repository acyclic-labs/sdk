//! Canonical blob client over the caller's original binary account transport.
use crate::error::unavailable;
use crate::{
    Availability, Blobs, ByteRange, Collection, Deletion, Digest, Error, Head, Manifest, Object,
    ObjectsMethod, Page, Put, Released, Retained, RootId, Sealed, Usage, convert, wire,
};
use acyclic_stream::{OwnedBytes, ProviderPlatform, ProviderTask, Revision};
use bytes::Bytes;
use prost::Message;
use std::future::Future;

#[cfg(all(feature = "javascript", target_arch = "wasm32"))]
pub mod javascript;

/// One admitted response and the original account revision serving it.
pub type Reply = (Bytes, Option<Revision>);

/// An already authenticated account's original transport and observation owner.
///
/// Each invocation carries the canonical Protobuf message for its route. A put
/// is acknowledged only after durability, as in [`crate::HTTP_ROUTES`]. The
/// producer retains its original credential, cancellation and response admission;
/// losing it must settle pending work with its typed error. Dropping an invocation
/// drops that request's original future. No endpoint or credentials are reopened.
pub trait Transport: ProviderPlatform {
    /// Sends one canonical request and returns its original admitted reply.
    fn invoke(
        &self,
        route: &str,
        input: Bytes,
    ) -> impl Future<Output = Result<Reply, Error>> + ProviderTask;

    /// Registers the account's original exact-key physical observation.
    /// A transport without that producer returns `Unsupported`.
    fn availability(
        &self,
        digest: Digest,
    ) -> impl Future<Output = Result<Availability, Error>> + ProviderTask;
}

/// Blob semantics over an existing account port, on native threads or a browser
/// event loop. Request lowering and result validation use the same wire contract
/// and validators as the native clients; this adapter owns no connection loop.
#[derive(Clone)]
pub struct BinaryObjects<T> {
    transport: T,
}

impl<T> BinaryObjects<T> {
    /// Retains the original admitted account transport without I/O.
    #[must_use]
    pub const fn new(transport: T) -> Self {
        Self { transport }
    }
}

impl<T: Transport> BinaryObjects<T> {
    async fn call<I: Message, O: Message + Default>(
        &self,
        method: ObjectsMethod,
        input: I,
    ) -> Result<(O, Option<Revision>), Error> {
        let (body, revision) = self
            .transport
            .invoke(method.http_route(), input.encode_to_vec().into())
            .await?;
        // Owned decoding retains body fields in the original response allocation.
        let reply = O::decode(body).map_err(|_| unavailable())?;
        Ok((reply, revision))
    }

    async fn publish(
        &self,
        method: ObjectsMethod,
        input: impl Message,
        digest: Digest,
        length: u64,
    ) -> Result<Put, Error> {
        let (reply, revision) = self.call(method, input).await?;
        convert::verified_put(&reply, digest, length, revision)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<T: Transport> Blobs for BinaryObjects<T> {
    async fn availability(&self, digest: Digest) -> Result<Availability, Error> {
        self.transport.availability(digest).await
    }

    async fn put(&self, body: OwnedBytes, references: Vec<Digest>) -> Result<Put, Error> {
        self.put_sealed(Sealed::new(body), references).await
    }

    async fn put_sealed(&self, body: Sealed, references: Vec<Digest>) -> Result<Put, Error> {
        let (request, (digest, length)) = convert::put_blob(body, &references);
        self.publish(ObjectsMethod::PutBlob, request, digest, length)
            .await
    }

    async fn put_batch_sealed(
        &self,
        requests: Vec<(Sealed, Vec<Digest>)>,
    ) -> Vec<Result<Put, Error>> {
        let members = requests.len();
        if members == 0 {
            return Vec::new();
        }
        let (request, expected) = convert::put_blobs(requests);
        let replied = async {
            let (reply, revision) = self.call(ObjectsMethod::PutBlobs, request).await?;
            convert::verified_puts(reply, &expected, revision.as_ref())
        };
        replied
            .await
            .unwrap_or_else(|error| vec![Err(error); members])
    }

    async fn put_manifest(&self, manifest: Manifest) -> Result<Put, Error> {
        let (request, (digest, length)) = convert::put_manifest(&manifest)?;
        self.publish(ObjectsMethod::PutManifest, request, digest, length)
            .await
    }

    async fn get(
        &self,
        digest: Digest,
        range: ByteRange,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
        let (reply, revision) = self
            .call(
                ObjectsMethod::GetBlob,
                convert::get_request(digest, range, maximum_bytes),
            )
            .await?;
        convert::object(reply, digest, range, maximum_bytes, revision)
    }

    async fn head(&self, digest: Digest) -> Result<Head, Error> {
        let (reply, revision) = self
            .call(
                ObjectsMethod::HeadBlob,
                wire::HeadBlobRequest {
                    digest: Some(digest.into()),
                },
            )
            .await?;
        convert::headed(&reply, digest, revision)
    }

    async fn delete(&self, digest: Digest) -> Result<Deletion, Error> {
        let (reply, revision) = self
            .call(
                ObjectsMethod::DeleteBlob,
                wire::DeleteBlobRequest {
                    digest: Some(digest.into()),
                },
            )
            .await?;
        convert::delete(&reply, revision)
    }

    async fn list(&self, after: Option<Digest>, limit: u32) -> Result<Page, Error> {
        let (reply, revision) = self
            .call(
                ObjectsMethod::ListBlobs,
                convert::list_request(after, limit),
            )
            .await?;
        convert::listed(&reply, after, limit, revision)
    }

    async fn flush(&self, _only: Option<&[Digest]>) -> Result<(), Error> {
        // The original binary producer acknowledges each put durably.
        Ok(())
    }

    async fn retain(&self, root: &RootId, references: Vec<Digest>) -> Result<Retained, Error> {
        let (reply, revision): (wire::RetainResponse, _) = self
            .call(
                ObjectsMethod::Retain,
                convert::retain_request(root, &references),
            )
            .await?;
        Ok(convert::retained(&reply, revision))
    }

    async fn release(&self, root: &RootId) -> Result<Released, Error> {
        let (reply, revision): (wire::ReleaseResponse, _) = self
            .call(ObjectsMethod::Release, convert::release_request(root))
            .await?;
        Ok(convert::released(&reply, revision))
    }

    async fn retain_fenced(
        &self,
        root: &RootId,
        fence: u64,
        references: Vec<Digest>,
    ) -> Result<Retained, Error> {
        let (reply, revision): (wire::RetainResponse, _) = self
            .call(
                ObjectsMethod::RetainFenced,
                convert::retain_fenced_request(root, fence, &references),
            )
            .await?;
        Ok(convert::retained(&reply, revision))
    }

    async fn release_terminal(&self, root: &RootId, fence: u64) -> Result<Released, Error> {
        let (reply, revision): (wire::ReleaseResponse, _) = self
            .call(
                ObjectsMethod::ReleaseTerminal,
                convert::release_terminal_request(root, fence),
            )
            .await?;
        Ok(convert::released(&reply, revision))
    }

    async fn collect(&self) -> Result<Collection, Error> {
        let (reply, revision) = self
            .call(ObjectsMethod::Collect, wire::CollectRequest {})
            .await?;
        Ok(convert::collection(&reply, revision))
    }

    async fn usage(&self) -> Result<Usage, Error> {
        let (reply, _) = self
            .call(ObjectsMethod::Usage, wire::UsageRequest {})
            .await?;
        Ok(convert::usage(&reply))
    }
}
