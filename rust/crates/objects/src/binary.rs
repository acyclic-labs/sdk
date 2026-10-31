//! Canonical blob client over the caller's original binary account transport.
use crate::{
    Availability, Blobs, ByteRange, Collection, Deletion, Digest, Error, Head, Manifest, Object,
    Page, Put, Released, Retained, RootId, Sealed, Usage, convert, wire,
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

fn unavailable() -> Error {
    wire::ErrorCode::Unavailable.into()
}

impl<T: Transport> BinaryObjects<T> {
    async fn call<I: Message, O: Message + Default>(
        &self,
        route: &str,
        input: I,
    ) -> Result<(O, Option<Revision>), Error> {
        let (body, revision) = self
            .transport
            .invoke(route, input.encode_to_vec().into())
            .await?;
        // Owned decoding retains body fields in the original response allocation.
        let reply = O::decode(body).map_err(|_| unavailable())?;
        Ok((reply, revision))
    }

    async fn publish(
        &self,
        route: &str,
        input: impl Message,
        digest: Digest,
        length: u64,
    ) -> Result<Put, Error> {
        let (reply, revision) = self.call(route, input).await?;
        Ok(convert::verified_put(&reply, digest, length)
            .map_err(|_| unavailable())?
            .committed_at(revision))
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
        let digest = Digest::sealed(&body);
        let length = body.bytes().len() as u64;
        self.publish(
            "blobs/put",
            wire::PutBlobRequest {
                digest: Some(digest.into()),
                body: Bytes::from_owner(body.into_bytes()),
                references: convert::wire_digests(&references),
            },
            digest,
            length,
        )
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
            let (reply, revision) = self.call("blobs/put-batch", request).await?;
            convert::verified_puts(&reply, &expected, revision.as_ref()).map_err(|_| unavailable())
        };
        replied
            .await
            .unwrap_or_else(|error| vec![Err(error); members])
    }

    async fn put_manifest(&self, manifest: Manifest) -> Result<Put, Error> {
        let digest = Digest::of(&manifest.encode());
        let length = manifest.length()?;
        self.publish(
            "blobs/put-manifest",
            wire::PutManifestRequest {
                manifest: Some(convert::manifest(&manifest)),
            },
            digest,
            length,
        )
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
                "blobs/get",
                wire::GetBlobRequest {
                    digest: Some(digest.into()),
                    range: Some(convert::range(range)),
                    maximum_bytes,
                },
            )
            .await?;
        Ok(convert::object(reply, digest, range, maximum_bytes)
            .map_err(|_| unavailable())?
            .served_at(revision))
    }

    async fn head(&self, digest: Digest) -> Result<Head, Error> {
        let (reply, revision) = self
            .call(
                "blobs/head",
                wire::HeadBlobRequest {
                    digest: Some(digest.into()),
                },
            )
            .await?;
        let mut head = convert::headed(&reply, digest).map_err(|_| unavailable())?;
        head.revision = revision;
        Ok(head)
    }

    async fn delete(&self, digest: Digest) -> Result<Deletion, Error> {
        let (reply, revision) = self
            .call(
                "blobs/delete",
                wire::DeleteBlobRequest {
                    digest: Some(digest.into()),
                },
            )
            .await?;
        Ok(Deletion {
            outcome: convert::delete(&reply).map_err(|_| unavailable())?,
            revision,
        })
    }

    async fn list(&self, after: Option<Digest>, limit: u32) -> Result<Page, Error> {
        let (reply, revision) = self
            .call(
                "blobs/list",
                wire::ListBlobsRequest {
                    after: after.map(Into::into),
                    limit,
                },
            )
            .await?;
        let mut page = convert::listed(&reply, after, limit).map_err(|_| unavailable())?;
        page.revision = revision;
        Ok(page)
    }

    async fn flush(&self, _only: Option<&[Digest]>) -> Result<(), Error> {
        // The original binary producer acknowledges each put durably.
        Ok(())
    }

    async fn retain(&self, root: &RootId, references: Vec<Digest>) -> Result<Retained, Error> {
        let (reply, revision): (wire::RetainResponse, _) = self
            .call(
                "blobs/retain",
                wire::RetainRequest {
                    root: root.as_bytes().to_vec(),
                    references: convert::wire_digests(&references),
                },
            )
            .await?;
        let mut retained = convert::retained(&reply);
        retained.revision = revision;
        Ok(retained)
    }

    async fn release(&self, root: &RootId) -> Result<Released, Error> {
        let (reply, revision): (wire::ReleaseResponse, _) = self
            .call(
                "blobs/release",
                wire::ReleaseRequest {
                    root: root.as_bytes().to_vec(),
                },
            )
            .await?;
        Ok(Released {
            existed: reply.existed,
            revision,
        })
    }

    async fn collect(&self) -> Result<Collection, Error> {
        let (reply, revision) = self.call("blobs/collect", wire::CollectRequest {}).await?;
        let mut collection = convert::collection(&reply);
        collection.revision = revision;
        Ok(collection)
    }

    async fn usage(&self) -> Result<Usage, Error> {
        let (reply, _) = self.call("blobs/usage", wire::UsageRequest {}).await?;
        Ok(convert::usage(&reply))
    }
}
