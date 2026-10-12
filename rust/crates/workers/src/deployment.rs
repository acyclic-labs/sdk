//! Cloud Workers deploy as stateless Actors, with module bytes retained in Objects.
use acyclic_actors::{http, wire};
use acyclic_objects::{ObjectsProvider, wire as objects};
use sha2::{Digest, Sha256};

/// Publication or lifecycle failure from the existing canonical service clients.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Objects publication failed.
    #[error(transparent)]
    Objects(#[from] acyclic_objects::Error),
    /// Actors lifecycle operation failed.
    #[error(transparent)]
    Actors(#[from] http::Error),
}

/// Worker lifecycle over the existing Objects provider and Actors HTTP client.
pub struct Client<P> {
    actors: http::Client,
    objects: P,
    code_bucket: String,
}

impl<P: ObjectsProvider> Client<P> {
    /// Compose clients with an existing account-owned code bucket.
    pub fn new(actors: http::Client, objects: P, code_bucket: String) -> Self {
        Self {
            actors,
            objects,
            code_bucket,
        }
    }

    /// Publish exact module bytes at Cloud's canonical content address.
    ///
    /// # Errors
    /// Returns the Objects publication failure.
    pub async fn publish(
        &self,
        code: bytes::Bytes,
        idempotency_key: String,
    ) -> Result<[u8; 32], Error> {
        let digest: [u8; 32] = Sha256::digest(&code).into();
        self.objects
            .put(
                objects::PutObjectHeader {
                    bucket: Some(objects::BucketRef {
                        name: self.code_bucket.clone(),
                    }),
                    object_key: format!("sha256/{}", hex::encode(digest)),
                    mutation: Some(objects::MutationIdentity { idempotency_key }),
                    ..Default::default()
                },
                code,
            )
            .await?;
        Ok(digest)
    }

    /// Create a module exporting default.fetch without an actor export.
    ///
    /// # Errors
    /// Returns the canonical Actors failure.
    pub async fn create(
        &self,
        request: &wire::CreateActorRequest,
    ) -> Result<wire::CreateActorResponse, Error> {
        Ok(self.actors.create_actor(request).await?)
    }

    /// Invoke the deployment using the canonical HTTP request envelope.
    ///
    /// # Errors
    /// Returns the canonical Actors failure.
    pub async fn invoke(
        &self,
        request: &wire::InvokeActorRequest,
    ) -> Result<wire::InvokeActorResponse, Error> {
        Ok(self.actors.invoke_actor(request).await?)
    }

    /// Delete the deployment using the caller's idempotency key.
    ///
    /// # Errors
    /// Returns the canonical Actors failure.
    pub async fn delete(
        &self,
        request: &wire::DeleteActorRequest,
    ) -> Result<wire::DeleteActorResponse, Error> {
        Ok(self.actors.delete_actor(request).await?)
    }
}
