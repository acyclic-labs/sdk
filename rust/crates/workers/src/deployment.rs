//! Named Worker script deployments; module bytes are retained in Objects.
pub use acyclic_actors::client::CredentialProvider;
use acyclic_actors::{http, wire};
use acyclic_objects::Blobs;
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
}

impl Client<acyclic_objects::http::HttpObjects> {
    /// Share one renewal callback across Objects publication and Actors lifecycle calls.
    pub fn with_credential_provider(
        actors_endpoint: &str,
        objects_endpoint: &str,
        credential: CredentialProvider,
        maximum_response_bytes: usize,
    ) -> Result<Self, Error> {
        let actors = http::Client::with_credential_provider(
            actors_endpoint,
            credential.clone(),
            maximum_response_bytes,
        )?;
        let objects = acyclic_objects::http::HttpObjects::with_credential_provider(
            objects_endpoint,
            credential,
            maximum_response_bytes,
            None,
        )?;
        Ok(Self::new(actors, objects))
    }
}

#[cfg(test)]
mod credential_tests;

impl<P: Blobs> Client<P> {
    /// Compose clients over the account's Objects and Actors services.
    pub fn new(actors: http::Client, objects: P) -> Self {
        Self { actors, objects }
    }

    /// Publish exact module bytes as an account blob and return the SHA-256
    /// that deployments name.
    ///
    /// Publication is idempotent by content.
    ///
    /// # Errors
    /// Returns the Objects publication failure, or `OutcomeUnknown` when the
    /// store could not confirm it; repeating the call is safe.
    pub async fn publish(&self, code: bytes::Bytes) -> Result<[u8; 32], Error> {
        let digest: [u8; 32] = Sha256::digest(&code).into();
        if self
            .objects
            .put(code.into(), Vec::new())
            .await?
            .verified()
            .is_none()
        {
            return Err(acyclic_objects::Error::from(
                acyclic_objects::wire::ErrorCode::OutcomeUnknown,
            )
            .into());
        }
        Ok(digest)
    }

    /// Deploy a named module exporting default.fetch without an actor export.
    ///
    /// # Errors
    /// Returns the canonical Actors failure.
    pub async fn deploy(
        &self,
        request: &wire::DeploymentRequest,
    ) -> Result<wire::DeploymentResponse, Error> {
        Ok(self.actors.deploy_worker(request).await?)
    }

    /// Invoke the deployment using the canonical HTTP request envelope.
    ///
    /// # Errors
    /// Returns the canonical Actors failure.
    pub async fn invoke(
        &self,
        request: &wire::InvokeWorkerRequest,
    ) -> Result<wire::InvokeActorResponse, Error> {
        Ok(self.actors.invoke_worker(request).await?)
    }

    /// Delete the deployment using the caller's idempotency key.
    ///
    /// # Errors
    /// Returns the canonical Actors failure.
    pub async fn delete(
        &self,
        request: &wire::DeleteWorkerRequest,
    ) -> Result<wire::DeleteWorkerResponse, Error> {
        Ok(self.actors.delete_worker(request).await?)
    }
}
