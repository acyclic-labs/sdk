//! Named Worker deployments over the shared immutable entry store and Actors client.
use crate::entry::EntryStore;
pub use acyclic_actors::client::CredentialProvider;
use acyclic_actors::{http, wire};
use acyclic_objects::Blobs;

/// Construction or lifecycle failure from the existing canonical service clients.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Objects client construction failed.
    #[error(transparent)]
    Objects(#[from] acyclic_objects::Error),
    /// Actors lifecycle operation failed.
    #[error(transparent)]
    Actors(#[from] http::Error),
}

/// Worker lifecycle over the existing Objects provider and Actors HTTP client.
pub struct Client<P> {
    actors: http::Client,
    entries: EntryStore<P>,
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
        Self {
            actors,
            entries: EntryStore::new(objects),
        }
    }

    /// Publish, restore and release selected entry closures through the original
    /// account provider. Each deployment retains its own stable intent root.
    /// This borrow performs no allocation, connection or request.
    #[must_use]
    pub const fn entries(&self) -> &EntryStore<P> {
        &self.entries
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
