use super::*;
pub use acyclic_auth::credential::CredentialProvider;
use tonic::{Request, Status};
use transport::{Deadline as _, Endpoint as RpcEndpoint, Messages as _, RpcClient as _};

/// Typed public RPC transport and platform ownership contracts.
pub mod transport;
/// Thread-affine binding to an embedding platform's authenticated Connect transport.
#[cfg(all(target_arch = "wasm32", feature = "javascript"))]
pub mod javascript;

/// One Machines client core over an authenticated platform endpoint.
#[derive(Clone)]
pub struct Provider<E: RpcEndpoint> {
    endpoint: E,
}

impl<E: RpcEndpoint> Provider<E> {
    /// Retain the original protected transport. No RPC or credential copy occurs.
    pub fn over(endpoint: E) -> Self {
        Self { endpoint }
    }

    async fn client(&self) -> Result<E::Client, ProviderError> {
        self.endpoint.client().await
    }
    /// Qualifies the service's installed default and returns its actual immutable
    /// identity. Use that image in CreateMachine; no guessed image alias is used.
    pub async fn qualify_default_image(&self) -> Result<ImageQualification, ProviderError> {
        self.qualification(None).await
    }

    async fn qualification(
        &self,
        expected: Option<Image>,
    ) -> Result<ImageQualification, ProviderError> {
        let response = self
            .client()
            .await?
            .qualify_image(wire::QualifyImageRequest {
                protocol: Some(protocol()),
                image: expected.as_ref().map(encode_image).transpose()?,
            })
            .await
            .map_err(|error| read_error(&error))?
            .into_inner();
        decode_qualification(response, expected.as_ref())
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::{NativeEndpoint, Tls};
#[cfg(not(target_arch = "wasm32"))]
/// Authenticated native gRPC Machines provider.
pub type GrpcProvider = Provider<NativeEndpoint>;

#[cfg(all(test, not(target_arch = "wasm32")))]
use native::BearerAuth;
#[cfg(all(test, not(target_arch = "wasm32")))]
use tonic::{
    metadata::MetadataValue,
    service::Interceptor as _,
    transport::{Certificate, Endpoint as TonicEndpoint, Identity},
};

mod guest;
use guest::*;
mod codec;
mod duplex;
mod files;
mod input;
mod provider;
mod recovery;
use codec::*;
use recovery::*;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod credential_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

#[cfg(all(test, target_os = "linux"))]
use native::owner_private_socket;
