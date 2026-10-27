//! Single-dispatch gRPC transport shared above provider wire definitions.
use crate::ProviderError;
use futures::StreamExt;
use prost::Message;
use std::time::Duration;
use tonic::{
    Request,
    metadata::MetadataMap,
    transport::{Channel, ClientTlsConfig, Endpoint},
};

#[derive(Clone)]
pub(super) struct Rpc {
    channel: Channel,
    pub metadata: MetadataMap,
}

impl Rpc {
    pub fn new(endpoint: &str, metadata: MetadataMap) -> Result<Self, ProviderError> {
        let url = super::http::endpoint_url(endpoint)?;
        let mut endpoint = Endpoint::from_shared(url.to_string())
            .map_err(|_| {
                ProviderError::Invalid(
                    "provider endpoint admission failed; recovery: configure an absolute gRPC URL"
                        .into(),
                )
            })?
            .connect_timeout(Duration::from_secs(5));
        if url.scheme() == "https" {
            endpoint = endpoint
                .tls_config(ClientTlsConfig::new().with_webpki_roots())
                .map_err(|_| ProviderError::Unavailable)?;
        }
        Ok(Self {
            channel: endpoint.connect_lazy(),
            metadata,
        })
    }

    fn request<T>(&self, message: T, timeout: Duration) -> Request<T> {
        let mut request = Request::new(message);
        *request.metadata_mut() = self.metadata.clone();
        request.set_timeout(timeout);
        request
    }

    pub async fn unary<I, O>(
        &self,
        service: &'static str,
        method: &'static str,
        message: I,
        timeout: Duration,
    ) -> Result<O, ProviderError>
    where
        I: Message + Default + Send + Sync + 'static,
        O: Message + Default + Send + Sync + 'static,
    {
        let mut client = tonic::client::Grpc::new(self.channel.clone());
        client
            .ready()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        let path = format!("/{service}/{method}").parse().map_err(|_| ProviderError::Invalid("provider RPC path construction failed; recovery: use the shipped adapter's method names".into()))?;
        client
            .unary(
                self.request(message, timeout),
                path,
                tonic_prost::ProstCodec::default(),
            )
            .await
            .map(tonic::Response::into_inner)
            .map_err(rpc_error)
    }

    pub async fn streaming<I, O>(
        &self,
        service: &'static str,
        method: &'static str,
        message: I,
        timeout: Duration,
    ) -> Result<impl futures::Stream<Item = Result<O, ProviderError>> + Send + 'static, ProviderError>
    where
        I: Message + Default + Send + Sync + 'static,
        O: Message + Default + Send + Sync + 'static,
    {
        let mut client = tonic::client::Grpc::new(self.channel.clone());
        client
            .ready()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        let path = format!("/{service}/{method}").parse().map_err(|_| ProviderError::Invalid("provider RPC path construction failed; recovery: use the shipped adapter's method names".into()))?;
        let response: tonic::Response<tonic::Streaming<O>> = client
            .server_streaming(
                self.request(message, timeout),
                path,
                tonic_prost::ProstCodec::default(),
            )
            .await
            .map_err(rpc_error)?;
        Ok(response.into_inner().map(|item| item.map_err(rpc_error)))
    }
}

fn rpc_error(error: tonic::Status) -> ProviderError {
    match error.code() {
        tonic::Code::NotFound => ProviderError::NotFound("provider resource".into()),
        tonic::Code::AlreadyExists | tonic::Code::FailedPrecondition => ProviderError::Conflict("provider state conflict; recovery: inspect the sandbox before a new mutation".into()),
        tonic::Code::InvalidArgument | tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => ProviderError::Rejected("provider request rejected; recovery: inspect resource, network policy and credential configuration".into()),
        _ => ProviderError::Unavailable,
    }
}
