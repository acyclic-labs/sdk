//! Rust-owned native gRPC transport for the Inference customer contract.
//!
//! This module is intentionally a thin generated-wire facade. It owns TLS,
//! authentication, message bounds, and the exact fourteen service methods;
//! semantic validation remains in the Rust contract module and high-level
//! builders.

use std::time::Duration;

use prost::Message;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::Request;

use crate::{wire, MAXIMUM_MESSAGE_BYTES};

/// Native gRPC setup or canonical service failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid endpoint, API key, or trust configuration.
    #[error("invalid Inference gRPC configuration")]
    Invalid,
    /// Channel setup failed before an RPC was sent.
    #[error("Inference gRPC transport setup failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// The service rejected or could not complete the operation.
    #[error("Inference gRPC service failure: {0}")]
    Status(#[from] tonic::Status),
    /// The authenticated endpoint did not prove the Rust-owned contract.
    #[error("Inference control handshake failed: {0}")]
    Negotiation(String),
}

/// Authenticated native gRPC operations for the Inference customer contract.
#[derive(Clone)]
pub struct Client {
    channel: Channel,
    authorization: String,
}

impl Client {
    /// Connect only after the authenticated, non-mutating control handshake.
    ///
    /// `Ok(None)` means the endpoint does not expose the control service and
    /// permits the caller to try the verified HTTP transport. Any response
    /// that claims the service exists but fails identity validation is
    /// terminal.
    pub async fn connect_verified(
        endpoint: &str,
        token: &str,
        ca_pem: &[u8],
    ) -> Result<Option<Self>, Error> {
        use acyclic_sdk_contract_wire::{BindingFamily, transport_control as control};
        use crate::control_wire::protocol::v1::{
            Capability, CapabilitySet, HandshakeRequest, ProtocolIdentity,
        };

        let (channel, authorization) = Self::authenticated_channel(endpoint, token, ca_pem).await?;
        let family = BindingFamily::Inference;
        let version = control::control_protocol_version(family);
        let mut probe = crate::control_wire::transport::v1::protocol_service_client::ProtocolServiceClient::new(channel.clone())
            .max_decoding_message_size(control::MAXIMUM_HANDSHAKE_RESPONSE_BYTES);
        let mut request = Request::new(HandshakeRequest {
            protocol: Some(ProtocolIdentity {
                version: version.into(),
                descriptor_digest: control::archived_descriptor_digest(family),
            }),
            required: Some(CapabilitySet {
                capabilities: vec![Capability {
                    name: family.name().into(),
                    version: version.into(),
                }],
            }),
        });
        request.metadata_mut().insert(
            control::FAMILY_METADATA_KEY,
            MetadataValue::from_static(family.name()),
        );
        let mut bearer = MetadataValue::<Ascii>::try_from(authorization.as_str())
            .map_err(|_| Error::Invalid)?;
        bearer.set_sensitive(true);
        request
            .metadata_mut()
            .insert("authorization", bearer);
        request.set_timeout(Duration::from_secs(10));
        let response = match probe.handshake(request).await {
            Ok(response) => response.into_inner(),
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::Unimplemented
                        | tonic::Code::Unavailable
                        | tonic::Code::DeadlineExceeded
                ) =>
            {
                return Ok(None);
            }
            Err(status) => return Err(Error::Status(status)),
        };
        control::validate_handshake_response(
            family,
            version,
            &[control::RequiredCapability {
                name: family.name(),
                version,
            }],
            &response.encode_to_vec(),
            control::MAXIMUM_HANDSHAKE_RESPONSE_BYTES,
        )
        .map_err(|error| Error::Negotiation(format!("{error:?}")))?;
        Ok(Some(Self {
            channel,
            authorization,
        }))
    }

    async fn authenticated_channel(
        endpoint: &str,
        token: &str,
        ca_pem: &[u8],
    ) -> Result<(Channel, String), Error> {
        if ca_pem.len() > 64 * 1024 || token.is_empty() || token.len() > 8_192 {
            return Err(Error::Invalid);
        }
        let authorization = format!("Bearer {token}");
        MetadataValue::<Ascii>::try_from(authorization.as_str()).map_err(|_| Error::Invalid)?;
        let endpoint = Endpoint::new(endpoint.to_owned())?;
        if endpoint.uri().scheme_str() != Some("https") {
            return Err(Error::Invalid);
        }
        let mut tls = ClientTlsConfig::new().with_enabled_roots();
        if !ca_pem.is_empty() {
            tls = tls.ca_certificate(Certificate::from_pem(ca_pem));
        }
        let channel = endpoint
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(10))
            .tls_config(tls)?
            .connect()
            .await?;
        Ok((channel, authorization))
    }

    fn request<T>(&self, value: T) -> Result<Request<T>, Error> {
        let mut request = Request::new(value);
        let mut authorization = MetadataValue::<Ascii>::try_from(self.authorization.as_str())
            .map_err(|_| Error::Invalid)?;
        authorization.set_sensitive(true);
        request.metadata_mut().insert("authorization", authorization);
        request.set_timeout(Duration::from_secs(60));
        Ok(request)
    }

    /// Models/List.
    pub async fn list(&self, request: &wire::ListModelsRequest) -> Result<wire::ListModelsResponse, Error> {
        let mut client = wire::models_service_client::ModelsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES);
        Ok(client.list(self.request(request.clone())?).await?.into_inner())
    }

    /// Contexts/Create.
    pub async fn create_context(&self, request: &wire::CreateContextRequest) -> Result<wire::MutationReceipt, Error> {
        let mut client = wire::contexts_service_client::ContextsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES);
        Ok(client.create(self.request(request.clone())?).await?.into_inner())
    }

    /// Contexts/Inspect.
    pub async fn inspect_context(&self, request: &wire::InspectContextRequest) -> Result<wire::ContextView, Error> {
        let mut client = wire::contexts_service_client::ContextsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES);
        Ok(client.inspect(self.request(request.clone())?).await?.into_inner())
    }

    /// Contexts/Mutate.
    pub async fn mutate_context(&self, request: &wire::MutateContextRequest) -> Result<wire::MutationReceipt, Error> {
        let mut client = wire::contexts_service_client::ContextsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES);
        Ok(client.mutate(self.request(request.clone())?).await?.into_inner())
    }

    /// WarmContexts/Retain.
    pub async fn retain_warm(&self, request: &wire::RetainWarmRequest) -> Result<wire::WarmView, Error> {
        let mut client = wire::warm_contexts_service_client::WarmContextsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES);
        Ok(client.retain(self.request(request.clone())?).await?.into_inner())
    }

    /// WarmContexts/Inspect.
    pub async fn inspect_warm(&self, request: &wire::InspectWarmRequest) -> Result<wire::WarmView, Error> {
        let mut client = wire::warm_contexts_service_client::WarmContextsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES);
        Ok(client.inspect(self.request(request.clone())?).await?.into_inner())
    }

    /// WarmContexts/Renew.
    pub async fn renew_warm(&self, request: &wire::RenewWarmRequest) -> Result<wire::WarmView, Error> {
        let mut client = wire::warm_contexts_service_client::WarmContextsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES);
        Ok(client.renew(self.request(request.clone())?).await?.into_inner())
    }

    /// WarmContexts/Release.
    pub async fn release_warm(&self, request: &wire::ReleaseWarmRequest) -> Result<wire::WarmView, Error> {
        let mut client = wire::warm_contexts_service_client::WarmContextsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES);
        Ok(client.release(self.request(request.clone())?).await?.into_inner())
    }

    /// Runs/Generate.
    pub async fn generate_run(&self, request: &wire::GenerateRunRequest) -> Result<wire::GenerateRunResponse, Error> {
        let mut client = wire::runs_service_client::RunsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES);
        Ok(client.generate(self.request(request.clone())?).await?.into_inner())
    }

    /// Runs/Inspect.
    pub async fn inspect_run(&self, request: &wire::InspectRunRequest) -> Result<wire::RunView, Error> {
        let mut client = wire::runs_service_client::RunsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES);
        Ok(client.inspect(self.request(request.clone())?).await?.into_inner())
    }

    /// Runs/Watch, collected in protocol order while preserving every event.
    pub async fn watch_run(&self, request: &wire::WatchRunRequest) -> Result<Vec<wire::RunEvent>, Error> {
        let mut client = wire::runs_service_client::RunsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES);
        let mut stream = client.watch(self.request(request.clone())?).await?.into_inner();
        let mut events = Vec::new();
        while let Some(event) = stream.message().await? {
            events.push(event);
        }
        Ok(events)
    }

    /// Runs/Cancel.
    pub async fn cancel_run(&self, request: &wire::InspectRunRequest) -> Result<wire::RunView, Error> {
        let mut client = wire::runs_service_client::RunsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES);
        Ok(client.cancel(self.request(request.clone())?).await?.into_inner())
    }

    /// Evaluations/Create.
    pub async fn create_evaluation(&self, request: &wire::CreateEvaluationRequest) -> Result<wire::EvaluationView, Error> {
        let mut client = wire::evaluations_service_client::EvaluationsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES);
        Ok(client.create(self.request(request.clone())?).await?.into_inner())
    }

    /// Evaluations/Inspect.
    pub async fn inspect_evaluation(&self, request: &wire::InspectEvaluationRequest) -> Result<wire::EvaluationView, Error> {
        let mut client = wire::evaluations_service_client::EvaluationsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES);
        Ok(client.inspect(self.request(request.clone())?).await?.into_inner())
    }
}
