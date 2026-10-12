use std::sync::Arc;
use std::time::Duration;
use tonic::Request;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use zeroize::{Zeroize, Zeroizing};

use crate::MAXIMUM_MESSAGE_BYTES;
use crate::contract;
use crate::obs;
use crate::wire;

/// Largest caller-supplied PEM trust bundle accepted by [`Inference::connect`].
pub const MAXIMUM_CA_CERTIFICATE_BYTES: usize = 64 * 1024;
const INVALID_CA_CERTIFICATE_LENGTH: &str = "CA certificate must contain 1 to 65536 bytes";

impl Drop for wire::Item {
    fn drop(&mut self) {
        self.payload.zeroize();
    }
}

impl Drop for wire::Replace {
    fn drop(&mut self) {
        self.payload.zeroize();
    }
}

impl Drop for wire::RunEvent {
    fn drop(&mut self) {
        scrub_run_event(self);
    }
}

impl Drop for wire::RunResult {
    fn drop(&mut self) {
        scrub_run_result(self);
    }
}

fn scrub_run_event(event: &mut wire::RunEvent) {
    if let Some(wire::run_event::Event::Output(output)) = event.event.as_mut() {
        output.zeroize();
    }
}

fn scrub_run_result(result: &mut wire::RunResult) {
    result.output.zeroize();
}

/// Transport or contract failure. Reuse the same builder to reconcile uncertainty.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid configuration, request identity or response shape.
    #[error("invalid customer contract: {0}")]
    Invalid(&'static str),
    /// Channel setup failed before an RPC was sent.
    #[error("customer transport setup failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// A failed observation never proves an admitted mutation was absent.
    #[error("customer operation observation failed: {0}")]
    Observation(#[from] tonic::Status),
}

impl Error {
    /// Stable, message-free name of this failure class.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Invalid(_) => "invalid",
            Self::Transport(_) => "transport",
            Self::Observation(_) => "observation",
        }
    }
}

struct Connection {
    channel: Channel,
    authorization: Zeroizing<String>,
    client_instance: [u8; 16],
}

/// Authenticated customer connection; infrastructure remains entirely service-owned.
#[derive(Clone)]
pub struct Inference(Arc<Connection>);

impl Inference {
    /// Connect using an explicit trusted CA, ambient `WebPKI` roots, and bounded TLS/RPC deadlines.
    #[tracing::instrument(
        name = "acyclic.inference.connect",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    pub async fn connect(endpoint: &str, api_key: &str, ca_pem: &[u8]) -> Result<Self, Error> {
        obs::finish(
            async {
                let endpoint = Endpoint::new(endpoint.to_owned())?;
                if endpoint.uri().scheme_str() != Some("https") {
                    return Err(Error::Invalid("HTTPS is required"));
                }
                if ca_pem.is_empty() || ca_pem.len() > MAXIMUM_CA_CERTIFICATE_BYTES {
                    return Err(Error::Invalid(INVALID_CA_CERTIFICATE_LENGTH));
                }
                let authorization = authorization(api_key)?;
                let channel = endpoint
                    .connect_timeout(Duration::from_secs(10))
                    .timeout(Duration::from_secs(60))
                    .http2_keep_alive_interval(Duration::from_secs(30))
                    .keep_alive_timeout(Duration::from_secs(10))
                    .tls_config(
                        ClientTlsConfig::new()
                            .with_enabled_roots()
                            .ca_certificate(Certificate::from_pem(ca_pem)),
                    )?
                    .connect()
                    .await?;
                Ok(Self(Arc::new(Connection {
                    channel,
                    authorization,
                    client_instance: *uuid::Uuid::now_v7().as_bytes(),
                })))
            }
            .await,
        )
    }

    /// Prepares one immutable Context creation with a stable pre-dispatch identity.
    #[must_use]
    pub fn context(&self, model: impl Into<String>) -> wire::CreateContextRequest {
        wire::CreateContextRequest {
            identity: Some(self.identity()),
            model: model.into(),
            items: Vec::new(),
        }
    }

    /// Creates or reconciles the same prepared Context and requires a retained revision.
    #[tracing::instrument(
        name = "acyclic.inference.context.create",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    pub async fn create_context(
        &self,
        request: &wire::CreateContextRequest,
    ) -> Result<[u8; 32], Error> {
        obs::finish(
            async {
                bounded(request)?;
                let receipt = self
                    .rpc()
                    .create(self.request(request.clone())?)
                    .await?
                    .into_inner();
                validate_receipt(&receipt)?;
                if !receipt.retained {
                    return Err(Error::Invalid("creation did not retain a revision"));
                }
                fixed(&receipt.revision)
            }
            .await,
        )
    }

    /// Reads one retained Context and validates its exact revision and logical identity.
    #[tracing::instrument(
        name = "acyclic.inference.context.inspect",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    pub async fn inspect_context(&self, revision: [u8; 32]) -> Result<wire::ContextView, Error> {
        obs::finish(
            async {
                nonzero(&revision)?;
                let view = self
                    .rpc()
                    .inspect(self.request(wire::InspectContextRequest {
                        revision: revision.to_vec(),
                    })?)
                    .await?
                    .into_inner();
                validate_context_view(&view, revision)?;
                Ok(view)
            }
            .await,
        )
    }

    /// Submits or reconciles the same prepared mutation and validates its receipt.
    #[tracing::instrument(
        name = "acyclic.inference.context.mutate",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    pub async fn mutate_context(
        &self,
        request: &wire::MutateContextRequest,
    ) -> Result<wire::MutationReceipt, Error> {
        obs::finish(
            async {
                bounded(request)?;
                fixed::<32>(&request.source)?;
                let receipt = self
                    .rpc()
                    .mutate(self.request(request.clone())?)
                    .await?
                    .into_inner();
                validate_receipt(&receipt)?;
                Ok(receipt)
            }
            .await,
        )
    }

    /// Prepares one recoverable Run against an explicit immutable revision.
    #[must_use]
    pub fn generate(
        &self,
        revision: [u8; 32],
        input: impl Into<String>,
        maximum_output: u64,
    ) -> wire::GenerateRunRequest {
        wire::GenerateRunRequest {
            identity: Some(self.identity()),
            context: revision.to_vec(),
            input: Some(text_item(wire::ItemKind::User, input.into())),
            maximum_output,
            seed: None,
        }
    }

    /// Prepares one warm-retention request for an explicit immutable revision.
    #[must_use]
    pub fn retention_request(
        &self,
        revision: [u8; 32],
        policy: Retention,
    ) -> wire::RetainWarmRequest {
        wire::RetainWarmRequest {
            identity: Some(self.identity()),
            context: revision.to_vec(),
            latency_profile: if policy.idle_kv.is_some() {
                Vec::new()
            } else {
                policy.latency_profile.to_vec()
            },
            expires_at_ms: policy.expires_at_ms,
            idle_kv: policy
                .idle_kv
                .map(|(profile, idle_timeout_ms)| wire::IdleKvPolicy {
                    profile: profile.to_vec(),
                    idle_timeout_ms,
                }),
        }
    }

    /// Allocates one request identity before effects, retaining this connection's client instance.
    pub fn identity(&self) -> wire::RequestIdentity {
        wire::RequestIdentity {
            client_instance: self.0.client_instance.to_vec(),
            request_id: uuid::Uuid::now_v7().as_bytes().to_vec(),
        }
    }

    fn rpc(&self) -> wire::contexts_service_client::ContextsServiceClient<Channel> {
        wire::contexts_service_client::ContextsServiceClient::new(self.0.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES)
    }

    fn runs(&self) -> wire::runs_service_client::RunsServiceClient<Channel> {
        wire::runs_service_client::RunsServiceClient::new(self.0.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES)
    }

    fn warm(&self) -> wire::warm_contexts_service_client::WarmContextsServiceClient<Channel> {
        wire::warm_contexts_service_client::WarmContextsServiceClient::new(self.0.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES)
    }

    fn discovery(&self) -> wire::models_service_client::ModelsServiceClient<Channel> {
        wire::models_service_client::ModelsServiceClient::new(self.0.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES)
    }

    fn evaluations(&self) -> wire::evaluations_service_client::EvaluationsServiceClient<Channel> {
        wire::evaluations_service_client::EvaluationsServiceClient::new(self.0.channel.clone())
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES)
    }

    /// Prepares one immutable evaluation request with its identity allocated before effects.
    #[must_use]
    pub fn evaluation(&self, spec: wire::EvaluationSpec) -> wire::CreateEvaluationRequest {
        wire::CreateEvaluationRequest {
            identity: Some(self.identity()),
            spec: Some(spec),
        }
    }

    /// Admits or reconciles the same prepared evaluation request.
    #[tracing::instrument(
        name = "acyclic.inference.evaluation.create",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    pub async fn create_evaluation(
        &self,
        request: &wire::CreateEvaluationRequest,
    ) -> Result<[u8; 16], Error> {
        obs::finish(
            async {
                bounded(request)?;
                let expected = fixed(
                    &request
                        .identity
                        .as_ref()
                        .ok_or(Error::Invalid("missing evaluation identity"))?
                        .request_id,
                )?;
                let spec = request
                    .spec
                    .as_ref()
                    .ok_or(Error::Invalid("evaluation spec is absent"))?;
                validate_evaluation_spec(spec)?;
                let view = self
                    .evaluations()
                    .create(self.request(request.clone())?)
                    .await?
                    .into_inner();
                validate_evaluation_admission(&view, expected, spec)?;
                Ok(expected)
            }
            .await,
        )
    }

    /// Inspects one retained evaluation ID and validates its exact result evidence.
    #[tracing::instrument(
        name = "acyclic.inference.evaluation.inspect",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    pub async fn inspect_evaluation(
        &self,
        evaluation_id: [u8; 16],
    ) -> Result<wire::EvaluationView, Error> {
        obs::finish(
            async {
                nonzero(&evaluation_id)?;
                let view = self
                    .evaluations()
                    .inspect(self.request(wire::InspectEvaluationRequest {
                        evaluation_id: evaluation_id.to_vec(),
                    })?)
                    .await?
                    .into_inner();
                validate_evaluation_view(&view, evaluation_id)?;
                Ok(view)
            }
            .await,
        )
    }

    /// Discover exact model revisions and the customer features currently admitted for them.
    #[tracing::instrument(
        name = "acyclic.inference.models",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    pub async fn models(&self) -> Result<Vec<wire::ModelCapability>, Error> {
        obs::finish(
            async {
                let response = self
                    .discovery()
                    .list(self.request(wire::ListModelsRequest {})?)
                    .await?
                    .into_inner();
                if response.models.is_empty() || response.models.len() > 4_096 {
                    return Err(Error::Invalid("model capability count is invalid"));
                }
                let mut names = std::collections::BTreeSet::new();
                for model in &response.models {
                    let mut retention_profiles = std::collections::BTreeSet::new();
                    if model.model.is_empty()
                        || model.model.len() > 256
                        || !names.insert(model.model.as_str())
                        || fixed::<32>(&model.execution_profile).is_err()
                        || model.maximum_context == 0
                        || model.maximum_output == 0
                        || model.features.is_empty()
                        || model.features.len() > 64
                        || model
                            .features
                            .iter()
                            .any(|feature| feature.is_empty() || feature.len() > 64)
                        || model.retention_profiles.len() > 64
                        || model.retention_profiles.iter().any(|profile| {
                            fixed::<32>(&profile.profile).is_err()
                                || profile.minimum_duration_ms == 0
                                || profile.maximum_duration_ms < profile.minimum_duration_ms
                                || !retention_profiles.insert(profile.profile.as_slice())
                        })
                    {
                        return Err(Error::Invalid("model capability is invalid"));
                    }
                }
                Ok(response.models)
            }
            .await,
        )
    }

    /// Admits or reconciles the same prepared Run and validates its identity and input.
    #[tracing::instrument(
        name = "acyclic.inference.run.generate",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    pub async fn generate_run(
        &self,
        request: &wire::GenerateRunRequest,
    ) -> Result<[u8; 16], Error> {
        obs::finish(
            async {
                bounded(request)?;
                if request.maximum_output == 0 {
                    return Err(Error::Invalid("zero output bound"));
                }
                let context = fixed::<32>(&request.context)?;
                let run_id = fixed::<16>(
                    &request
                        .identity
                        .as_ref()
                        .ok_or(Error::Invalid("missing Run identity"))?
                        .request_id,
                )?;
                let response = self
                    .runs()
                    .generate(self.request(request.clone())?)
                    .await?
                    .into_inner();
                let view = response.run.ok_or(Error::Invalid("missing Run response"))?;
                contract::validate_generated_run_view(&view, run_id, context)
                    .map_err(contract_error)?;
                Ok(run_id)
            }
            .await,
        )
    }

    /// Inspects durable Run state and validates its exact identity.
    #[tracing::instrument(
        name = "acyclic.inference.run.inspect",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    pub async fn inspect_run(&self, run_id: [u8; 16]) -> Result<wire::RunView, Error> {
        obs::finish(
            async {
                let view = self
                    .runs()
                    .inspect(self.request(wire::InspectRunRequest {
                        run_id: run_id.to_vec(),
                    })?)
                    .await?
                    .into_inner();
                validate_run_view(&view, run_id)?;
                Ok(view)
            }
            .await,
        )
    }

    /// Resumes the validated event stream at an inclusive zero-based cursor.
    #[tracing::instrument(
        name = "acyclic.inference.run.watch",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    pub async fn watch_run(
        &self,
        run_id: [u8; 16],
        from_sequence: u64,
    ) -> Result<RunEvents, Error> {
        obs::finish(
            async {
                let view = self.inspect_run(run_id).await?;
                let state =
                    contract::watch_run_start(&view, from_sequence).map_err(contract_error)?;
                if state.is_terminal() {
                    return Ok(RunEvents {
                        stream: None,
                        state,
                    });
                }
                let stream = self
                    .runs()
                    .watch(self.request(wire::WatchRunRequest {
                        run_id: run_id.to_vec(),
                        from_sequence,
                    })?)
                    .await?
                    .into_inner();
                Ok(RunEvents {
                    stream: Some(stream),
                    state,
                })
            }
            .await,
        )
    }

    /// Requests durable cancellation and validates the resulting Run state.
    #[tracing::instrument(
        name = "acyclic.inference.run.cancel",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    pub async fn cancel_run(&self, run_id: [u8; 16]) -> Result<wire::RunView, Error> {
        obs::finish(
            async {
                let view = self
                    .runs()
                    .cancel(self.request(wire::InspectRunRequest {
                        run_id: run_id.to_vec(),
                    })?)
                    .await?
                    .into_inner();
                validate_run_view(&view, run_id)?;
                Ok(view)
            }
            .await,
        )
    }

    /// Admits or reconciles the same prepared warm-retention request.
    #[tracing::instrument(
        name = "acyclic.inference.warm.retain",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    pub async fn retain_warm(&self, request: &wire::RetainWarmRequest) -> Result<[u8; 32], Error> {
        obs::finish(
            async {
                bounded(request)?;
                contract::validate_retain_request(request)
                    .map_err(|error| Error::Invalid(error.message()))?;
                let view = self
                    .warm()
                    .retain(self.request(request.clone())?)
                    .await?
                    .into_inner();
                contract::validate_retain_response(&view, request)
                    .map_err(|error| Error::Invalid(error.message()))?;
                fixed(&view.commitment)
            }
            .await,
        )
    }

    /// Inspects one warm commitment and validates the returned lifecycle evidence.
    #[tracing::instrument(
        name = "acyclic.inference.warm.inspect",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    pub async fn inspect_warm(&self, commitment: [u8; 32]) -> Result<wire::WarmView, Error> {
        obs::finish(
            async {
                nonzero(&commitment)?;
                let view = self
                    .warm()
                    .inspect(self.request(wire::InspectWarmRequest {
                        commitment: commitment.to_vec(),
                    })?)
                    .await?
                    .into_inner();
                validate_warm_view(&view, None, Some(commitment))?;
                Ok(view)
            }
            .await,
        )
    }

    /// Renews the same prepared warm promise without changing its requested policy.
    #[tracing::instrument(
        name = "acyclic.inference.warm.renew",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    pub async fn renew_warm(
        &self,
        request: &wire::RenewWarmRequest,
    ) -> Result<wire::WarmView, Error> {
        obs::finish(
            async {
                bounded(request)?;
                contract::validate_renew_request(request)
                    .map_err(|error| Error::Invalid(error.message()))?;
                let view = self
                    .warm()
                    .renew(self.request(request.clone())?)
                    .await?
                    .into_inner();
                contract::validate_renew_response(&view, request)
                    .map_err(|error| Error::Invalid(error.message()))?;
                Ok(view)
            }
            .await,
        )
    }

    /// Releases the same prepared warm promise and requires factual terminal release.
    #[tracing::instrument(
        name = "acyclic.inference.warm.release",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    pub async fn release_warm(
        &self,
        request: &wire::ReleaseWarmRequest,
    ) -> Result<wire::WarmView, Error> {
        obs::finish(
            async {
                bounded(request)?;
                let commitment = fixed::<32>(&request.commitment)?;
                let view = self
                    .warm()
                    .release(self.request(request.clone())?)
                    .await?
                    .into_inner();
                validate_warm_view(&view, None, Some(commitment))?;
                if wire::WarmState::try_from(view.state).unwrap_or(wire::WarmState::Unspecified)
                    != wire::WarmState::Released
                {
                    return Err(Error::Invalid("warm release is not terminal"));
                }
                Ok(view)
            }
            .await,
        )
    }

    fn request<T>(&self, value: T) -> Result<Request<T>, Error> {
        let mut request = Request::new(value);
        // Tonic owns unavoidable transport metadata copies. Retained credential
        // storage is zeroizing and is never cloned into another long-lived field.
        let mut authorization = MetadataValue::<Ascii>::try_from(self.0.authorization.as_str())
            .map_err(|_| Error::Invalid("invalid API key"))?;
        authorization.set_sensitive(true);
        request
            .metadata_mut()
            .insert("authorization", authorization);
        request.set_timeout(Duration::from_secs(60));
        Ok(request)
    }
}

fn authorization(api_key: &str) -> Result<Zeroizing<String>, Error> {
    if api_key.is_empty() || api_key.len() > 8_192 {
        return Err(Error::Invalid("invalid API key"));
    }
    let value = Zeroizing::new(format!("Bearer {api_key}"));
    MetadataValue::<Ascii>::try_from(value.as_str())
        .map_err(|_| Error::Invalid("invalid API key"))?;
    Ok(value)
}

fn nonzero<const N: usize>(value: &[u8; N]) -> Result<(), Error> {
    if *value == [0; N] {
        return Err(Error::Invalid("zero identity"));
    }
    Ok(())
}

fn fixed<const N: usize>(value: &[u8]) -> Result<[u8; N], Error> {
    let bytes = value
        .try_into()
        .map_err(|_| Error::Invalid("identity length differs"))?;
    nonzero(&bytes)?;
    Ok(bytes)
}

fn bounded<M: prost::Message>(message: &M) -> Result<(), Error> {
    if message.encoded_len() > MAXIMUM_MESSAGE_BYTES {
        return Err(Error::Invalid("message exceeds transport ceiling"));
    }
    Ok(())
}

/// Explicit customer warm-retention policy. Distribution and capacity remain
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Retention {
    latency_profile: [u8; 32],
    expires_at_ms: u64,
    idle_kv: Option<([u8; 32], u64)>,
}

impl Retention {
    /// Request an admitted warm promise through an exact published latency profile.
    #[must_use]
    pub const fn warm_until(latency_profile: [u8; 32], expires_at_ms: u64) -> Self {
        Self {
            latency_profile,
            expires_at_ms,
            idle_kv: None,
        }
    }

    /// Paid KV pin with a positive idle timeout; no capacity or latency guarantee.
    #[must_use]
    pub const fn idle_kv(profile: [u8; 32], idle_timeout_ms: u64) -> Self {
        Self {
            latency_profile: [0; 32],
            expires_at_ms: 0,
            idle_kv: Some((profile, idle_timeout_ms)),
        }
    }
}

/// Validating event-stream observation. Dropping it never cancels the Run.
pub struct RunEvents {
    stream: Option<tonic::Streaming<wire::RunEvent>>,
    state: contract::WatchRunState,
}

impl RunEvents {
    /// Read and validate the next ordered event.
    pub async fn next(&mut self) -> Result<Option<wire::RunEvent>, Error> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(None);
        };
        let Some(event) = stream.message().await? else {
            self.state.finish().map_err(Error::Invalid)?;
            return Ok(None);
        };
        contract::watch_run_event(&mut self.state, &event).map_err(contract_error)?;
        Ok(Some(event))
    }
}

fn contract_error(error: contract::Error) -> Error {
    Error::Invalid(error.message())
}

fn validate_evaluation_spec(spec: &wire::EvaluationSpec) -> Result<(), Error> {
    contract::validate_evaluation_spec(spec).map_err(contract_error)
}

fn validate_evaluation_admission(
    view: &wire::EvaluationView,
    expected: [u8; 16],
    spec: &wire::EvaluationSpec,
) -> Result<(), Error> {
    contract::validate_evaluation_admission(view, expected, spec).map_err(contract_error)
}

fn validate_evaluation_view(view: &wire::EvaluationView, expected: [u8; 16]) -> Result<(), Error> {
    contract::validate_evaluation_view(view, expected).map_err(contract_error)
}

fn validate_run_view(view: &wire::RunView, expected: [u8; 16]) -> Result<(), Error> {
    contract::validate_run_view(view, expected).map_err(contract_error)
}

fn validate_context_view(view: &wire::ContextView, expected: [u8; 32]) -> Result<(), Error> {
    contract::validate_context_view(view, expected).map_err(contract_error)
}

fn validate_warm_view(
    view: &wire::WarmView,
    expected_context: Option<[u8; 32]>,
    expected_commitment: Option<[u8; 32]>,
) -> Result<(), Error> {
    contract::validate_warm_view(view, expected_context, expected_commitment)
        .map_err(contract_error)
}

fn validate_receipt(receipt: &wire::MutationReceipt) -> Result<(), Error> {
    contract::validate_receipt(receipt).map_err(contract_error)
}

/// Allocates a fresh item identity before its exact text bytes are dispatched.
pub fn text_item(kind: wire::ItemKind, text: String) -> wire::Item {
    wire::Item {
        id: uuid::Uuid::now_v7().as_bytes().to_vec(),
        kind: kind as i32,
        payload: text.into_bytes(),
        link: Vec::new(),
        continuation_profile: Vec::new(),
    }
}

#[cfg(test)]
mod tests;
