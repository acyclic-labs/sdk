use std::sync::Arc;
use std::time::Duration;
use tonic::Request;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use zeroize::{Zeroize, Zeroizing};

use crate::MAXIMUM_MESSAGE_BYTES;
use crate::contract;
use crate::wire;

/// Customer-only reflection; no backend descriptors or implementation are packaged.
pub const DESCRIPTOR: &[u8] = include_bytes!("../inference_descriptor.bin");
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

struct Connection {
    channel: Channel,
    authorization: Zeroizing<String>,
    client_instance: [u8; 16],
}

/// Authenticated customer connection; infrastructure remains entirely service-owned.
#[derive(Clone)]
pub struct Inference(Arc<Connection>);

impl Inference {
    /// Connect the default remote client over tonic gRPC on authenticated HTTPS, using an explicit trusted CA, ambient `WebPKI` roots, and bounded TLS/RPC deadlines.
    ///
    /// # Errors
    /// Rejects non-HTTPS endpoints, invalid credentials and failed TLS setup.
    pub async fn connect(endpoint: &str, api_key: &str, ca_pem: &[u8]) -> Result<Self, Error> {
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

    /// Start an exact immutable Context creation with a stable pre-dispatch identity.
    #[must_use]
    pub fn context(&self, model: impl Into<String>) -> CreateContext {
        CreateContext {
            client: self.clone(),
            request: wire::CreateContextRequest {
                identity: Some(self.identity()),
                model: model.into(),
                items: Vec::new(),
            },
        }
    }

    /// Authenticate and attach to an existing immutable retained revision.
    ///
    /// # Errors
    /// Rejects unknown/unretained revisions or an invalid service response.
    pub async fn attach(&self, revision: [u8; 32]) -> Result<Context, Error> {
        nonzero(&revision)?;
        let context = Context {
            client: self.clone(),
            revision,
        };
        context.inspect().await?;
        Ok(context)
    }

    fn identity(&self) -> wire::RequestIdentity {
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

    /// Prepare one immutable evaluation admission with a caller-known identity.
    #[must_use]
    pub fn evaluation(&self, spec: wire::EvaluationSpec) -> CreateEvaluation {
        CreateEvaluation {
            client: self.clone(),
            request: wire::CreateEvaluationRequest {
                identity: Some(self.identity()),
                spec: Some(spec),
            },
        }
    }

    /// Recover one previously admitted evaluation without admitting new work.
    #[must_use]
    pub fn recover_evaluation(&self, evaluation_id: [u8; 16]) -> Evaluation {
        Evaluation {
            client: self.clone(),
            evaluation_id,
        }
    }

    /// Discover exact model revisions and the customer features currently admitted for them.
    ///
    /// # Errors
    /// Rejects unauthenticated, malformed, duplicate, or unbounded capability responses.
    pub async fn models(&self) -> Result<Vec<wire::ModelCapability>, Error> {
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

    /// Recover one previously admitted Run by its caller-known identity.
    #[must_use]
    pub fn recover_run(&self, run_id: [u8; 16]) -> Run {
        Run {
            client: self.clone(),
            run_id,
        }
    }

    /// Recover a previously admitted warm commitment without creating another.
    #[must_use]
    pub fn recover_warm(&self, commitment: [u8; 32]) -> WarmContext {
        WarmContext {
            client: self.clone(),
            commitment,
        }
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

/// Replayable immutable evaluation admission. Reusing this value reconciles the
/// same candidate/suite/grader contract and never creates another evaluation.
#[derive(Clone)]
pub struct CreateEvaluation {
    client: Inference,
    request: wire::CreateEvaluationRequest,
}

impl CreateEvaluation {
    /// Caller-known evaluation identity allocated before network effects.
    pub fn id(&self) -> Result<[u8; 16], Error> {
        fixed(
            &self
                .request
                .identity
                .as_ref()
                .ok_or(Error::Invalid("missing evaluation identity"))?
                .request_id,
        )
    }

    /// Admit or reconcile this exact immutable evaluation.
    pub async fn send(&self) -> Result<Evaluation, Error> {
        bounded(&self.request)?;
        let expected = self.id()?;
        validate_evaluation_spec(
            self.request
                .spec
                .as_ref()
                .ok_or(Error::Invalid("evaluation spec is absent"))?,
        )?;
        let view = self
            .client
            .evaluations()
            .create(self.client.request(self.request.clone())?)
            .await?
            .into_inner();
        validate_evaluation_admission(
            &view,
            expected,
            self.request
                .spec
                .as_ref()
                .ok_or(Error::Invalid("evaluation spec is absent"))?,
        )?;
        Ok(Evaluation {
            client: self.client.clone(),
            evaluation_id: expected,
        })
    }
}

/// Recoverable immutable evaluation handle.
#[derive(Clone)]
pub struct Evaluation {
    client: Inference,
    evaluation_id: [u8; 16],
}

impl Evaluation {
    /// Stable caller-known evaluation identity.
    #[must_use]
    pub const fn id(&self) -> [u8; 16] {
        self.evaluation_id
    }

    /// Inspect durable evaluation state and exact result evidence.
    pub async fn inspect(&self) -> Result<wire::EvaluationView, Error> {
        nonzero(&self.evaluation_id)?;
        let view = self
            .client
            .evaluations()
            .inspect(self.client.request(wire::InspectEvaluationRequest {
                evaluation_id: self.evaluation_id.to_vec(),
            })?)
            .await?
            .into_inner();
        validate_evaluation_view(&view, self.evaluation_id)?;
        Ok(view)
    }
}

/// Replayable creation builder. Reusing it reconciles the same exact command.
#[derive(Clone)]
pub struct CreateContext {
    client: Inference,
    request: wire::CreateContextRequest,
}

impl CreateContext {
    /// Append exact instruction bytes with a fresh item identity.
    #[must_use]
    pub fn instructions(self, text: impl Into<String>) -> Self {
        self.item(text_item(wire::ItemKind::Instruction, text.into()))
    }

    /// Add one typed item. Semantic validation belongs to the authenticated service.
    #[must_use]
    pub fn item(mut self, item: wire::Item) -> Self {
        self.request.items.push(item);
        self
    }

    /// Caller-known command identity, available before network effects.
    #[must_use]
    pub fn identity(&self) -> Option<&wire::RequestIdentity> {
        self.request.identity.as_ref()
    }

    /// Create or reconcile this immutable Context; never repeat logical work.
    ///
    /// # Errors
    /// Failed observations require the same builder, not a new creation command.
    pub async fn create(&self) -> Result<Context, Error> {
        bounded(&self.request)?;
        let receipt = self
            .client
            .rpc()
            .create(self.client.request(self.request.clone())?)
            .await?
            .into_inner();
        validate_receipt(&receipt)?;
        if !receipt.retained {
            return Err(Error::Invalid("creation did not retain a revision"));
        }
        Ok(Context {
            client: self.client.clone(),
            revision: fixed(&receipt.revision)?,
        })
    }
}

/// Immutable revision handle; cloning a handle does not create a fork or retention edge.
#[derive(Clone)]
pub struct Context {
    client: Inference,
    revision: [u8; 32],
}

/// Explicit customer warm-retention policy. Distribution and capacity remain
/// service-owned; the opaque profile must come from model discovery.
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

impl Context {
    /// Portable revision identity for later authenticated attachment.
    #[must_use]
    pub const fn id(&self) -> [u8; 32] {
        self.revision
    }

    /// Read exact retained content and its pinned logical execution identity.
    ///
    /// # Errors
    /// Returns service rejection or malformed response without fabricating content.
    pub async fn inspect(&self) -> Result<wire::ContextView, Error> {
        let view = self
            .client
            .rpc()
            .inspect(self.client.request(wire::InspectContextRequest {
                revision: self.revision.to_vec(),
            })?)
            .await?
            .into_inner();
        validate_context_view(&view, self.revision)?;
        Ok(view)
    }

    /// Prepare an independently retained fork. Sending twice reconciles that fork.
    #[must_use]
    pub fn fork(&self) -> ContextMutation {
        self.mutation(wire::mutate_context_request::Action::Fork(wire::Empty {}))
    }

    /// Prepare exact atomic item edits, leaving this revision unchanged.
    #[must_use]
    pub fn edit(&self, edits: Vec<wire::Edit>) -> ContextMutation {
        self.mutation(wire::mutate_context_request::Action::Edit(wire::Edits {
            edits,
        }))
    }

    /// Prepare one exact user-message append.
    #[must_use]
    pub fn append(&self, text: impl Into<String>) -> ContextMutation {
        self.edit(vec![wire::Edit {
            action: Some(wire::edit::Action::Append(text_item(
                wire::ItemKind::User,
                text.into(),
            ))),
        }])
    }

    /// Retain exactly the selected prefix; None explicitly means an empty prefix.
    #[must_use]
    pub fn truncate(&self, through: Option<[u8; 16]>) -> ContextMutation {
        self.mutation(wire::mutate_context_request::Action::Truncate(
            wire::Truncate {
                through: through.map(|id| id.to_vec()),
            },
        ))
    }

    /// Prepare explicit replacement of selected items; no summary is generated.
    #[must_use]
    pub fn compact(
        &self,
        selected: Vec<[u8; 16]>,
        replacement: Vec<wire::Item>,
    ) -> ContextMutation {
        self.mutation(wire::mutate_context_request::Action::Compact(
            wire::Compact {
                selected: selected.into_iter().map(|id| id.to_vec()).collect(),
                replacement,
            },
        ))
    }

    /// Release only this revision's own edge, never descendants or physical bytes.
    #[must_use]
    pub fn release(&self) -> ContextMutation {
        self.mutation(wire::mutate_context_request::Action::Release(
            wire::Empty {},
        ))
    }

    /// Resolve a target model revision and replay this exact canonical content into a new Context.
    #[must_use]
    pub fn transfer(&self, model: impl Into<String>) -> ContextMutation {
        self.mutation(wire::mutate_context_request::Action::Transfer(
            wire::Transfer {
                model: model.into(),
            },
        ))
    }

    /// Prepare one recoverable generation against this exact immutable revision.
    #[must_use]
    pub fn generate(&self, input: impl Into<String>, maximum_output: u64) -> GenerateRun {
        let identity = self.client.identity();
        GenerateRun {
            client: self.client.clone(),
            request: wire::GenerateRunRequest {
                identity: Some(identity),
                context: self.revision.to_vec(),
                input: Some(text_item(wire::ItemKind::User, input.into())),
                maximum_output,
                seed: None,
            },
        }
    }

    /// Prepare one replayable warm-retention admission for this exact revision.
    #[must_use]
    pub fn retain(&self, policy: Retention) -> RetainWarm {
        RetainWarm {
            client: self.client.clone(),
            request: wire::RetainWarmRequest {
                identity: Some(self.client.identity()),
                context: self.revision.to_vec(),
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
            },
        }
    }

    fn mutation(&self, action: wire::mutate_context_request::Action) -> ContextMutation {
        ContextMutation {
            client: self.client.clone(),
            request: wire::MutateContextRequest {
                identity: Some(self.client.identity()),
                source: self.revision.to_vec(),
                action: Some(action),
            },
        }
    }
}

/// Replayable warm admission. A failed observation must be reconciled by
/// sending this same value, never by allocating another request identity.
#[derive(Clone)]
pub struct RetainWarm {
    client: Inference,
    request: wire::RetainWarmRequest,
}

impl RetainWarm {
    /// Caller-known request identity allocated before effects.
    #[must_use]
    pub fn identity(&self) -> Option<&wire::RequestIdentity> {
        self.request.identity.as_ref()
    }

    /// Admit or reconcile the exact warm promise.
    ///
    /// # Errors
    /// Rejects malformed policy, transport failure, service rejection, or a
    /// response not bound to the requested Context.
    pub async fn send(&self) -> Result<WarmContext, Error> {
        bounded(&self.request)?;
        let expected_context = fixed::<32>(&self.request.context)?;
        contract::validate_retain_request(&self.request)
            .map_err(|error| Error::Invalid(error.message()))?;
        let view = self
            .client
            .warm()
            .retain(self.client.request(self.request.clone())?)
            .await?
            .into_inner();
        validate_warm_view(&view, Some(expected_context), None)?;
        if self.request.idle_kv.is_some() != view.idle_kv.is_some() {
            return Err(Error::Invalid("retention mode differs"));
        }
        if let Some(policy) = &self.request.idle_kv
            && view.idle_kv.as_ref().and_then(|idle| idle.policy.as_ref()) != Some(policy)
        {
            return Err(Error::Invalid("idle retention policy differs"));
        }
        Ok(WarmContext {
            client: self.client.clone(),
            commitment: fixed(&view.commitment)?,
        })
    }
}

/// Recoverable warm commitment. It never exposes physical placement or KV
/// allocation identity and therefore remains valid through service rebalancing.
#[derive(Clone)]
pub struct WarmContext {
    client: Inference,
    commitment: [u8; 32],
}

impl WarmContext {
    /// Stable commitment identity.
    #[must_use]
    pub const fn id(&self) -> [u8; 32] {
        self.commitment
    }

    /// Inspect the latest durable logical warm fact.
    ///
    /// # Errors
    /// Rejects transport/service failure or malformed commitment evidence.
    pub async fn inspect(&self) -> Result<wire::WarmView, Error> {
        nonzero(&self.commitment)?;
        let view = self
            .client
            .warm()
            .inspect(self.client.request(wire::InspectWarmRequest {
                commitment: self.commitment.to_vec(),
            })?)
            .await?
            .into_inner();
        validate_warm_view(&view, None, Some(self.commitment))?;
        Ok(view)
    }

    /// Prepare an extension of the current promise. Reusing the returned builder
    /// reconciles the same renewal; a failed renewal leaves the prior promise intact.
    #[must_use]
    pub fn renew(&self, expires_at_ms: u64) -> RenewWarm {
        RenewWarm {
            client: self.client.clone(),
            request: wire::RenewWarmRequest {
                identity: Some(self.client.identity()),
                commitment: self.commitment.to_vec(),
                expires_at_ms,
                idle_timeout_ms: None,
            },
        }
    }

    /// Change idle timeout from the last verified use (initial pin before first use).
    /// Does not advance last-use time or resurrect an expired/released commitment.
    #[must_use]
    pub fn renew_idle(&self, idle_timeout_ms: u64) -> RenewWarm {
        RenewWarm {
            client: self.client.clone(),
            request: wire::RenewWarmRequest {
                identity: Some(self.client.identity()),
                commitment: self.commitment.to_vec(),
                expires_at_ms: 0,
                idle_timeout_ms: Some(idle_timeout_ms),
            },
        }
    }

    /// Prepare cleanup and release of only this warm promise.
    #[must_use]
    pub fn release(&self) -> ReleaseWarm {
        ReleaseWarm {
            client: self.client.clone(),
            request: wire::ReleaseWarmRequest {
                identity: Some(self.client.identity()),
                commitment: self.commitment.to_vec(),
            },
        }
    }
}

/// Replayable warm renewal.
#[derive(Clone)]
pub struct RenewWarm {
    client: Inference,
    request: wire::RenewWarmRequest,
}

impl RenewWarm {
    /// Identity allocated before effects; reuse this builder to reconcile retries.
    #[must_use]
    pub fn identity(&self) -> Option<&wire::RequestIdentity> {
        self.request.identity.as_ref()
    }

    /// Extend and reconcile this exact commitment.
    ///
    /// # Errors
    /// Rejects malformed expiry, transport/service failure, or a foreign response.
    pub async fn send(&self) -> Result<wire::WarmView, Error> {
        bounded(&self.request)?;
        let commitment = fixed::<32>(&self.request.commitment)?;
        contract::validate_renew_request(&self.request)
            .map_err(|error| Error::Invalid(error.message()))?;
        let view = self
            .client
            .warm()
            .renew(self.client.request(self.request.clone())?)
            .await?
            .into_inner();
        validate_warm_view(&view, None, Some(commitment))?;
        if self.request.idle_timeout_ms.is_some() != view.idle_kv.is_some() {
            return Err(Error::Invalid("renewal retention mode differs"));
        }
        if let Some(timeout) = self.request.idle_timeout_ms
            && view
                .idle_kv
                .as_ref()
                .and_then(|idle| idle.policy.as_ref())
                .map(|policy| policy.idle_timeout_ms)
                != Some(timeout)
        {
            return Err(Error::Invalid("idle renewal timeout differs"));
        }
        Ok(view)
    }
}

/// Replayable cleanup-complete release.
#[derive(Clone)]
pub struct ReleaseWarm {
    client: Inference,
    request: wire::ReleaseWarmRequest,
}

impl ReleaseWarm {
    /// Release or reconcile this exact warm promise.
    ///
    /// # Errors
    /// Rejects transport/service failure or a response without factual release.
    pub async fn send(&self) -> Result<wire::WarmView, Error> {
        bounded(&self.request)?;
        let commitment = fixed::<32>(&self.request.commitment)?;
        let view = self
            .client
            .warm()
            .release(self.client.request(self.request.clone())?)
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
}

/// Replayable Run admission builder. Reusing it reconciles one logical Run.
#[derive(Clone)]
pub struct GenerateRun {
    client: Inference,
    request: wire::GenerateRunRequest,
}

impl GenerateRun {
    /// Pin a deterministic sampling seed.
    #[must_use]
    pub fn seed(mut self, seed: u64) -> Self {
        self.request.seed = Some(seed);
        self
    }

    /// Caller-known Run identity allocated before network effects.
    ///
    /// # Errors
    /// Rejects a missing, zero, or incorrectly sized identity.
    pub fn id(&self) -> Result<[u8; 16], Error> {
        fixed(
            &self
                .request
                .identity
                .as_ref()
                .ok_or(Error::Invalid("missing Run identity"))?
                .request_id,
        )
    }

    /// Admit or reconcile this exact Run without creating a successor.
    ///
    /// # Errors
    /// An unavailable response requires replaying this builder, never allocating another Run.
    pub async fn send(&self) -> Result<Run, Error> {
        bounded(&self.request)?;
        if self.request.maximum_output == 0 {
            return Err(Error::Invalid("zero output bound"));
        }
        let context = fixed::<32>(&self.request.context)?;
        let run_id = self.id()?;
        let response = self
            .client
            .runs()
            .generate(self.client.request(self.request.clone())?)
            .await?
            .into_inner();
        let view = response.run.ok_or(Error::Invalid("missing Run response"))?;
        contract::validate_generated_run_view(&view, run_id, context).map_err(contract_error)?;
        Ok(Run {
            client: self.client.clone(),
            run_id,
        })
    }
}

/// Recoverable logical Run. Cloning this handle never repeats execution.
#[derive(Clone)]
pub struct Run {
    client: Inference,
    run_id: [u8; 16],
}

impl Run {
    /// Caller-known stable Run identity.
    #[must_use]
    pub const fn id(&self) -> [u8; 16] {
        self.run_id
    }

    fn inspect_request(&self) -> wire::InspectRunRequest {
        wire::InspectRunRequest {
            run_id: self.run_id.to_vec(),
        }
    }

    /// Inspect durable Run state without opening a watch or admitting work.
    ///
    /// # Errors
    /// Returns authenticated service rejection or malformed response evidence.
    pub async fn inspect(&self) -> Result<wire::RunView, Error> {
        let view = self
            .client
            .runs()
            .inspect(self.client.request(self.inspect_request())?)
            .await?
            .into_inner();
        validate_run_view(&view, self.run_id)?;
        Ok(view)
    }

    /// Resume the ordered event stream at an inclusive zero-based cursor.
    ///
    /// # Errors
    /// Returns transport or authenticated service rejection before the stream is established.
    pub async fn watch(&self, from_sequence: u64) -> Result<RunEvents, Error> {
        let view = self.inspect().await?;
        let state = contract::watch_run_start(&view, from_sequence).map_err(contract_error)?;
        if state.is_terminal() {
            return Ok(RunEvents {
                stream: None,
                state,
            });
        }
        let stream = self
            .client
            .runs()
            .watch(self.client.request(wire::WatchRunRequest {
                run_id: self.run_id.to_vec(),
                from_sequence,
            })?)
            .await?
            .into_inner();
        Ok(RunEvents {
            stream: Some(stream),
            state,
        })
    }

    /// Request durable cancellation of this Run only.
    ///
    /// # Errors
    /// Returns authenticated service rejection or malformed post-cancellation state.
    pub async fn cancel(&self) -> Result<wire::RunView, Error> {
        let view = self
            .client
            .runs()
            .cancel(self.client.request(self.inspect_request())?)
            .await?
            .into_inner();
        validate_run_view(&view, self.run_id)?;
        Ok(view)
    }
}

/// Validating event-stream observation. Dropping it never cancels the Run.
pub struct RunEvents {
    stream: Option<tonic::Streaming<wire::RunEvent>>,
    state: contract::WatchRunState,
}

impl RunEvents {
    /// Read and validate the next ordered event.
    ///
    /// # Errors
    /// Rejects transport failure, a gap/reorder, malformed event, invalid terminal, or a stream
    /// that closes without terminal evidence.
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

/// Replayable mutation with a pre-dispatch command identity, not an execution retry.
#[derive(Clone)]
pub struct ContextMutation {
    client: Inference,
    request: wire::MutateContextRequest,
}

impl ContextMutation {
    /// Caller-known command identity before dispatch.
    #[must_use]
    pub fn identity(&self) -> Option<&wire::RequestIdentity> {
        self.request.identity.as_ref()
    }

    /// Submit or reconcile the exact command. The receipt is not a billing/deletion proof.
    ///
    /// # Errors
    /// An unavailable observation does not imply the mutation failed to commit.
    pub async fn send(&self) -> Result<wire::MutationReceipt, Error> {
        bounded(&self.request)?;
        let receipt = self
            .client
            .rpc()
            .mutate(self.client.request(self.request.clone())?)
            .await?
            .into_inner();
        validate_receipt(&receipt)?;
        Ok(receipt)
    }
}

fn validate_receipt(receipt: &wire::MutationReceipt) -> Result<(), Error> {
    contract::validate_receipt(receipt).map_err(contract_error)
}

fn text_item(kind: wire::ItemKind, text: String) -> wire::Item {
    wire::Item {
        id: uuid::Uuid::now_v7().as_bytes().to_vec(),
        kind: kind as i32,
        payload: text.into_bytes(),
        link: Vec::new(),
        continuation_profile: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use rcgen::generate_simple_self_signed;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::{Identity, Server, ServerTlsConfig};
    use tonic::{Request, Response, Status};

    #[derive(Default)]
    struct ModelsService;

    #[tonic::async_trait]
    impl wire::models_service_server::ModelsService for ModelsService {
        async fn list(
            &self,
            _request: Request<wire::ListModelsRequest>,
        ) -> Result<Response<wire::ListModelsResponse>, Status> {
            Ok(Response::new(wire::ListModelsResponse {
                models: vec![wire::ModelCapability {
                    model: "fixture-model".to_owned(),
                    execution_profile: vec![1; 32],
                    maximum_context: 1,
                    maximum_output: 1,
                    features: vec!["generate".to_owned()],
                    ..Default::default()
                }],
            }))
        }
    }

    #[derive(Default)]
    struct ContextsService;

    #[tonic::async_trait]
    impl wire::contexts_service_server::ContextsService for ContextsService {
        async fn create(
            &self,
            request: Request<wire::CreateContextRequest>,
        ) -> Result<Response<wire::MutationReceipt>, Status> {
            assert_eq!(request.into_inner().model, "fixture-model");
            Ok(Response::new(wire::MutationReceipt {
                revision: vec![1; 32],
                command_digest: vec![2; 32],
                sequence: 1,
                retained: true,
            }))
        }

        async fn inspect(
            &self,
            request: Request<wire::InspectContextRequest>,
        ) -> Result<Response<wire::ContextView>, Status> {
            assert_eq!(request.into_inner().revision, vec![1; 32]);
            Ok(Response::new(wire::ContextView {
                revision: vec![1; 32],
                lineage: vec![3; 32],
                execution_profile: vec![4; 32],
                content_digest: vec![5; 32],
                model: "fixture-model".to_owned(),
                provenance: Some(wire::ContextProvenance {
                    origin: Some(wire::context_provenance::Origin::Created(wire::Empty {})),
                }),
                ..Default::default()
            }))
        }

        async fn mutate(
            &self,
            _request: Request<wire::MutateContextRequest>,
        ) -> Result<Response<wire::MutationReceipt>, Status> {
            Ok(Response::new(wire::MutationReceipt {
                revision: vec![6; 32],
                command_digest: vec![7; 32],
                sequence: 2,
                retained: false,
            }))
        }
    }

    #[derive(Clone, Default)]
    struct RunsService {
        state: Arc<std::sync::Mutex<RunFixtureState>>,
    }

    #[derive(Default)]
    struct RunFixtureState {
        run_id: Vec<u8>,
        context: Vec<u8>,
        cancelled: bool,
    }

    fn run_view(state: &RunFixtureState) -> wire::RunView {
        wire::RunView {
            run_id: state.run_id.clone(),
            input: state.context.clone(),
            model: "fixture-model".to_owned(),
            last_sequence: if state.cancelled { 1 } else { 0 },
            cancellation_requested: state.cancelled,
            result: state.cancelled.then(|| wire::RunResult {
                output: Vec::new(),
                context: None,
                terminal: wire::RunTerminal::Cancelled.into(),
                receipt: None,
            }),
        }
    }

    #[tonic::async_trait]
    impl wire::runs_service_server::RunsService for RunsService {
        async fn generate(
            &self,
            request: Request<wire::GenerateRunRequest>,
        ) -> Result<Response<wire::GenerateRunResponse>, Status> {
            let request = request.into_inner();
            let identity = request
                .identity
                .ok_or_else(|| Status::invalid_argument("missing request identity"))?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| Status::internal("run fixture lock poisoned"))?;
            state.run_id = identity.request_id;
            state.context = request.context;
            state.cancelled = false;
            Ok(Response::new(wire::GenerateRunResponse {
                run: Some(run_view(&state)),
            }))
        }

        async fn inspect(
            &self,
            request: Request<wire::InspectRunRequest>,
        ) -> Result<Response<wire::RunView>, Status> {
            let request = request.into_inner();
            let state = self
                .state
                .lock()
                .map_err(|_| Status::internal("run fixture lock poisoned"))?;
            if request.run_id != state.run_id {
                return Err(Status::not_found("unknown run"));
            }
            Ok(Response::new(run_view(&state)))
        }

        type WatchStream = std::pin::Pin<
            Box<dyn tokio_stream::Stream<Item = Result<wire::RunEvent, Status>> + Send>,
        >;

        async fn watch(
            &self,
            request: Request<wire::WatchRunRequest>,
        ) -> Result<Response<Self::WatchStream>, Status> {
            let request = request.into_inner();
            let mut state = self
                .state
                .lock()
                .map_err(|_| Status::internal("run fixture lock poisoned"))?;
            if request.run_id != state.run_id || request.from_sequence != 0 {
                return Err(Status::invalid_argument("unexpected run cursor"));
            }
            state.cancelled = true;
            let events = [
                Ok(wire::RunEvent {
                    sequence: 0,
                    event: Some(wire::run_event::Event::Progress(wire::RunProgress {
                        kind: "cancellation-requested".to_owned(),
                    })),
                }),
                Ok(wire::RunEvent {
                    sequence: 1,
                    event: Some(wire::run_event::Event::Terminal(
                        wire::RunTerminal::Cancelled.into(),
                    )),
                }),
            ];
            Ok(Response::new(Box::pin(tokio_stream::iter(events))))
        }

        async fn cancel(
            &self,
            request: Request<wire::InspectRunRequest>,
        ) -> Result<Response<wire::RunView>, Status> {
            let request = request.into_inner();
            let mut state = self
                .state
                .lock()
                .map_err(|_| Status::internal("run fixture lock poisoned"))?;
            if request.run_id != state.run_id {
                return Err(Status::not_found("unknown run"));
            }
            state.cancelled = true;
            Ok(Response::new(run_view(&state)))
        }
    }

    fn evaluation_spec() -> wire::EvaluationSpec {
        wire::EvaluationSpec {
            candidates: vec![wire::EvaluationArtifact {
                digest: vec![1; 32],
                media_type: "application/vnd.acyclic.model".to_owned(),
                logical_size: 1024,
            }],
            suite: Some(wire::EvaluationSuite {
                identity: "native-output-binding-v1".to_owned(),
                digest: vec![2; 32],
                cases: vec![wire::EvaluationCase {
                    case_id: vec![3; 16],
                    input: b"deterministic device input".to_vec(),
                    input_artifact_digest: None,
                }],
            }),
            grader: Some(wire::EvaluationGrader {
                handle: b"grader://exact-v1".to_vec(),
                artifact_digest: vec![4; 32],
            }),
            metrics: vec![wire::EvaluationMetric {
                identity: "exact-match".to_owned(),
                aggregation: wire::EvaluationAggregation::Mean.into(),
            }],
            maximum_case_results: 1,
            spec_digest: vec![5; 32],
        }
    }

    fn completed_evaluation() -> wire::EvaluationView {
        let native_output_digest = [7; 32];
        let grader_observation_digest = [8; 32];
        wire::EvaluationView {
            evaluation_id: vec![6; 16],
            spec: Some(evaluation_spec()),
            state: wire::EvaluationState::Completed.into(),
            result: Some(wire::EvaluationResult {
                spec_digest: vec![5; 32],
                case_results: vec![wire::EvaluationCaseResult {
                    candidate_digest: vec![1; 32],
                    case_id: vec![3; 16],
                    observation: Some(wire::EvaluationGraderObservation {
                        native_output_digest: native_output_digest.to_vec(),
                        observation_digest: grader_observation_digest.to_vec(),
                        binding_digest: contract::evaluation_observation_binding(
                            &native_output_digest,
                            &grader_observation_digest,
                        )
                        .to_vec(),
                    }),
                    metrics: vec![wire::EvaluationMetricValue {
                        metric_identity: "exact-match".to_owned(),
                        value: Some(wire::ExactRational {
                            numerator: 1,
                            denominator: 1,
                        }),
                    }],
                    outcome: wire::EvaluationCaseOutcome::Scored.into(),
                }],
                aggregates: vec![wire::EvaluationAggregate {
                    candidate_digest: vec![1; 32],
                    metric_identity: "exact-match".to_owned(),
                    aggregation: wire::EvaluationAggregation::Mean.into(),
                    value: Some(wire::ExactRational {
                        numerator: 1,
                        denominator: 1,
                    }),
                }],
                result_digest: vec![9; 32],
            }),
            sequence: 2,
        }
    }

    #[test]
    fn descriptor_contains_customer_and_validation_files() -> Result<(), Box<dyn std::error::Error>>
    {
        let descriptor = prost_types::FileDescriptorSet::decode(DESCRIPTOR)?;
        assert_eq!(descriptor.file.len(), 3);
        let descriptor_names: Vec<_> = descriptor
            .file
            .iter()
            .map(|file| file.name.as_deref())
            .collect();
        assert_eq!(
            descriptor_names,
            [
                Some("google/protobuf/descriptor.proto"),
                Some("validation/v1/options.proto"),
                Some("inference/v1/inference.proto"),
            ]
        );
        let file = descriptor
            .file
            .iter()
            .find(|file| file.name.as_deref() == Some("inference/v1/inference.proto"))
            .ok_or("inference descriptor is missing")?;
        assert_eq!(file.package.as_deref(), Some("inference.customer.v1"));
        assert_eq!(
            file.dependency,
            vec!["validation/v1/options.proto".to_owned()]
        );
        Ok(())
    }

    #[test]
    #[allow(
        clippy::indexing_slicing,
        reason = "each index is preceded by an assert_eq! on the corresponding Vec's len(), so the index is proven in-bounds"
    )]
    fn descriptor_exposes_customer_services_and_messages() -> Result<(), Box<dyn std::error::Error>>
    {
        let descriptor = prost_types::FileDescriptorSet::decode(DESCRIPTOR)?;
        let file = descriptor
            .file
            .iter()
            .find(|file| file.name.as_deref() == Some("inference/v1/inference.proto"))
            .ok_or("inference descriptor is missing")?;
        assert_eq!(file.service.len(), 5);
        assert_eq!(file.service[0].name.as_deref(), Some("ModelsService"));
        assert_eq!(file.service[0].method.len(), 1);
        assert_eq!(file.service[1].name.as_deref(), Some("ContextsService"));
        assert_eq!(file.service[1].method.len(), 3);
        assert_eq!(file.service[2].name.as_deref(), Some("WarmContextsService"));
        assert_eq!(file.service[2].method.len(), 4);
        assert_eq!(file.service[3].name.as_deref(), Some("RunsService"));
        assert_eq!(file.service[3].method.len(), 4);
        assert_eq!(file.service[4].name.as_deref(), Some("EvaluationsService"));
        assert_eq!(file.service[4].method.len(), 2);
        let names: Vec<_> = file
            .message_type
            .iter()
            .map(|message| message.name.as_deref())
            .collect();
        assert_eq!(
            names,
            [
                "ListModelsRequest",
                "ListModelsResponse",
                "ModelCapability",
                "RetentionProfile",
                "RetainWarmRequest",
                "IdleKvPolicy",
                "IdleKvRetention",
                "InspectWarmRequest",
                "RenewWarmRequest",
                "ReleaseWarmRequest",
                "WarmView",
                "EvaluationArtifact",
                "EvaluationCase",
                "EvaluationSuite",
                "EvaluationGrader",
                "EvaluationMetric",
                "EvaluationSpec",
                "CreateEvaluationRequest",
                "InspectEvaluationRequest",
                "ExactRational",
                "EvaluationMetricValue",
                "EvaluationCaseResult",
                "EvaluationGraderObservation",
                "EvaluationAggregate",
                "EvaluationResult",
                "EvaluationView",
                "RequestIdentity",
                "Item",
                "CreateContextRequest",
                "InspectContextRequest",
                "Empty",
                "Insert",
                "Replace",
                "Edit",
                "Edits",
                "Truncate",
                "Compact",
                "Transfer",
                "MutateContextRequest",
                "MutationReceipt",
                "ContextView",
                "ContextProvenance",
                "ProvenanceSource",
                "TransferProvenance",
                "GenerationProvenance",
                "RunInputProvenance",
                "GenerateRunRequest",
                "GenerateRunResponse",
                "InspectRunRequest",
                "WatchRunRequest",
                "LogicalUsage",
                "UsageReceipt",
                "RunResult",
                "RunView",
                "RunEvent",
                "RunProgress"
            ]
            .map(Some)
        );
        let manifest = include_str!("../Cargo.toml");
        for dependency in ["inference-protocol", "inference-client", "git ="] {
            assert!(
                !manifest.contains(dependency),
                "private/source dependency in customer manifest"
            );
        }
        Ok(())
    }

    #[test]
    fn descriptor_declares_unique_http_routes_for_every_rpc()
    -> Result<(), Box<dyn std::error::Error>> {
        let pool = prost_reflect::DescriptorPool::decode(DESCRIPTOR)?;
        let file = pool
            .get_file_by_name("inference/v1/inference.proto")
            .ok_or("inference descriptor is missing")?;
        let extension = pool
            .get_extension_by_name("acyclic.validation.v1.http_path")
            .ok_or("http_path descriptor extension is missing")?;
        let expected = [
            "models/list",
            "contexts/create",
            "contexts/inspect",
            "contexts/mutate",
            "warm/retain",
            "warm/inspect",
            "warm/renew",
            "warm/release",
            "runs/generate",
            "runs/inspect",
            "runs/watch",
            "runs/cancel",
            "evaluations/create",
            "evaluations/inspect",
        ];
        let mut routes = Vec::new();
        for service in file.services() {
            for method in service.methods() {
                let options = method.options();
                assert!(
                    options.has_extension(&extension),
                    "{}.{} has no http_path",
                    service.name(),
                    method.name()
                );
                let path = options.get_extension(&extension).into_owned();
                let prost_reflect::Value::String(path) = path else {
                    return Err(format!(
                        "{}.{} has an invalid http_path option",
                        service.name(),
                        method.name()
                    )
                    .into());
                };
                assert!(!path.is_empty());
                routes.push(path);
            }
        }
        assert_eq!(routes.len(), expected.len());
        assert_eq!(routes, expected);
        let unique: std::collections::HashSet<_> = routes.iter().collect();
        assert_eq!(unique.len(), routes.len());
        Ok(())
    }

    #[test]
    fn rejects_incomplete_receipts_and_sensitive_credentials() -> Result<(), Error> {
        assert!(validate_receipt(&wire::MutationReceipt::default()).is_err());
        assert!(validate_warm_view(&wire::WarmView::default(), None, None).is_err());
        let valid = wire::WarmView {
            commitment: vec![1; 32],
            context: vec![2; 32],
            model_profile: vec![3; 32],
            latency_profile: vec![4; 32],
            expires_at_ms: 1,
            state: wire::WarmState::Active.into(),
            evidence_digest: vec![5; 32],
            admission_receipt_id: vec![6; 32],
            sequence: 1,
            idle_kv: None,
        };
        validate_warm_view(&valid, Some([2; 32]), Some([1; 32]))?;
        assert!(authorization("").is_err());
        assert!(authorization("line\nbreak").is_err());
        let secret = authorization("secret")?;
        assert_eq!(secret.as_str(), "Bearer secret");
        Ok(())
    }

    #[test]
    fn evaluation_contract_binds_native_output_to_exact_grader_observation() -> Result<(), Error> {
        let view = completed_evaluation();
        validate_evaluation_view(&view, [6; 16])?;

        let mut wrong_output = view.clone();
        wrong_output
            .result
            .as_mut()
            .and_then(|result| result.case_results.first_mut())
            .and_then(|result| result.observation.as_mut())
            .ok_or(Error::Invalid("missing case result"))?
            .native_output_digest = vec![0; 32];
        assert!(validate_evaluation_view(&wrong_output, [6; 16]).is_err());

        let mut wrong_grader = view.clone();
        wrong_grader
            .result
            .as_mut()
            .and_then(|result| result.case_results.first_mut())
            .and_then(|result| result.observation.as_mut())
            .ok_or(Error::Invalid("missing case result"))?
            .observation_digest = vec![9; 32];
        assert!(validate_evaluation_view(&wrong_grader, [6; 16]).is_err());

        let mut missing_score = view.clone();
        missing_score
            .result
            .as_mut()
            .and_then(|result| result.case_results.first_mut())
            .ok_or(Error::Invalid("missing case result"))?
            .metrics
            .clear();
        assert!(validate_evaluation_view(&missing_score, [6; 16]).is_err());

        let mut missing_aggregate = view.clone();
        missing_aggregate
            .result
            .as_mut()
            .ok_or(Error::Invalid("missing evaluation result"))?
            .aggregates
            .clear();
        assert!(validate_evaluation_view(&missing_aggregate, [6; 16]).is_err());

        let mut changed_spec = evaluation_spec();
        changed_spec.spec_digest = vec![10; 32];
        assert!(validate_evaluation_admission(&view, [6; 16], &changed_spec).is_err());

        let mut unbounded = evaluation_spec();
        unbounded.maximum_case_results = 2;
        assert!(validate_evaluation_spec(&unbounded).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn request_metadata_marks_the_ephemeral_bearer_copy_sensitive() -> Result<(), Error> {
        let client = Inference(Arc::new(Connection {
            channel: Endpoint::from_static("https://localhost").connect_lazy(),
            authorization: authorization("secret")?,
            client_instance: [1; 16],
        }));
        let request = client.request(())?;
        let bearer = request
            .metadata()
            .get("authorization")
            .ok_or(Error::Invalid("missing authorization"))?;
        assert!(bearer.is_sensitive());
        assert_eq!(client.0.authorization.as_str(), "Bearer secret");
        Ok(())
    }

    #[tokio::test]
    async fn connection_rejects_insecure_or_unbounded_trust_configuration() {
        assert!(matches!(
            Inference::connect("http://localhost", "secret", b"certificate").await,
            Err(Error::Invalid("HTTPS is required"))
        ));
        assert!(matches!(
            Inference::connect("https://localhost", "secret", b"").await,
            Err(Error::Invalid(INVALID_CA_CERTIFICATE_LENGTH))
        ));
        let oversized = vec![b'x'; MAXIMUM_CA_CERTIFICATE_BYTES + 1];
        assert!(matches!(
            Inference::connect("https://localhost", "secret", &oversized).await,
            Err(Error::Invalid(INVALID_CA_CERTIFICATE_LENGTH))
        ));
    }

    #[tokio::test]
    async fn default_remote_transport_completes_authenticated_tls_grpc_handshake()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let certified = generate_simple_self_signed(["localhost".to_owned()])?;
        let certificate_pem = certified.cert.pem();
        let private_key_pem = certified.signing_key.serialize_pem();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server_certificate_pem = certificate_pem.clone();
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(
                    ServerTlsConfig::new()
                        .identity(Identity::from_pem(server_certificate_pem, private_key_pem)),
                )?
                .add_service(wire::models_service_server::ModelsServiceServer::new(
                    ModelsService,
                ))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let client =
            Inference::connect(&endpoint, "fixture-token", certificate_pem.as_bytes()).await?;
        assert_eq!(client.models().await?.len(), 1);

        let _ = shutdown_tx.send(());
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn remote_context_create_and_inspect_preserve_server_identities()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let certified = generate_simple_self_signed(["localhost".to_owned()])?;
        let certificate_pem = certified.cert.pem();
        let private_key_pem = certified.signing_key.serialize_pem();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server_certificate_pem = certificate_pem.clone();
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(
                    ServerTlsConfig::new()
                        .identity(Identity::from_pem(server_certificate_pem, private_key_pem)),
                )?
                .add_service(wire::contexts_service_server::ContextsServiceServer::new(
                    ContextsService,
                ))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let client =
            Inference::connect(&endpoint, "fixture-token", certificate_pem.as_bytes()).await?;
        let context = client
            .context("fixture-model")
            .instructions("bounded fixture input")
            .create()
            .await?;
        let view = context.inspect().await?;
        assert_eq!(view.revision, vec![1; 32]);
        assert_eq!(view.model, "fixture-model");
        assert_eq!(view.lineage, vec![3; 32]);

        let _ = shutdown_tx.send(());
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn remote_run_watch_cancel_and_resume_preserve_terminal_identity()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let certified = generate_simple_self_signed(["localhost".to_owned()])?;
        let certificate_pem = certified.cert.pem();
        let private_key_pem = certified.signing_key.serialize_pem();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server_certificate_pem = certificate_pem.clone();
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(
                    ServerTlsConfig::new()
                        .identity(Identity::from_pem(server_certificate_pem, private_key_pem)),
                )?
                .add_service(wire::runs_service_server::RunsServiceServer::new(
                    RunsService::default(),
                ))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let client =
            Inference::connect(&endpoint, "fixture-token", certificate_pem.as_bytes()).await?;
        let revision = [9; 32];
        let context = Context {
            client: client.clone(),
            revision,
        };
        let run = context
            .generate("bounded watch input", 64)
            .seed(7)
            .send()
            .await?;
        let mut events = run.watch(0).await?;
        assert_eq!(
            events.next().await?.and_then(|event| event.event.clone()),
            Some(wire::run_event::Event::Progress(wire::RunProgress {
                kind: "cancellation-requested".to_owned(),
            }))
        );
        assert_eq!(
            events.next().await?.and_then(|event| event.event.clone()),
            Some(wire::run_event::Event::Terminal(
                wire::RunTerminal::Cancelled.into(),
            ))
        );
        assert!(events.next().await?.is_none());

        let cancelled = run.cancel().await?;
        assert_eq!(cancelled.run_id, run.id().to_vec());
        assert!(cancelled.cancellation_requested);
        assert_eq!(
            wire::RunTerminal::try_from(
                cancelled
                    .result
                    .as_ref()
                    .ok_or("missing run result")?
                    .terminal,
            )?,
            wire::RunTerminal::Cancelled
        );
        let mut resumed = run.watch(2).await?;
        assert!(resumed.next().await?.is_none());

        let _ = shutdown_tx.send(());
        server.await??;
        Ok(())
    }

    #[test]
    fn customer_output_destructors_scrub_owned_buffers() -> Result<(), Error> {
        let mut event = wire::RunEvent {
            sequence: 0,
            event: Some(wire::run_event::Event::Output(vec![7; 32])),
        };
        scrub_run_event(&mut event);
        let Some(wire::run_event::Event::Output(bytes)) = event.event.as_ref() else {
            return Err(Error::Invalid("output event is absent"));
        };
        assert!(bytes.iter().all(|byte| *byte == 0));

        let mut result = wire::RunResult {
            output: vec![9; 32],
            context: None,
            terminal: wire::RunTerminal::Completed.into(),
            receipt: None,
        };
        scrub_run_result(&mut result);
        assert!(result.output.iter().all(|byte| *byte == 0));
        Ok(())
    }
}
