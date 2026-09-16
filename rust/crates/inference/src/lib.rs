#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;
use tonic::Request;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use zeroize::{Zeroize, Zeroizing};

/// Generated customer contract and service traits, also used by deterministic mocks.
#[allow(missing_docs, unused_qualifications, clippy::all, clippy::pedantic)]
pub mod wire {
    tonic::include_proto!("inference.customer.v1");
}

/// Customer-only reflection; no backend descriptors or implementation are packaged.
pub const DESCRIPTOR: &[u8] = include_bytes!("../inference_descriptor.bin");
/// Bounded customer transport ceiling, not a published retention entitlement.
pub const MAXIMUM_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
/// Largest caller-supplied PEM trust bundle accepted by [`Inference::connect`].
pub const MAXIMUM_CA_CERTIFICATE_BYTES: usize = 64 * 1024;
/// Maximum immutable candidates admitted by one evaluation.
pub const MAXIMUM_EVALUATION_CANDIDATES: usize = 256;
/// Maximum ordered cases admitted by one evaluation suite.
pub const MAXIMUM_EVALUATION_CASES: usize = 4_096;
/// Maximum metric identities admitted by one evaluation.
pub const MAXIMUM_EVALUATION_METRICS: usize = 64;
/// Maximum materialized candidate/case results in one evaluation.
pub const MAXIMUM_EVALUATION_RESULTS: u64 = 65_536;
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
    /// Connect using an explicit trusted CA, ambient `WebPKI` roots, and bounded TLS/RPC deadlines.
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
}

impl Retention {
    /// Request an admitted warm promise through an exact published latency profile.
    #[must_use]
    pub const fn warm_until(latency_profile: [u8; 32], expires_at_ms: u64) -> Self {
        Self {
            latency_profile,
            expires_at_ms,
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
        if fixed::<32>(&view.revision)? != self.revision {
            return Err(Error::Invalid("revision differs"));
        }
        fixed::<32>(&view.lineage)?;
        fixed::<32>(&view.execution_profile)?;
        fixed::<32>(&view.content_digest)?;
        if let Some(parent) = &view.parent {
            fixed::<32>(parent)?;
        }
        if view.model.is_empty() || view.model.len() > 256 {
            return Err(Error::Invalid("Context model is invalid"));
        }
        validate_provenance(
            view.provenance
                .as_ref()
                .ok_or(Error::Invalid("Context provenance is absent"))?,
        )?;
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
                latency_profile: policy.latency_profile.to_vec(),
                expires_at_ms: policy.expires_at_ms,
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
        fixed::<32>(&self.request.latency_profile)?;
        if self.request.expires_at_ms == 0 {
            return Err(Error::Invalid("warm expiry is zero"));
        }
        let view = self
            .client
            .warm()
            .retain(self.client.request(self.request.clone())?)
            .await?
            .into_inner();
        validate_warm_view(&view, Some(expected_context), None)?;
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
    /// Extend and reconcile this exact commitment.
    ///
    /// # Errors
    /// Rejects malformed expiry, transport/service failure, or a foreign response.
    pub async fn send(&self) -> Result<wire::WarmView, Error> {
        bounded(&self.request)?;
        let commitment = fixed::<32>(&self.request.commitment)?;
        if self.request.expires_at_ms == 0 {
            return Err(Error::Invalid("warm expiry is zero"));
        }
        let view = self
            .client
            .warm()
            .renew(self.client.request(self.request.clone())?)
            .await?
            .into_inner();
        validate_warm_view(&view, None, Some(commitment))?;
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
        let run_id = self.id()?;
        let response = self
            .client
            .runs()
            .generate(self.client.request(self.request.clone())?)
            .await?
            .into_inner();
        let view = response.run.ok_or(Error::Invalid("missing Run response"))?;
        validate_run_view(&view, run_id)?;
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
        if view.result.is_some() {
            let end = view
                .last_sequence
                .checked_add(1)
                .ok_or(Error::Invalid("Run sequence exhausted"))?;
            if from_sequence > end {
                return Err(Error::Invalid("Run cursor exceeds retained events"));
            }
            if from_sequence == end {
                return Ok(RunEvents {
                    stream: None,
                    expected: from_sequence,
                    terminal: true,
                });
            }
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
            expected: from_sequence,
            terminal: false,
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
    expected: u64,
    terminal: bool,
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
            if self.terminal {
                return Ok(None);
            }
            return Err(Error::Invalid("Run stream ended before terminal"));
        };
        if self.terminal || event.sequence != self.expected || event.event.is_none() {
            return Err(Error::Invalid("Run event order or shape differs"));
        }
        self.expected = self
            .expected
            .checked_add(1)
            .ok_or(Error::Invalid("Run sequence exhausted"))?;
        if let Some(wire::run_event::Event::Terminal(value)) = event.event {
            if wire::RunTerminal::try_from(value).unwrap_or(wire::RunTerminal::Unspecified)
                == wire::RunTerminal::Unspecified
            {
                return Err(Error::Invalid("Run terminal is invalid"));
            }
            self.terminal = true;
        }
        Ok(Some(event))
    }
}

fn validate_evaluation_spec(spec: &wire::EvaluationSpec) -> Result<(), Error> {
    if spec.candidates.is_empty() || spec.candidates.len() > MAXIMUM_EVALUATION_CANDIDATES {
        return Err(Error::Invalid("evaluation candidate count is invalid"));
    }
    let mut candidate_digests = std::collections::BTreeSet::new();
    for candidate in &spec.candidates {
        let digest = fixed::<32>(&candidate.digest)?;
        if candidate.media_type.is_empty()
            || candidate.media_type.len() > 256
            || candidate.logical_size == 0
            || !candidate_digests.insert(digest)
        {
            return Err(Error::Invalid("evaluation candidate is invalid"));
        }
    }

    let suite = spec
        .suite
        .as_ref()
        .ok_or(Error::Invalid("evaluation suite is absent"))?;
    if suite.identity.is_empty()
        || suite.identity.len() > 256
        || suite.cases.is_empty()
        || suite.cases.len() > MAXIMUM_EVALUATION_CASES
    {
        return Err(Error::Invalid("evaluation suite is invalid"));
    }
    fixed::<32>(&suite.digest)?;
    let mut case_ids = std::collections::BTreeSet::new();
    for case in &suite.cases {
        let case_id = fixed::<16>(&case.case_id)?;
        let inline = !case.input.is_empty();
        let artifact = case.input_artifact_digest.as_ref();
        if !case_ids.insert(case_id) || inline == artifact.is_some() {
            return Err(Error::Invalid("evaluation case is invalid"));
        }
        if let Some(digest) = artifact {
            fixed::<32>(digest)?;
        }
    }

    let grader = spec
        .grader
        .as_ref()
        .ok_or(Error::Invalid("evaluation grader is absent"))?;
    if grader.handle.is_empty() || grader.handle.len() > 4_096 {
        return Err(Error::Invalid("evaluation grader is invalid"));
    }
    fixed::<32>(&grader.artifact_digest)?;

    if spec.metrics.is_empty() || spec.metrics.len() > MAXIMUM_EVALUATION_METRICS {
        return Err(Error::Invalid("evaluation metric count is invalid"));
    }
    let mut metric_ids = std::collections::BTreeSet::new();
    for metric in &spec.metrics {
        if metric.identity.is_empty()
            || metric.identity.len() > 256
            || !metric_ids.insert(metric.identity.as_str())
            || wire::EvaluationAggregation::try_from(metric.aggregation)
                .unwrap_or(wire::EvaluationAggregation::Unspecified)
                == wire::EvaluationAggregation::Unspecified
        {
            return Err(Error::Invalid("evaluation metric is invalid"));
        }
    }

    let possible_results = u64::try_from(spec.candidates.len())
        .ok()
        .and_then(|candidates| {
            u64::try_from(suite.cases.len())
                .ok()
                .and_then(|cases| candidates.checked_mul(cases))
        })
        .ok_or(Error::Invalid("evaluation result count overflow"))?;
    if spec.maximum_case_results == 0
        || spec.maximum_case_results > possible_results
        || spec.maximum_case_results > MAXIMUM_EVALUATION_RESULTS
    {
        return Err(Error::Invalid("evaluation result bound is invalid"));
    }
    fixed::<32>(&spec.spec_digest)?;
    Ok(())
}

fn validate_exact_rational(value: Option<&wire::ExactRational>) -> Result<(), Error> {
    if value.is_none_or(|value| value.denominator == 0) {
        return Err(Error::Invalid("evaluation rational is invalid"));
    }
    Ok(())
}

fn evaluation_observation_binding(native: &[u8; 32], observation: &[u8; 32]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"acyclic.inference.grader-observation.v1\0");
    digest.update(native);
    digest.update(observation);
    digest.finalize().into()
}

fn validate_evaluation_admission(
    view: &wire::EvaluationView,
    expected: [u8; 16],
    expected_spec: &wire::EvaluationSpec,
) -> Result<(), Error> {
    if view.spec.as_ref() != Some(expected_spec) {
        return Err(Error::Invalid("evaluation admission spec differs"));
    }
    validate_evaluation_view(view, expected)
}

fn validate_evaluation_view(view: &wire::EvaluationView, expected: [u8; 16]) -> Result<(), Error> {
    if fixed::<16>(&view.evaluation_id)? != expected || view.sequence == 0 {
        return Err(Error::Invalid("evaluation identity differs"));
    }
    let spec = view
        .spec
        .as_ref()
        .ok_or(Error::Invalid("evaluation spec is absent"))?;
    validate_evaluation_spec(spec)?;
    let state =
        wire::EvaluationState::try_from(view.state).unwrap_or(wire::EvaluationState::Unspecified);
    if state == wire::EvaluationState::Unspecified
        || (state == wire::EvaluationState::Completed) != view.result.is_some()
    {
        return Err(Error::Invalid("evaluation state is invalid"));
    }
    let Some(result) = &view.result else {
        return Ok(());
    };
    validate_evaluation_result(spec, result)
}

fn validate_evaluation_result(
    spec: &wire::EvaluationSpec,
    result: &wire::EvaluationResult,
) -> Result<(), Error> {
    if fixed::<32>(&result.spec_digest)? != fixed::<32>(&spec.spec_digest)?
        || result.case_results.is_empty()
        || u64::try_from(result.case_results.len()).unwrap_or(u64::MAX) != spec.maximum_case_results
    {
        return Err(Error::Invalid("evaluation result is invalid"));
    }
    fixed::<32>(&result.result_digest)?;

    let suite = spec
        .suite
        .as_ref()
        .ok_or(Error::Invalid("evaluation suite is absent"))?;
    let candidates: std::collections::BTreeSet<_> = spec
        .candidates
        .iter()
        .map(|candidate| candidate.digest.as_slice())
        .collect();
    let cases: std::collections::BTreeSet<_> = suite
        .cases
        .iter()
        .map(|case| case.case_id.as_slice())
        .collect();
    let metrics: std::collections::BTreeSet<_> = spec
        .metrics
        .iter()
        .map(|metric| metric.identity.as_str())
        .collect();
    let mut observations = std::collections::BTreeSet::new();
    for case in &result.case_results {
        validate_evaluation_case(case, &candidates, &cases, &metrics, &mut observations)?;
    }
    validate_evaluation_aggregates(spec, result, &candidates, &metrics)
}

fn validate_evaluation_case<'a>(
    case: &'a wire::EvaluationCaseResult,
    candidates: &std::collections::BTreeSet<&[u8]>,
    cases: &std::collections::BTreeSet<&[u8]>,
    metrics: &std::collections::BTreeSet<&str>,
    observations: &mut std::collections::BTreeSet<(&'a [u8], &'a [u8])>,
) -> Result<(), Error> {
    let observation = case
        .observation
        .as_ref()
        .ok_or(Error::Invalid("evaluation grader observation is absent"))?;
    let native_output_digest = fixed::<32>(&observation.native_output_digest)?;
    let grader_observation_digest = fixed::<32>(&observation.observation_digest)?;
    if fixed::<32>(&observation.binding_digest)?
        != evaluation_observation_binding(&native_output_digest, &grader_observation_digest)
    {
        return Err(Error::Invalid(
            "evaluation grader observation binding differs",
        ));
    }
    let outcome = wire::EvaluationCaseOutcome::try_from(case.outcome)
        .unwrap_or(wire::EvaluationCaseOutcome::Unspecified);
    if !candidates.contains(case.candidate_digest.as_slice())
        || !cases.contains(case.case_id.as_slice())
        || !observations.insert((case.candidate_digest.as_slice(), case.case_id.as_slice()))
        || outcome == wire::EvaluationCaseOutcome::Unspecified
    {
        return Err(Error::Invalid("evaluation case result is invalid"));
    }
    let mut observed_metrics = std::collections::BTreeSet::new();
    for metric in &case.metrics {
        if !metrics.contains(metric.metric_identity.as_str())
            || !observed_metrics.insert(metric.metric_identity.as_str())
        {
            return Err(Error::Invalid("evaluation case metric is invalid"));
        }
        validate_exact_rational(metric.value.as_ref())?;
    }
    if (outcome == wire::EvaluationCaseOutcome::Scored && observed_metrics.len() != metrics.len())
        || (outcome != wire::EvaluationCaseOutcome::Scored && !observed_metrics.is_empty())
    {
        return Err(Error::Invalid("evaluation case metric coverage is invalid"));
    }
    Ok(())
}

fn validate_evaluation_aggregates(
    spec: &wire::EvaluationSpec,
    result: &wire::EvaluationResult,
    candidates: &std::collections::BTreeSet<&[u8]>,
    metrics: &std::collections::BTreeSet<&str>,
) -> Result<(), Error> {
    let mut aggregates = std::collections::BTreeSet::new();
    for aggregate in &result.aggregates {
        let Some(metric) = spec
            .metrics
            .iter()
            .find(|metric| metric.identity == aggregate.metric_identity)
        else {
            return Err(Error::Invalid("evaluation aggregate metric is invalid"));
        };
        if !candidates.contains(aggregate.candidate_digest.as_slice())
            || metric.aggregation != aggregate.aggregation
            || !aggregates.insert((
                aggregate.candidate_digest.as_slice(),
                aggregate.metric_identity.as_str(),
            ))
        {
            return Err(Error::Invalid("evaluation aggregate is invalid"));
        }
        validate_exact_rational(aggregate.value.as_ref())?;
    }
    if aggregates.len() != candidates.len().saturating_mul(metrics.len()) {
        return Err(Error::Invalid("evaluation aggregate coverage is invalid"));
    }
    Ok(())
}

fn validate_run_view(view: &wire::RunView, expected: [u8; 16]) -> Result<(), Error> {
    if fixed::<16>(&view.run_id)? != expected || fixed::<32>(&view.input).is_err() {
        return Err(Error::Invalid("Run identity differs"));
    }
    if view.model.is_empty() || view.model.len() > 256 {
        return Err(Error::Invalid("Run model is invalid"));
    }
    if let Some(result) = &view.result {
        let terminal =
            wire::RunTerminal::try_from(result.terminal).unwrap_or(wire::RunTerminal::Unspecified);
        if terminal == wire::RunTerminal::Unspecified {
            return Err(Error::Invalid("Run result terminal is invalid"));
        }
        if let Some(receipt) = &result.receipt {
            fixed::<32>(&receipt.receipt_id)?;
            fixed::<32>(&receipt.model_profile)?;
            fixed::<32>(&receipt.meter_revision)?;
            fixed::<32>(&receipt.rate_card_revision)?;
            if receipt.usage.is_none() {
                return Err(Error::Invalid("Run usage is absent"));
            }
        }
    }
    Ok(())
}

fn validate_warm_view(
    view: &wire::WarmView,
    expected_context: Option<[u8; 32]>,
    expected_commitment: Option<[u8; 32]>,
) -> Result<(), Error> {
    let commitment = fixed::<32>(&view.commitment)?;
    let context = fixed::<32>(&view.context)?;
    fixed::<32>(&view.model_profile)?;
    fixed::<32>(&view.latency_profile)?;
    fixed::<32>(&view.evidence_digest)?;
    fixed::<32>(&view.admission_receipt_id)?;
    let state = wire::WarmState::try_from(view.state).unwrap_or(wire::WarmState::Unspecified);
    if expected_context.is_some_and(|expected| expected != context)
        || expected_commitment.is_some_and(|expected| expected != commitment)
        || view.expires_at_ms == 0
        || view.sequence == 0
        || state == wire::WarmState::Unspecified
    {
        return Err(Error::Invalid("warm commitment shape differs"));
    }
    Ok(())
}

fn validate_provenance(value: &wire::ContextProvenance) -> Result<(), Error> {
    use wire::context_provenance::Origin;
    match value
        .origin
        .as_ref()
        .ok_or(Error::Invalid("Context provenance is absent"))?
    {
        Origin::Created(_) => Ok(()),
        Origin::Derived(value) | Origin::Forked(value) => fixed::<32>(&value.source).map(|_| ()),
        Origin::Transferred(value) => fixed::<32>(&value.source).map(|_| ()),
        Origin::Generated(value) => {
            fixed::<16>(&value.run_id)?;
            fixed::<32>(&value.terminal_receipt_digest).map(|_| ())
        }
        Origin::RunInput(value) => {
            fixed::<32>(&value.source)?;
            fixed::<16>(&value.run_id)?;
            if value.maximum_output == 0 {
                return Err(Error::Invalid("Run input output bound is zero"));
            }
            Ok(())
        }
    }
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
    fixed::<32>(&receipt.revision)?;
    fixed::<32>(&receipt.command_digest)?;
    if receipt.sequence == 0 {
        return Err(Error::Invalid("missing publication sequence"));
    }
    Ok(())
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
                        binding_digest: evaluation_observation_binding(
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
    #[allow(
        clippy::indexing_slicing,
        reason = "each index is preceded by an assert_eq! on the corresponding Vec's len(), so the index is proven in-bounds"
    )]
    fn descriptor_contains_only_customer_contract() -> Result<(), Box<dyn std::error::Error>> {
        let descriptor = prost_types::FileDescriptorSet::decode(DESCRIPTOR)?;
        assert_eq!(descriptor.file.len(), 1);
        let file = &descriptor.file[0];
        assert_eq!(file.package.as_deref(), Some("inference.customer.v1"));
        assert!(file.dependency.is_empty());
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
