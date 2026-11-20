use super::{
    ActorRuntime, AdmittedCaller, GuestEvent, GuestEventResult, GuestState, GuestStreamingRequest,
    GuestStreamingResponse, retry_key,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

type EventHandler<State, Bindings> = for<'a> fn(
    &'a GuestState<State>,
    GuestEvent,
    &'a GuestContext<Bindings>,
) -> GuestFuture<'a, Option<GuestEventResult>>;

/// Artifact-defined codec identity. Versions identify byte formats, not code builds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CodecIdentity {
    /// Stable artifact codec name.
    pub name: String,
    /// Artifact-defined format version.
    pub version: String,
}

/// Opaque artifact-owned bytes, tagged with their encoder identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Checkpoint {
    /// Encoder identity.
    pub codec: CodecIdentity,
    /// Encoded logical state. No runtime heap format is implied.
    #[ts(type = "Uint8Array")]
    pub bytes: Bytes,
}

/// Authoring and admission failures.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS, thiserror::Error)]
#[serde(tag = "type", content = "detail", rename_all = "camelCase")]
pub enum GuestError {
    /// A host compositor observed a changed external guard before submission.
    /// No part of its candidate committed; the original delivery is revalidated.
    #[error("composed transaction precondition changed")]
    PreconditionChanged,
    /// Host does not support the artifact-declared ABI.
    #[error("unsupported guest ABI: {0}")]
    UnsupportedAbi(u32),
    /// A recognized guest message carries an absent, old or unknown format version.
    #[error("unsupported guest message format")]
    UnsupportedFormat,
    /// Nested synchronous mutations violate the documented no-nesting lock order.
    #[error("nested guest state mutation")]
    NestedMutation,
    /// Retry keys require a delivery ID and an effect name.
    #[error("missing delivery or effect identity")]
    MissingIdentity,
    /// The candidate does not admit the retained codec.
    #[error("incompatible checkpoint codec")]
    IncompatibleCodec,
    /// Artifact decoder rejected the retained bytes.
    #[error("checkpoint decode failed: {0}")]
    Decode(String),
    /// Artifact encoder failed.
    #[error("checkpoint encode failed: {0}")]
    Encode(String),
    /// Guest handler failed definitively; the host must discard its heap.
    #[error("guest handler failed: {0}")]
    Handler(String),
    /// External work is undecided. Retain its delivery without a failure
    /// transition that could acknowledge or replace the original operation.
    #[error("guest handler pending: {0}")]
    Pending(String),
    /// A capability call named a request whose handler invocation ended:
    /// it returned, timed out, was cancelled or lost its placement epoch.
    #[error("host request is revoked")]
    Revoked,
    /// A manifest binding or host endpoint the handler required failed or is
    /// absent. Definitive for this delivery; the name is never prose.
    #[error("guest dependency {dependency} is unavailable")]
    DependencyUnavailable {
        /// Manifest binding or host endpoint name, in the environment name grammar.
        #[serde(deserialize_with = "dependency_name")]
        dependency: String,
    },
    /// Original local cause, independent of the guest's wire classification.
    #[serde(untagged, skip_deserializing)]
    #[ts(skip)]
    #[error("{}", .0.classification())]
    Caused(
        #[source]
        #[serde(serialize_with = "serialize_guest_cause")]
        acyclic_auth::failure::Failure<Self>,
    ),
}

fn dependency_name<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let name = String::deserialize(deserializer)?;
    if crate::domain::valid_environment_name(&name) {
        Ok(name)
    } else {
        Err(serde::de::Error::custom(
            "guest dependency is not a binding name",
        ))
    }
}

fn serialize_guest_cause<S: serde::Serializer>(
    failure: &acyclic_auth::failure::Failure<GuestError>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    failure.classification().serialize(serializer)
}

impl GuestError {
    /// A required binding or host endpoint failed; `None` unless `dependency`
    /// is a manifest binding or endpoint name, so it can never carry prose.
    #[must_use]
    pub fn dependency_unavailable(dependency: &str) -> Option<Self> {
        crate::domain::valid_environment_name(dependency).then(|| Self::DependencyUnavailable {
            dependency: dependency.to_owned(),
        })
    }

    /// Stable, path-free snake_case name of the variant; a retained local
    /// cause reports its classification's name. Exhaustive, so each new
    /// variant is named where it is declared.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Caused(failure) => failure.classification().kind(),
            Self::PreconditionChanged => "precondition_changed",
            Self::UnsupportedAbi(_) => "unsupported_abi",
            Self::UnsupportedFormat => "unsupported_format",
            Self::NestedMutation => "nested_mutation",
            Self::MissingIdentity => "missing_identity",
            Self::IncompatibleCodec => "incompatible_codec",
            Self::Decode(_) => "decode",
            Self::Encode(_) => "encode",
            Self::Handler(_) => "handler",
            Self::Pending(_) => "pending",
            Self::Revoked => "revoked",
            Self::DependencyUnavailable { .. } => "dependency_unavailable",
        }
    }

    /// Keep the original local failure without changing the wire refusal.
    #[must_use]
    pub fn with_source(self, source: impl acyclic_auth::failure::Source) -> Self {
        Self::Caused(acyclic_auth::failure::Failure::new(self, source))
    }

    /// The original guest refusal, independently of local diagnostic custody.
    #[must_use]
    pub fn category(&self) -> &Self {
        match self {
            Self::Caused(failure) => failure.classification().category(),
            category => category,
        }
    }

    /// Whether the same retained input may become decidable on another attempt.
    #[must_use]
    pub fn retryable(&self) -> bool {
        match self {
            Self::Caused(failure) => failure.classification().retryable(),
            Self::Pending(_) | Self::PreconditionChanged => true,
            Self::UnsupportedAbi(_)
            | Self::UnsupportedFormat
            | Self::NestedMutation
            | Self::MissingIdentity
            | Self::IncompatibleCodec
            | Self::Decode(_)
            | Self::Encode(_)
            | Self::Handler(_)
            | Self::Revoked
            | Self::DependencyUnavailable { .. } => false,
        }
    }

    /// Adds operation context without changing pending versus definitive failure.
    /// Structured guest refusals retain their original identity.
    #[must_use]
    pub fn with_context(self, context: &str) -> Self {
        match self {
            Self::Caused(failure) => {
                Self::Caused(failure.map_classification(|category| category.with_context(context)))
            }
            Self::Pending(message) => Self::Pending(format!("{context}: {message}")),
            Self::Handler(message) => Self::Handler(format!("{context}: {message}")),
            other => other,
        }
    }
}
/// Artifact codec hooks for exactly one checkpoint byte format.
#[derive(TS)]
#[ts(bound = "State: TS")]
pub struct CheckpointCodec<State> {
    /// Identity written on the next checkpoint.
    pub identity: CodecIdentity,
    /// Artifact encoder.
    #[ts(type = "(state: State) => Uint8Array")]
    pub encode: fn(&State) -> Result<Bytes, GuestError>,
    /// Artifact decoder, called only after its exact codec identity is admitted.
    #[ts(type = "(checkpoint: Checkpoint) => State")]
    pub decode: fn(&Checkpoint) -> Result<State, GuestError>,
}

type RetryKey = Box<dyn Fn(&str) -> Result<String, GuestError> + Send + Sync>;

/// One context for every lifecycle hook and event. Hosts inject scoped handles.
#[derive(TS)]
#[ts(bound = "Bindings: TS", rename_all = "camelCase")]
#[non_exhaustive]
pub struct GuestContext<Bindings> {
    /// Scoped binding handles, never deployment secrets in plain configuration.
    pub env: Bindings,
    /// Injectable Unix millisecond clock; shared with the local runtime.
    #[ts(type = "() => bigint")]
    pub clock: Box<dyn Fn() -> u64 + Send + Sync>,
    /// Stable across retries; null only for restore and quiesced checkpoint.
    pub delivery_id: Option<String>,
    /// Host-admitted caller, null for Stream/timer/restore/checkpoint deliveries.
    pub admitted_identity: Option<AdmittedCaller>,
    /// Reserved runtime namespace, independent of manifest bindings.
    pub actor: ActorRuntime,
    /// Delivery-scoped socket owner. The JavaScript host port uses the SDK's
    /// binary request/reply codecs; it carries no actor-selected authority.
    #[ts(
        optional,
        type = "(request: Uint8Array, signal?: AbortSignal) => Promise<Uint8Array>"
    )]
    pub socket_host: Option<std::sync::Arc<dyn super::SocketHost>>,
    /// Effect retry keys derived from the admitted delivery identity.
    #[ts(type = "(effect: string) => string")]
    pub retry_key: RetryKey,
}

impl<Bindings> GuestContext<Bindings> {
    /// Start host context construction; later fields are optional builder extensions.
    pub fn builder(
        env: Bindings,
        clock: Box<dyn Fn() -> u64 + Send + Sync>,
    ) -> GuestContextBuilder<Bindings> {
        GuestContextBuilder {
            env,
            clock,
            delivery_id: None,
            admitted_identity: None,
            actor: ActorRuntime::default(),
            socket_host: None,
        }
    }
}

/// Host context builder with optional, additive runtime and caller metadata.
pub struct GuestContextBuilder<Bindings> {
    env: Bindings,
    clock: Box<dyn Fn() -> u64 + Send + Sync>,
    delivery_id: Option<String>,
    admitted_identity: Option<AdmittedCaller>,
    actor: ActorRuntime,
    socket_host: Option<std::sync::Arc<dyn super::SocketHost>>,
}

impl<Bindings> GuestContextBuilder<Bindings> {
    /// Supply the host's stable delivery identity.
    pub fn delivery(mut self, identity: String) -> Self {
        self.delivery_id = Some(identity);
        self
    }
    /// Supply identity only after host authority admission; never from public headers.
    pub fn admitted_identity(mut self, identity: AdmittedCaller) -> Self {
        self.admitted_identity = Some(identity);
        self
    }
    /// Supply runtime handles without adding manifest env properties.
    pub fn actor(mut self, actor: ActorRuntime) -> Self {
        self.actor = actor;
        self
    }
    /// Supply the socket host scoped to this admitted actor's delivery.
    pub fn socket_host(mut self, host: std::sync::Arc<dyn super::SocketHost>) -> Self {
        self.socket_host = Some(host);
        self
    }
    /// Finish context construction and bind effect keys to the admitted delivery.
    pub fn build(self) -> GuestContext<Bindings> {
        let admitted = self.delivery_id.clone();
        GuestContext {
            env: self.env,
            clock: self.clock,
            delivery_id: self.delivery_id,
            admitted_identity: self.admitted_identity,
            actor: self.actor,
            socket_host: self.socket_host,
            retry_key: Box::new(move |effect| {
                retry_key(admitted.as_deref().unwrap_or_default(), effect)
            }),
        }
    }
}

/// Async guest hook result; hosts decide execution, fencing and cancellation.
///
/// `Send` on native hosts; a browser host keeps it on its event loop.
pub type GuestFuture<'a, T> = acyclic_stream::BoxProviderFuture<'a, Result<T, GuestError>>;

/// Typed actor authoring surface. Export as actor; adapt fetch as default.fetch.
#[derive(TS)]
#[ts(bound = "State: TS, Bindings: TS")]
pub struct ActorDefinition<State, Bindings> {
    /// Explicit artifact handshake; hosts reject unsupported versions before restore.
    #[ts(rename = "abiVersion")]
    pub abi_version: u32,
    /// Artifact-owned codec exported as actor.codec, discoverable before update admission.
    pub codec: CheckpointCodec<State>,
    /// Restore absent state or decode/migrate a retained checkpoint.
    #[ts(
        type = "(checkpoint: Checkpoint | null, context: GuestContext<Bindings>) => State | Promise<State>"
    )]
    pub restore:
        for<'a> fn(Option<&'a Checkpoint>, &'a GuestContext<Bindings>) -> GuestFuture<'a, State>,
    /// Handle a single delivery. Public Invoke may return opaque response bytes.
    #[ts(
        type = "(state: GuestState<State>, event: GuestEvent, context: GuestContext<Bindings>) => GuestEventResult | null | Promise<GuestEventResult | null>"
    )]
    pub event: EventHandler<State, Bindings>,
    /// Capture only after all admitted handlers and exclusive sections quiesce.
    #[ts(
        type = "(state: GuestState<State>, context: GuestContext<Bindings>) => Checkpoint | Promise<Checkpoint>"
    )]
    pub checkpoint: for<'a> fn(
        &'a GuestState<State>,
        &'a GuestContext<Bindings>,
    ) -> GuestFuture<'a, Checkpoint>,
    /// HTTP entry, using the fetch member of the single event union.
    #[ts(
        type = "(state: GuestState<State>, request: Request, context: GuestContext<Bindings>) => GuestFetchResponse | Promise<GuestFetchResponse>"
    )]
    pub fetch: for<'a> fn(
        &'a GuestState<State>,
        GuestStreamingRequest,
        &'a GuestContext<Bindings>,
    ) -> GuestFuture<'a, GuestStreamingResponse>,
}
