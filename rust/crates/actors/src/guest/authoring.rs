use super::{
    ActorRuntime, AdmittedCaller, GuestEvent, GuestEventResult, GuestState, GuestStreamingRequest,
    GuestStreamingResponse, retry_key,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{future::Future, pin::Pin};
use ts_rs::TS;

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
    /// Host does not support the artifact-declared ABI.
    #[error("unsupported guest ABI: {0}")]
    UnsupportedAbi(u32),
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
    /// Guest handler failed; the host must discard its heap.
    #[error("guest handler failed: {0}")]
    Handler(String),
}

/// Artifact codec hooks. Decode must accept every identity listed in readable.
#[derive(TS)]
#[ts(bound = "State: TS")]
pub struct CheckpointCodec<State> {
    /// Identity written on the next checkpoint.
    pub identity: CodecIdentity,
    /// Explicit decoder compatibility, including identity itself.
    pub readable: Vec<CodecIdentity>,
    /// Artifact encoder.
    #[ts(type = "(state: State) => Uint8Array")]
    pub encode: fn(&State) -> Result<Bytes, GuestError>,
    /// Artifact decoder, including migration of readable older versions.
    #[ts(type = "(checkpoint: Checkpoint) => State")]
    pub decode: fn(&Checkpoint) -> Result<State, GuestError>,
}

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
    /// Effect retry keys derived from the admitted delivery identity.
    #[ts(type = "(effect: string) => string")]
    pub retry_key: Box<dyn Fn(&str) -> Result<String, GuestError> + Send + Sync>,
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
    /// Finish context construction and bind effect keys to the admitted delivery.
    pub fn build(self) -> GuestContext<Bindings> {
        let admitted = self.delivery_id.clone();
        GuestContext {
            env: self.env,
            clock: self.clock,
            delivery_id: self.delivery_id,
            admitted_identity: self.admitted_identity,
            actor: self.actor,
            retry_key: Box::new(move |effect| {
                retry_key(admitted.as_deref().unwrap_or_default(), effect)
            }),
        }
    }
}

/// Async guest hook result; hosts decide execution, fencing and cancellation.
pub type GuestFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, GuestError>> + Send + 'a>>;

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
    pub event: for<'a> fn(
        &'a GuestState<State>,
        GuestEvent,
        &'a GuestContext<Bindings>,
    ) -> GuestFuture<'a, Option<GuestEventResult>>,
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
        type = "(state: GuestState<State>, request: Request, context: GuestContext<Bindings>) => Response | Promise<Response>"
    )]
    pub fetch: for<'a> fn(
        &'a GuestState<State>,
        GuestStreamingRequest,
        &'a GuestContext<Bindings>,
    ) -> GuestFuture<'a, GuestStreamingResponse>,
}

/// Preserve the inferred actor type without introducing an execution runtime.
pub const fn define_actor<State, Bindings>(
    definition: ActorDefinition<State, Bindings>,
) -> ActorDefinition<State, Bindings> {
    definition
}
