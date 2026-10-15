use super::{GuestEvent, GuestRequest, GuestResponse, retry_key};
use serde::{Deserialize, Serialize};
use std::{future::Future, pin::Pin};
use ts_rs::TS;

/// Artifact-defined codec identity. Versions identify byte formats, not code builds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodecIdentity {
    /// Stable artifact codec name.
    pub name: String,
    /// Artifact-defined format version.
    pub version: String,
}

/// Opaque artifact-owned bytes, tagged with their encoder identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Checkpoint {
    /// Encoder identity.
    pub codec: CodecIdentity,
    /// Encoded logical state. No runtime heap format is implied.
    pub bytes: Vec<u8>,
}

/// Authoring and admission failures.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS, thiserror::Error)]
#[serde(tag = "type", content = "detail", rename_all = "camelCase")]
pub enum GuestError {
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
    #[ts(type = "(state: State) => number[]")]
    pub encode: fn(&State) -> Result<Vec<u8>, GuestError>,
    /// Artifact decoder, including migration of readable older versions.
    #[ts(type = "(checkpoint: Checkpoint) => State")]
    pub decode: fn(&Checkpoint) -> Result<State, GuestError>,
}

/// One context for every lifecycle hook and event. Hosts inject scoped handles.
#[derive(TS)]
#[ts(bound = "Bindings: TS, State: TS", rename_all = "camelCase")]
pub struct GuestContext<Bindings, State> {
    /// Scoped binding handles, never deployment secrets in plain configuration.
    pub env: Bindings,
    /// Injectable Unix millisecond clock; shared with the local runtime.
    #[ts(type = "() => bigint")]
    pub clock: Box<dyn Fn() -> u64 + Send + Sync>,
    /// Stable across retries; null only for restore and quiesced checkpoint.
    pub delivery_id: Option<String>,
    /// Artifact-owned codec hooks.
    pub codec: CheckpointCodec<State>,
    /// Effect retry keys derived from the admitted delivery identity.
    #[ts(type = "(effect: string) => string")]
    pub retry_key: Box<dyn Fn(&str) -> Result<String, GuestError> + Send + Sync>,
}

impl<Bindings, State> GuestContext<Bindings, State> {
    /// Construct scoped hooks from the admitted delivery; absent delivery rejects effects.
    pub fn new(
        env: Bindings,
        clock: Box<dyn Fn() -> u64 + Send + Sync>,
        delivery_id: Option<String>,
        codec: CheckpointCodec<State>,
    ) -> Self {
        let admitted = delivery_id.clone();
        Self {
            env,
            clock,
            delivery_id,
            codec,
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
    /// Restore absent state or decode/migrate a retained checkpoint.
    #[ts(
        type = "(checkpoint: Checkpoint | null, context: GuestContext<Bindings, State>) => State | Promise<State>"
    )]
    pub restore: for<'a> fn(
        Option<&'a Checkpoint>,
        &'a GuestContext<Bindings, State>,
    ) -> GuestFuture<'a, State>,
    /// Handle a single delivery. Public Invoke may return opaque response bytes.
    #[ts(
        type = "(state: State, event: GuestEvent, context: GuestContext<Bindings, State>) => number[] | null | Promise<number[] | null>"
    )]
    pub event: for<'a> fn(
        &'a mut State,
        GuestEvent,
        &'a GuestContext<Bindings, State>,
    ) -> GuestFuture<'a, Option<Vec<u8>>>,
    /// Capture only after all admitted handlers and exclusive sections quiesce.
    #[ts(
        type = "(state: State, context: GuestContext<Bindings, State>) => Checkpoint | Promise<Checkpoint>"
    )]
    pub checkpoint:
        for<'a> fn(&'a State, &'a GuestContext<Bindings, State>) -> GuestFuture<'a, Checkpoint>,
    /// HTTP entry, using the fetch member of the single event union.
    #[ts(
        type = "(state: State, request: GuestRequest, context: GuestContext<Bindings, State>) => GuestResponse | Promise<GuestResponse>"
    )]
    pub fetch: for<'a> fn(
        &'a mut State,
        GuestRequest,
        &'a GuestContext<Bindings, State>,
    ) -> GuestFuture<'a, GuestResponse>,
}

/// Preserve the inferred actor type without introducing an execution runtime.
pub const fn define_actor<State, Bindings>(
    definition: ActorDefinition<State, Bindings>,
) -> ActorDefinition<State, Bindings> {
    definition
}
