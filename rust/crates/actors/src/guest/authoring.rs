use super::{
    ActorRuntime, AdmittedCaller, Checkpoint, CheckpointCodec, GuestError, GuestEvent,
    GuestEventResult, GuestState, GuestStreamingRequest, GuestStreamingResponse, retry_key,
};
use ts_rs::TS;

type EventHandler<State, Bindings> = for<'a> fn(
    &'a GuestState<State>,
    GuestEvent,
    &'a GuestContext<Bindings>,
) -> GuestFuture<'a, Option<GuestEventResult>>;

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

/// Preserve the inferred actor type without introducing an execution runtime.
pub const fn define_actor<State, Bindings>(
    definition: ActorDefinition<State, Bindings>,
) -> ActorDefinition<State, Bindings> {
    definition
}
