use super::{
    Checkpoint, CheckpointCodec, GuestError, GuestEvent, GuestEventResult, GuestState,
    GuestStreamingRequest, GuestStreamingResponse,
};
use ts_rs::TS;

type EventHandler<State, Bindings> = for<'a> fn(
    &'a GuestState<State>,
    GuestEvent,
    &'a GuestContext<Bindings>,
) -> GuestFuture<'a, Option<GuestEventResult>>;

/// Lifecycle context with the admitted Actors socket host.
pub type GuestContext<Bindings> =
    acyclic_actors_guest_wire::GuestContext<Bindings, dyn super::SocketHost>;

/// Builder for the admitted lifecycle context.
pub type GuestContextBuilder<Bindings> =
    acyclic_actors_guest_wire::GuestContextBuilder<Bindings, dyn super::SocketHost>;

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
