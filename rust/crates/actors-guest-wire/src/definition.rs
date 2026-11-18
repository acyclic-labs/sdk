//! Portable actor definitions used by runtime hosts and schema generation.
use crate::{
    Checkpoint, CheckpointCodec, GuestContext, GuestError, GuestEvent, GuestEventResult,
    GuestStreamingRequest, GuestStreamingResponse,
};
use std::{future::Future, pin::Pin};
use ts_rs::TS;

/// Async guest hook result dispatched by the platform executor.
#[cfg(not(target_arch = "wasm32"))]
pub type GuestFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, GuestError>> + Send + 'a>>;
/// Async guest hook result retaining browser event-loop ownership.
#[cfg(target_arch = "wasm32")]
pub type GuestFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, GuestError>> + 'a>>;

type EventHandler<Handle, Bindings, Socket> =
    for<'a> fn(
        &'a Handle,
        GuestEvent,
        &'a GuestContext<Bindings, Socket>,
    ) -> GuestFuture<'a, Option<GuestEventResult>>;

/// Typed actor authoring surface. Export as actor; adapt fetch as default.fetch.
#[derive(TS)]
#[ts(bound = "State: TS, Bindings: TS", concrete(Handle = (), Socket = ()))]
pub struct ActorDefinition<State, Bindings, Handle, Socket: ?Sized> {
    /// Explicit artifact handshake; hosts reject unsupported versions before restore.
    #[ts(rename = "abiVersion")]
    pub abi_version: u32,
    /// Artifact-owned codec exported as actor.codec, discoverable before update admission.
    pub codec: CheckpointCodec<State>,
    /// Restore absent state or decode/migrate a retained checkpoint.
    #[ts(
        type = "(checkpoint: Checkpoint | null, context: GuestContext<Bindings>) => State | Promise<State>"
    )]
    pub restore: for<'a> fn(
        Option<&'a Checkpoint>,
        &'a GuestContext<Bindings, Socket>,
    ) -> GuestFuture<'a, State>,
    /// Handle a single delivery. Public Invoke may return opaque response bytes.
    #[ts(
        type = "(state: GuestState<State>, event: GuestEvent, context: GuestContext<Bindings>) => GuestEventResult | null | Promise<GuestEventResult | null>"
    )]
    pub event: EventHandler<Handle, Bindings, Socket>,
    /// Capture only after all admitted handlers and exclusive sections quiesce.
    #[ts(
        type = "(state: GuestState<State>, context: GuestContext<Bindings>) => Checkpoint | Promise<Checkpoint>"
    )]
    pub checkpoint:
        for<'a> fn(&'a Handle, &'a GuestContext<Bindings, Socket>) -> GuestFuture<'a, Checkpoint>,
    /// HTTP entry, using the fetch member of the single event union.
    #[ts(
        type = "(state: GuestState<State>, request: Request, context: GuestContext<Bindings>) => GuestFetchResponse | Promise<GuestFetchResponse>"
    )]
    pub fetch: for<'a> fn(
        &'a Handle,
        GuestStreamingRequest,
        &'a GuestContext<Bindings, Socket>,
    ) -> GuestFuture<'a, GuestStreamingResponse>,
}
