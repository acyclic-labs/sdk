//! Guest ABI v1. Artifacts own their codec; hosts own admission and publication.

pub use acyclic_actors_guest_wire::{
    ActorMetadataWrite, ActorRuntime, ActorStateAlarm, ActorStateCommit, ActorStateSeed,
    AdmittedCaller, CallerCapability, Checkpoint, CheckpointCodec, CodecIdentity,
    GUEST_ABI_VERSION, GuestBody, GuestError, GuestEvent, GuestEventResult, GuestMetadataError,
    GuestRequest, GuestResponse, GuestSocketEvent, GuestStreamingRequest, GuestStreamingResponse,
    RuntimeHandle, SocketCloseInitiator, SocketCommand, SocketPayload, SocketReply,
    SocketTransport, WebSocketAutoResponse, WebSocketId, retry_key,
};
use ts_rs::{Config, TS};

mod activation;
pub use activation::GuestActivation;
mod durable;
pub use durable::*;
mod authoring;
pub use authoring::*;
mod update;
pub use update::*;
mod socket_host;
pub use socket_host::{decode_socket_reply, dispatch_socket_request, encode_socket_request};
mod body;
mod state;
pub use state::GuestState;
mod state_transaction;
pub use state_transaction::ActorStateHost;
pub(crate) use state_transaction::StateCommand;
mod handler;
mod hosted;
pub use handler::{
    AdmissionClass, AlarmChange, Composer, Composition, Delivery, Handler, Interrupt, Manifest,
    Participant, Subscription, SubscriptionChange, SubscriptionCuts, Transition, Trigger,
};
pub use hosted::HostedHandler;
/// Cooperative delivery interruption; handler cleanup and publication remain
/// subject to the original timeout and checkpoint fence.
pub use tokio_util::sync::CancellationToken;
pub mod wire;

/// Required named export and its lifecycle hooks; HTTP enters through default.fetch.
pub const ACTOR_EXPORT: &str = "actor";
/// Hooks on the actor export, in lifecycle order.
pub const ACTOR_HOOKS: &[&str] = &["restore", "event", "checkpoint"];
/// HTTP hook on the default export.
pub const FETCH_EXPORT: &str = "fetch";
/// Artifact version handshake export, checked before guest restore or delivery.
pub const ABI_VERSION_EXPORT: &str = "abiVersion";
/// Artifact codec export, inspected before attempting an update.
pub const CODEC_EXPORT: &str = "codec";

/// Admit only explicitly supported artifact ABI versions before touching its heap.
pub fn negotiate_abi(version: u32) -> Result<(), GuestError> {
    if version == GUEST_ABI_VERSION {
        Ok(())
    } else {
        Err(GuestError::UnsupportedAbi(version))
    }
}

/// Generate one self-contained authoring contract directly from Rust declarations.
pub fn typescript() -> String {
    let config = Config::default();
    let mut output = acyclic_actors_guest_wire::typescript(&config);
    macro_rules! declarations {
        ($($ty:ty),+ $(,)?) => {$(
            output.push_str("export ");
            output.push_str(&<$ty as TS>::decl(&config));
            output.push('\n');
        )+};
    }
    declarations!(
        GuestState<()>, acyclic_actors_guest_wire::GuestContext<(), ()>, ActorDefinition<(), ()>);
    output
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}
