use super::*;

/// Address accepted by the common WebSocket connect API.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = ActorAddress::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/ActorAddress.ts", rename_all = "camelCase")]
pub struct ActorAddress {
    #[wire(tag = 1, string)]
    actor_id: ActorId,
    #[wire(tag = 2, string)]
    #[ts(optional)]
    app_channel: Option<String>,
}
impl ActorAddress {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            self.app_channel
                .as_ref()
                .is_none_or(|channel| valid_socket_id(channel)),
        )
    }
    /// Target actor identity.
    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }
    /// Optional application channel.
    pub fn app_channel(&self) -> Option<&str> {
        self.app_channel.as_deref()
    }
}
/// Diagnostic transport chosen by the runtime, not by callers.
#[acyclic_contract_derive::enumeration(error = DomainError, unknown = DomainError::UnknownSocketEnum)]
#[derive(TS)]
#[ts(export_to = "actors/SocketTransport.ts")]
#[ts(repr(enum))]
pub enum SocketTransport {
    /// No transport was specified.
    Unspecified = 0,
    /// Multiplexed internal channel, with no per-actor physical socket.
    Internal = 1,
    /// Runtime or edge-held third-party socket.
    External = 2,
}
