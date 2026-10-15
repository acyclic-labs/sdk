use super::*;

/// SocketReconnectMode supported by the Actors runtime.
#[acyclic_contract_derive::enumeration(error = DomainError, unknown = DomainError::UnknownSocketEnum)]
#[derive(TS)]
#[ts(export_to = "actors/SocketReconnectMode.ts")]
#[ts(repr(enum))]
pub enum SocketReconnectMode {
    /// Unspecified.
    Unspecified = 0,
    /// None.
    None = 1,
    /// Backoff.
    Backoff = 2,
}

/// SocketCloseInitiator supported by the Actors runtime.
#[acyclic_contract_derive::enumeration(error = DomainError, unknown = DomainError::UnknownSocketEnum)]
#[derive(TS)]
#[ts(export_to = "actors/SocketCloseInitiator.ts")]
#[ts(repr(enum))]
pub enum SocketCloseInitiator {
    /// Unspecified.
    Unspecified = 0,
    /// Peer.
    Peer = 1,
    /// Actor.
    Actor = 2,
    /// Runtime.
    Runtime = 3,
}

/// Presence marker for actor scope and clear operations; false is invalid.
#[derive(Clone, Copy, Debug, PartialEq, TS)]
#[ts(type = "true", export_to = "actors/SocketMarker.ts")]
pub struct SocketMarker;
impl TryFrom<bool> for SocketMarker {
    type Error = DomainError;
    fn try_from(value: bool) -> Result<Self, Self::Error> {
        value.then_some(Self).ok_or(DomainError::InvalidSocket)
    }
}
impl From<SocketMarker> for bool {
    fn from(_: SocketMarker) -> bool {
        true
    }
}
