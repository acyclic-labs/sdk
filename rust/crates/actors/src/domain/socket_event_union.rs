use super::*;

/// A typed SocketEvent payload.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketEvent.ts")]
pub struct SocketEvent {
    #[wire(oneof = "1,2,3,4,5")]
    #[ts(inline)]
    pub(super) value: socket_event::Value,
}

impl SocketEvent {
    /// Wraps an already validated typed operation.
    pub fn new(value: socket_event::Value) -> Self {
        Self { value }
    }

    /// Returns the typed payload.
    pub fn value(&self) -> &socket_event::Value {
        &self.value
    }
}

/// Variants of SocketEvent.
pub mod socket_event {
    use super::*;
    /// One typed payload.
    #[acyclic_contract_derive::oneof(error = DomainError, file = ActorsFile)]
    #[derive(Clone, Debug, PartialEq, TS)]
    #[ts(
        tag = "case",
        content = "value",
        rename_all = "camelCase",
        rename = "SocketEventValue",
        export_to = "actors/SocketEventValue.ts"
    )]
    pub enum Value {
        /// Open payload.
        #[wire(tag = 1, message)]
        Open(SocketOpen),
        /// Message payload.
        #[wire(tag = 2, message)]
        Message(SocketMessage),
        /// Close payload.
        #[wire(tag = 3, message)]
        Close(SocketClose),
        /// Error payload.
        #[wire(tag = 4, message)]
        Error(SocketError),
        /// Reconnecting payload.
        #[wire(tag = 5, message)]
        Reconnecting(SocketReconnecting),
    }
}

/// A typed SocketDecision payload.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketDecision.ts")]
pub struct SocketDecision {
    #[wire(oneof = "1,2")]
    #[ts(inline)]
    pub(super) value: socket_decision::Value,
}

impl SocketDecision {
    /// Wraps an already validated typed operation.
    pub fn new(value: socket_decision::Value) -> Self {
        Self { value }
    }

    /// Returns the typed payload.
    pub fn value(&self) -> &socket_decision::Value {
        &self.value
    }
}

/// Variants of SocketDecision.
pub mod socket_decision {
    use super::*;
    /// One typed payload.
    #[acyclic_contract_derive::oneof(error = DomainError, file = ActorsFile)]
    #[derive(Clone, Debug, PartialEq, TS)]
    #[ts(
        tag = "case",
        content = "value",
        rename_all = "camelCase",
        rename = "SocketDecisionValue",
        export_to = "actors/SocketDecisionValue.ts"
    )]
    pub enum Value {
        /// Accept payload.
        #[wire(tag = 1, message)]
        Accept(SocketAccept),
        /// Reject payload.
        #[wire(tag = 2, message)]
        Reject(SocketReject),
    }
}
