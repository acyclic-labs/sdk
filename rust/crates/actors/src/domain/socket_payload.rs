use super::*;

/// A typed SocketPayload payload.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketPayload::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketPayload.ts")]
pub struct SocketPayload {
    #[wire(oneof = "1,2")]
    #[ts(inline)]
    pub(super) value: socket_payload::Value,
}

impl SocketPayload {
    /// Bounded text frame.
    pub fn text(text: String) -> Result<Self, DomainError> {
        let payload = Self {
            value: socket_payload::Value::Text(text),
        };
        payload.validate()?;
        Ok(payload)
    }
    /// Bounded binary frame.
    pub fn binary(bytes: Vec<u8>) -> Result<Self, DomainError> {
        let payload = Self {
            value: socket_payload::Value::Binary(bytes),
        };
        payload.validate()?;
        Ok(payload)
    }

    /// Returns the typed payload.
    pub fn value(&self) -> &socket_payload::Value {
        &self.value
    }
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(self.len() <= MAX_SOCKET_FRAME_BYTES)
    }
    /// Payload length in UTF-8 bytes for text.
    pub fn len(&self) -> usize {
        match &self.value {
            socket_payload::Value::Text(value) => value.len(),
            socket_payload::Value::Binary(value) => value.len(),
        }
    }
    /// Whether the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Variants of SocketPayload.
pub mod socket_payload {
    use super::*;
    /// One typed payload.
    #[acyclic_contract_derive::oneof(error = DomainError, file = ActorsFile)]
    #[derive(Clone, Debug, PartialEq, TS)]
    #[ts(
        tag = "case",
        content = "value",
        rename_all = "camelCase",
        rename = "SocketPayloadValue",
        export_to = "actors/SocketPayloadValue.ts"
    )]
    pub enum Value {
        /// Text payload.
        #[wire(tag = 1, string)]
        Text(String),
        /// Binary payload.
        #[wire(tag = 2, bytes)]
        #[ts(type = "import(\"../../readonly.js\").ReadonlyBytes")]
        Binary(Vec<u8>),
    }
}

/// A typed SocketBroadcastTarget payload.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketBroadcastTarget::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketBroadcastTarget.ts")]
pub struct SocketBroadcastTarget {
    #[wire(oneof = "1,2")]
    #[ts(inline)]
    pub(super) value: socket_broadcast_target::Value,
}

impl SocketBroadcastTarget {
    /// Returns the typed payload.
    pub fn value(&self) -> &socket_broadcast_target::Value {
        &self.value
    }
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(match &self.value {
            socket_broadcast_target::Value::Tag(tag) => valid_tag(tag),
            socket_broadcast_target::Value::Connections(_) => true,
        })
    }
}

/// Variants of SocketBroadcastTarget.
pub mod socket_broadcast_target {
    use super::*;
    /// One typed payload.
    #[acyclic_contract_derive::oneof(error = DomainError, file = ActorsFile)]
    #[derive(Clone, Debug, PartialEq, TS)]
    #[ts(
        tag = "case",
        content = "value",
        rename_all = "camelCase",
        rename = "SocketBroadcastTargetValue",
        export_to = "actors/SocketBroadcastTargetValue.ts"
    )]
    pub enum Value {
        /// Tag payload.
        #[wire(tag = 1, string)]
        Tag(String),
        /// Connections payload.
        #[wire(tag = 2, message)]
        Connections(SocketConnectionSet),
    }
}

/// A typed SocketScope payload.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketScope::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketScope.ts")]
pub struct SocketScope {
    #[wire(oneof = "1,2")]
    #[ts(inline)]
    pub(super) value: socket_scope::Value,
}

impl SocketScope {
    /// Actor default scope.
    pub fn actor() -> Self {
        Self {
            value: socket_scope::Value::Actor(SocketMarker),
        }
    }
    /// Per-connection override scope.
    pub fn connection(connection_id: String) -> Result<Self, DomainError> {
        let scope = Self {
            value: socket_scope::Value::ConnectionId(connection_id),
        };
        scope.validate()?;
        Ok(scope)
    }

    /// Returns the typed payload.
    pub fn value(&self) -> &socket_scope::Value {
        &self.value
    }
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(match &self.value {
            socket_scope::Value::Actor(_) => true,
            socket_scope::Value::ConnectionId(id) => valid_socket_id(id),
        })
    }
}

/// Variants of SocketScope.
pub mod socket_scope {
    use super::*;
    /// One typed payload.
    #[acyclic_contract_derive::oneof(error = DomainError, file = ActorsFile)]
    #[derive(Clone, Debug, PartialEq, TS)]
    #[ts(
        tag = "case",
        content = "value",
        rename_all = "camelCase",
        rename = "SocketScopeValue",
        export_to = "actors/SocketScopeValue.ts"
    )]
    pub enum Value {
        /// Actor payload.
        #[wire(tag = 1, bool)]
        Actor(SocketMarker),
        /// ConnectionId payload.
        #[wire(tag = 2, string)]
        ConnectionId(String),
    }
}

/// A typed SocketAutoResponseUpdate payload.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketAutoResponseUpdate.ts")]
pub struct SocketAutoResponseUpdate {
    #[wire(oneof = "1,2")]
    #[ts(inline)]
    pub(super) value: socket_auto_response_update::Value,
}

impl SocketAutoResponseUpdate {
    /// Installs the exact bounded pair.
    pub fn pair(pair: SocketAutoResponse) -> Self {
        Self {
            value: socket_auto_response_update::Value::Pair(pair),
        }
    }
    /// Clears the scope; a cleared connection inherits the actor default.
    pub fn clear() -> Self {
        Self {
            value: socket_auto_response_update::Value::Clear(SocketMarker),
        }
    }

    /// Returns the typed payload.
    pub fn value(&self) -> &socket_auto_response_update::Value {
        &self.value
    }
}

/// Variants of SocketAutoResponseUpdate.
pub mod socket_auto_response_update {
    use super::*;
    /// One typed payload.
    #[acyclic_contract_derive::oneof(error = DomainError, file = ActorsFile)]
    #[derive(Clone, Debug, PartialEq, TS)]
    #[ts(
        tag = "case",
        content = "value",
        rename_all = "camelCase",
        rename = "SocketAutoResponseUpdateValue",
        export_to = "actors/SocketAutoResponseUpdateValue.ts"
    )]
    pub enum Value {
        /// Pair payload.
        #[wire(tag = 1, message)]
        Pair(SocketAutoResponse),
        /// Clear payload.
        #[wire(tag = 2, bool)]
        Clear(SocketMarker),
    }
}
