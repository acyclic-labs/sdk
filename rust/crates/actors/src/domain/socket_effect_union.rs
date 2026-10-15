use super::*;

/// A typed SocketEffect payload.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketEffect.ts")]
pub struct SocketEffect {
    #[wire(oneof = "1,2,3,4,5,6,7")]
    #[ts(inline)]
    pub(super) value: socket_effect::Value,
}

impl SocketEffect {
    /// Wraps an already validated typed operation.
    pub fn new(value: socket_effect::Value) -> Self {
        Self { value }
    }

    /// Returns the typed payload.
    pub fn value(&self) -> &socket_effect::Value {
        &self.value
    }
    /// Stable checkpoint/outbox identity for replay deduplication.
    pub fn effect_id(&self) -> &EffectId {
        match &self.value {
            socket_effect::Value::Send(effect) => effect.effect_id(),
            socket_effect::Value::Close(effect) => effect.effect_id(),
            socket_effect::Value::Broadcast(effect) => effect.effect_id(),
            socket_effect::Value::SetAutoResponse(effect) => effect.effect_id(),
            socket_effect::Value::SetTags(effect) => effect.effect_id(),
            socket_effect::Value::SetAttachment(effect) => effect.effect_id(),
            socket_effect::Value::Connect(effect) => effect.effect_id(),
        }
    }
}

/// Variants of SocketEffect.
pub mod socket_effect {
    use super::*;
    /// One typed payload.
    #[acyclic_contract_derive::oneof(error = DomainError, file = ActorsFile)]
    #[derive(Clone, Debug, PartialEq, TS)]
    #[ts(
        tag = "case",
        content = "value",
        rename_all = "camelCase",
        rename = "SocketEffectValue",
        export_to = "actors/SocketEffectValue.ts"
    )]
    pub enum Value {
        /// Send payload.
        #[wire(tag = 1, message)]
        Send(Send),
        /// Close payload.
        #[wire(tag = 2, message)]
        Close(Close),
        /// Broadcast payload.
        #[wire(tag = 3, message)]
        Broadcast(Broadcast),
        /// SetAutoResponse payload.
        #[wire(tag = 4, message)]
        SetAutoResponse(SetAutoResponse),
        /// SetTags payload.
        #[wire(tag = 5, message)]
        SetTags(SetTags),
        /// SetAttachment payload.
        #[wire(tag = 6, message)]
        SetAttachment(SetAttachment),
        /// Connect payload.
        #[wire(tag = 7, message)]
        Connect(Connect),
    }
}
