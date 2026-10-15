use super::*;

/// Runtime resource accounting or declared keepalive; auto responses and pings are not billable.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketMetering::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketMetering.ts", rename_all = "camelCase")]
pub struct SocketMetering {
    #[wire(tag = 1, string)]
    pub(super) actor_id: ActorId,
    #[wire(tag = 2, string)]
    #[ts(optional)]
    pub(super) connection_id: Option<String>,
    #[wire(tag = 3, uint64)]
    #[ts(type = "bigint")]
    pub(super) wakes: u64,
    #[wire(tag = 4, uint64)]
    #[ts(type = "bigint")]
    pub(super) messages_in: u64,
    #[wire(tag = 5, uint64)]
    #[ts(type = "bigint")]
    pub(super) messages_out: u64,
    #[wire(tag = 6, uint64)]
    #[ts(type = "bigint")]
    pub(super) bytes_in: u64,
    #[wire(tag = 7, uint64)]
    #[ts(type = "bigint")]
    pub(super) bytes_out: u64,
    #[wire(tag = 8, double)]
    pub(super) real_socket_seconds: f64,
    #[wire(tag = 9, double)]
    pub(super) buffered_byte_seconds: f64,
    #[wire(tag = 10, uint64)]
    #[ts(type = "bigint")]
    pub(super) auto_responses: u64,
    #[wire(tag = 11, uint64)]
    #[ts(type = "bigint")]
    pub(super) protocol_pings: u64,
}
impl SocketMetering {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            self.connection_id
                .as_ref()
                .is_none_or(|id| valid_socket_id(id))
                && self.real_socket_seconds.is_finite()
                && self.real_socket_seconds >= 0.0
                && self.buffered_byte_seconds.is_finite()
                && self.buffered_byte_seconds >= 0.0,
        )
    }
    /// Returns actor id.
    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }
    /// Returns connection id.
    pub fn connection_id(&self) -> &Option<String> {
        &self.connection_id
    }
    /// Returns wakes.
    pub fn wakes(&self) -> u64 {
        self.wakes
    }
    /// Returns messages in.
    pub fn messages_in(&self) -> u64 {
        self.messages_in
    }
    /// Returns messages out.
    pub fn messages_out(&self) -> u64 {
        self.messages_out
    }
    /// Returns bytes in.
    pub fn bytes_in(&self) -> u64 {
        self.bytes_in
    }
    /// Returns bytes out.
    pub fn bytes_out(&self) -> u64 {
        self.bytes_out
    }
    /// Returns real socket seconds.
    pub fn real_socket_seconds(&self) -> f64 {
        self.real_socket_seconds
    }
    /// Returns buffered byte seconds.
    pub fn buffered_byte_seconds(&self) -> f64 {
        self.buffered_byte_seconds
    }
    /// Returns auto responses.
    pub fn auto_responses(&self) -> u64 {
        self.auto_responses
    }
    /// Returns protocol pings.
    pub fn protocol_pings(&self) -> u64 {
        self.protocol_pings
    }
}

/// Runtime resource accounting or declared keepalive; auto responses and pings are not billable.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketKeepalive.ts", rename_all = "camelCase")]
pub struct SocketKeepalive {
    #[wire(tag = 1, message)]
    pub(super) frame: SocketPayload,
    #[wire(tag = 2, uint64)]
    #[ts(type = "bigint")]
    pub(super) interval_ms: PositiveU64,
}
impl SocketKeepalive {
    /// Returns frame.
    pub fn frame(&self) -> &SocketPayload {
        &self.frame
    }
    /// Returns interval ms.
    pub fn interval_ms(&self) -> PositiveU64 {
        self.interval_ms
    }
}
