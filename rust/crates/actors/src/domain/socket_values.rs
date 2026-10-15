use super::*;

/// EffectId data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/EffectId.ts")]
#[ts(rename_all = "camelCase")]
pub struct EffectId {
    #[wire(tag = 1, string)]
    pub(super) actor_id: ActorId,
    #[wire(tag = 2, uint64)]
    #[ts(type = "bigint")]
    pub(super) checkpoint_sequence: PositiveU64,
    #[wire(tag = 3, uint32)]
    pub(super) index_in_outbox: u32,
}

impl EffectId {
    /// Deterministic position of an effect in one actor checkpoint outbox.
    pub fn new(actor_id: ActorId, checkpoint_sequence: PositiveU64, index_in_outbox: u32) -> Self {
        Self {
            actor_id,
            checkpoint_sequence,
            index_in_outbox,
        }
    }

    /// Returns actor id.
    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }
    /// Returns checkpoint sequence.
    pub fn checkpoint_sequence(&self) -> PositiveU64 {
        self.checkpoint_sequence
    }
    /// Returns index in outbox.
    pub fn index_in_outbox(&self) -> u32 {
        self.index_in_outbox
    }
}

/// SocketConnection data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketConnection::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketConnection.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketConnection {
    #[wire(tag = 1, string)]
    pub(super) connection_id: String,
    #[wire(tag = 2, bytes)]
    #[ts(type = "import(\"../../readonly.js\").ReadonlyBytes")]
    pub(super) attachment: Vec<u8>,
    #[wire(tag = 3, string)]
    pub(super) tags: Vec<String>,
    #[wire(tag = 4, uint64)]
    #[ts(type = "bigint")]
    pub(super) delivered_offset: u64,
    #[wire(tag = 5, bool)]
    pub(super) outbound: bool,
    #[wire(tag = 6, message)]
    #[ts(optional)]
    pub(super) auto_response: Option<SocketAutoResponse>,
    #[wire(tag = 7, uint64)]
    #[ts(type = "bigint")]
    #[ts(optional)]
    pub(super) last_auto_response_at: Option<u64>,
    #[wire(tag = 8, enumeration)]
    pub(super) transport: SocketTransport,
}

impl SocketConnection {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            valid_socket_id(&self.connection_id)
                && self.attachment.len() <= MAX_SOCKET_ATTACHMENT_BYTES
                && valid_tags(&self.tags),
        )
    }

    /// Runtime-selected diagnostic transport.
    pub fn transport(&self) -> SocketTransport {
        self.transport
    }

    /// Returns connection id.
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }
    /// Returns attachment.
    pub fn attachment(&self) -> &[u8] {
        &self.attachment
    }
    /// Returns tags.
    pub fn tags(&self) -> &[String] {
        &self.tags
    }
    /// Returns delivered offset.
    pub fn delivered_offset(&self) -> u64 {
        self.delivered_offset
    }
    /// Returns outbound.
    pub fn outbound(&self) -> bool {
        self.outbound
    }
    /// Returns auto response.
    pub fn auto_response(&self) -> &Option<SocketAutoResponse> {
        &self.auto_response
    }
    /// Returns last auto response at.
    pub fn last_auto_response_at(&self) -> Option<u64> {
        self.last_auto_response_at
    }
}

/// SocketAutoResponse data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketAutoResponse::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketAutoResponse.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketAutoResponse {
    #[wire(tag = 1, string)]
    pub(super) request: String,
    #[wire(tag = 2, string)]
    pub(super) response: String,
}

impl SocketAutoResponse {
    /// Creates a bounded exact text request/response pair.
    pub fn new(request: String, response: String) -> Result<Self, DomainError> {
        let pair = Self { request, response };
        pair.validate()?;
        Ok(pair)
    }

    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            self.request.len() <= MAX_SOCKET_AUTO_RESPONSE_BYTES
                && self.response.len() <= MAX_SOCKET_AUTO_RESPONSE_BYTES,
        )
    }

    /// Returns request.
    pub fn request(&self) -> &str {
        &self.request
    }
    /// Returns response.
    pub fn response(&self) -> &str {
        &self.response
    }
}

/// SocketConfig data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketConfig::validate)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/SocketConfig.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketConfig {
    #[wire(tag = 1, uint32)]
    pub(super) max_frame_bytes: u32,
    #[wire(tag = 2, uint32)]
    pub(super) max_connections: u32,
    #[wire(tag = 3, uint32)]
    pub(super) inbound_buffer_bytes: u32,
    #[wire(tag = 4, uint32)]
    pub(super) inbound_buffer_frames: u32,
    #[wire(tag = 5, uint32)]
    pub(super) actor_buffer_bytes: u32,
}

impl SocketConfig {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            self.max_frame_bytes > 0
                && self.max_frame_bytes as usize <= MAX_SOCKET_FRAME_BYTES
                && self.max_connections > 0
                && self.max_connections as usize <= MAX_SOCKET_CONNECTIONS
                && self.inbound_buffer_bytes > 0
                && self.inbound_buffer_bytes <= 1_048_576
                && self.inbound_buffer_frames > 0
                && self.inbound_buffer_frames <= 1024
                && self.actor_buffer_bytes >= self.inbound_buffer_bytes
                && self.actor_buffer_bytes <= 16_777_216,
        )
    }

    /// Returns max frame bytes.
    pub fn max_frame_bytes(&self) -> u32 {
        self.max_frame_bytes
    }
    /// Returns max connections.
    pub fn max_connections(&self) -> u32 {
        self.max_connections
    }
    /// Returns inbound buffer bytes.
    pub fn inbound_buffer_bytes(&self) -> u32 {
        self.inbound_buffer_bytes
    }
    /// Returns inbound buffer frames.
    pub fn inbound_buffer_frames(&self) -> u32 {
        self.inbound_buffer_frames
    }
    /// Returns actor buffer bytes.
    pub fn actor_buffer_bytes(&self) -> u32 {
        self.actor_buffer_bytes
    }
}
