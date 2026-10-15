use super::*;

/// SocketOpen data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketOpen::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketOpen.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketOpen {
    #[wire(tag = 1, string)]
    pub(super) actor_id: ActorId,
    #[wire(tag = 2, string)]
    pub(super) connection_id: String,
    #[wire(tag = 3, string)]
    #[ts(optional)]
    pub(super) app_channel: Option<String>,
    #[wire(tag = 4, string)]
    pub(super) subprotocols: Vec<String>,
    #[wire(tag = 5, message)]
    pub(super) headers: Vec<Header>,
    #[wire(tag = 6, uint64)]
    #[ts(type = "bigint")]
    pub(super) resume_after: u64,
    #[wire(tag = 7, bool)]
    pub(super) reconnected: bool,
    #[wire(tag = 9, enumeration)]
    pub(super) transport: SocketTransport,
}

impl SocketOpen {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            valid_socket_id(&self.connection_id)
                && self
                    .app_channel
                    .as_ref()
                    .is_none_or(|channel| valid_socket_id(channel))
                && valid_subprotocols(&self.subprotocols)
                && self.headers.len() <= 64
                && self.headers.iter().all(|header| {
                    valid_header_name(header.name())
                        && header.value().len() <= 8192
                        && !header.value().contains(['\r', '\n', '\0'])
                }),
        )
    }

    /// Runtime-selected diagnostic transport.
    pub fn transport(&self) -> SocketTransport {
        self.transport
    }

    /// Returns actor id.
    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }
    /// Returns connection id.
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }
    /// Returns app channel.
    pub fn app_channel(&self) -> &Option<String> {
        &self.app_channel
    }
    /// Returns subprotocols.
    pub fn subprotocols(&self) -> &[String] {
        &self.subprotocols
    }
    /// Returns headers.
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }
    /// Returns resume after.
    pub fn resume_after(&self) -> u64 {
        self.resume_after
    }
    /// Returns reconnected.
    pub fn reconnected(&self) -> bool {
        self.reconnected
    }
}

/// SocketMessage data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketMessage::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketMessage.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketMessage {
    #[wire(tag = 1, uint64)]
    #[ts(type = "bigint")]
    pub(super) offset: PositiveU64,
    #[wire(oneof = "2,3")]
    #[ts(inline)]
    pub(super) payload: socket_message::Payload,
}

impl SocketMessage {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(self.payload.len() <= MAX_SOCKET_FRAME_BYTES)
    }
    /// Returns offset.
    pub fn offset(&self) -> PositiveU64 {
        self.offset
    }
    /// Returns payload.
    pub fn payload(&self) -> &socket_message::Payload {
        &self.payload
    }
}

/// SocketClose data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketClose::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketClose.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketClose {
    #[wire(tag = 1, uint64)]
    #[ts(type = "bigint")]
    pub(super) offset: PositiveU64,
    #[wire(tag = 2, uint32)]
    pub(super) code: u32,
    #[wire(tag = 3, string)]
    pub(super) reason: String,
    #[wire(tag = 4, enumeration)]
    pub(super) initiator: SocketCloseInitiator,
}

impl SocketClose {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            (valid_close(self.code, &self.reason)
                || (matches!(self.code, 1005 | 1006 | 1015) && self.reason.len() <= 123))
                && self.initiator != SocketCloseInitiator::Unspecified,
        )
    }

    /// Returns offset.
    pub fn offset(&self) -> PositiveU64 {
        self.offset
    }
    /// Returns code.
    pub fn code(&self) -> u32 {
        self.code
    }
    /// Returns reason.
    pub fn reason(&self) -> &str {
        &self.reason
    }
    /// Returns initiator.
    pub fn initiator(&self) -> SocketCloseInitiator {
        self.initiator
    }
}

/// SocketError data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketError::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketError.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketError {
    #[wire(tag = 1, uint64)]
    #[ts(type = "bigint")]
    pub(super) offset: PositiveU64,
    #[wire(tag = 2, string)]
    pub(super) code: String,
    #[wire(tag = 3, string)]
    pub(super) message: String,
}

impl SocketError {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            valid_socket_id(&self.code) && !self.message.is_empty() && self.message.len() <= 4096,
        )
    }

    /// Returns offset.
    pub fn offset(&self) -> PositiveU64 {
        self.offset
    }
    /// Returns code.
    pub fn code(&self) -> &str {
        &self.code
    }
    /// Returns message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// SocketReconnecting data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketReconnecting::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketReconnecting.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketReconnecting {
    #[wire(tag = 1, uint64)]
    #[ts(type = "bigint")]
    pub(super) attempt: PositiveU64,
    #[wire(tag = 2, uint32)]
    pub(super) delay_ms: u32,
}

impl SocketReconnecting {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(self.delay_ms > 0 && self.delay_ms <= 300_000)
    }

    /// Returns attempt.
    pub fn attempt(&self) -> PositiveU64 {
        self.attempt
    }
    /// Returns delay ms.
    pub fn delay_ms(&self) -> u32 {
        self.delay_ms
    }
}

/// ActorCall data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = ActorCall::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/ActorCall.ts")]
#[ts(rename_all = "camelCase")]
pub struct ActorCall {
    #[wire(tag = 1, string)]
    pub(super) request_id: String,
    #[wire(tag = 2, message)]
    pub(super) request: InvokeActorRequest,
}

impl ActorCall {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(valid_socket_id(&self.request_id))
    }

    /// Returns request id.
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    /// Returns request.
    pub fn request(&self) -> &InvokeActorRequest {
        &self.request
    }
}

/// ActorCallResult data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = ActorCallResult::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/ActorCallResult.ts")]
#[ts(rename_all = "camelCase")]
pub struct ActorCallResult {
    #[wire(tag = 1, string)]
    pub(super) request_id: String,
    #[wire(tag = 2, message)]
    pub(super) response: InvokeActorResponse,
}

impl ActorCallResult {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(valid_socket_id(&self.request_id))
    }

    /// Returns request id.
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    /// Returns response.
    pub fn response(&self) -> &InvokeActorResponse {
        &self.response
    }
}

/// SocketCheckpoint data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketCheckpoint::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketCheckpoint.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketCheckpoint {
    #[wire(tag = 1, uint64)]
    #[ts(type = "bigint")]
    pub(super) checkpoint_sequence: PositiveU64,
    #[wire(tag = 2, bytes)]
    #[ts(type = "import(\"../../readonly.js\").ReadonlyBytes")]
    pub(super) state: Vec<u8>,
    #[wire(tag = 3, message)]
    pub(super) connections: Vec<SocketConnection>,
    #[wire(tag = 4, message)]
    pub(super) effects: Vec<SocketEffect>,
    #[wire(tag = 5, message)]
    #[ts(optional)]
    pub(super) auto_response: Option<SocketAutoResponse>,
}

impl SocketCheckpoint {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            self.connections.len() <= MAX_SOCKET_CONNECTIONS
                && self.effects.len() <= MAX_SOCKET_CONNECTIONS
                && unique_by(&self.connections, |connection| {
                    connection.connection_id.clone()
                })
                && self.effects.iter().enumerate().all(|(index, effect)| {
                    effect.effect_id().checkpoint_sequence() == self.checkpoint_sequence
                        && usize::try_from(effect.effect_id().index_in_outbox()) == Ok(index)
                })
                && self.effects.first().is_none_or(|first| {
                    self.effects
                        .iter()
                        .all(|effect| effect.effect_id().actor_id() == first.effect_id().actor_id())
                }),
        )
    }

    /// Returns checkpoint sequence.
    pub fn checkpoint_sequence(&self) -> PositiveU64 {
        self.checkpoint_sequence
    }
    /// Returns state.
    pub fn state(&self) -> &[u8] {
        &self.state
    }
    /// Returns connections.
    pub fn connections(&self) -> &[SocketConnection] {
        &self.connections
    }
    /// Returns effects.
    pub fn effects(&self) -> &[SocketEffect] {
        &self.effects
    }
    /// Returns auto response.
    pub fn auto_response(&self) -> &Option<SocketAutoResponse> {
        &self.auto_response
    }
}

/// Application payload cases for SocketMessage.
pub mod socket_message {
    use super::*;
    /// Exact text or binary frame; neither may be omitted on ingress.
    #[acyclic_contract_derive::oneof(error = DomainError, file = ActorsFile)]
    #[derive(Clone, Debug, PartialEq, TS)]
    #[ts(
        tag = "case",
        content = "value",
        rename_all = "camelCase",
        export_to = "actors/SocketMessagePayload.ts",
        rename = "SocketMessagePayload"
    )]
    pub enum Payload {
        /// UTF-8 text.
        #[wire(tag = 2, string)]
        Text(String),
        /// Unmodified binary bytes.
        #[wire(tag = 3, bytes)]
        #[ts(type = "import(\"../../readonly.js\").ReadonlyBytes")]
        Binary(Vec<u8>),
    }
    impl Payload {
        /// Application frame length in bytes.
        pub fn len(&self) -> usize {
            match self {
                Self::Text(value) => value.len(),
                Self::Binary(value) => value.len(),
            }
        }
        /// Whether this frame has no bytes.
        pub fn is_empty(&self) -> bool {
            self.len() == 0
        }
    }
}
