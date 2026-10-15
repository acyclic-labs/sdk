use super::*;

/// Connect data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = Connect::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/Connect.ts")]
#[ts(rename_all = "camelCase")]
pub struct Connect {
    #[wire(tag = 1, message)]
    pub(super) effect_id: EffectId,
    #[wire(tag = 2, string)]
    pub(super) binding: String,
    #[wire(oneof = "3,6")]
    #[ts(inline)]
    pub(super) target: connect::Target,
    #[wire(tag = 4, string)]
    pub(super) subprotocols: Vec<String>,
    #[wire(tag = 5, message)]
    pub(super) reconnect: SocketReconnect,
    #[wire(tag = 7, message)]
    #[ts(optional)]
    pub(super) keepalive: Option<SocketKeepalive>,
}

impl Connect {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            (self.binding.is_empty() || valid_socket_id(&self.binding))
                && (match &self.target {
                    connect::Target::Url(url) => valid_socket_url(url),
                    connect::Target::ActorAddress(_) => true,
                })
                && valid_subprotocols(&self.subprotocols),
        )
    }

    /// Declared fixed outbound frame and interval; runtime sends without actor wake.
    pub fn keepalive(&self) -> Option<&SocketKeepalive> {
        self.keepalive.as_ref()
    }

    /// Returns effect id.
    pub fn effect_id(&self) -> &EffectId {
        &self.effect_id
    }
    /// Returns binding.
    pub fn binding(&self) -> &str {
        &self.binding
    }
    /// One URL or actor address; the runtime chooses its transport.
    pub fn target(&self) -> &connect::Target {
        &self.target
    }
    /// Returns subprotocols.
    pub fn subprotocols(&self) -> &[String] {
        &self.subprotocols
    }
    /// Returns reconnect.
    pub fn reconnect(&self) -> &SocketReconnect {
        &self.reconnect
    }
}

/// SocketAccept data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketAccept::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketAccept.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketAccept {
    #[wire(tag = 1, string)]
    #[ts(optional)]
    pub(super) subprotocol: Option<String>,
    #[wire(tag = 2, bytes)]
    #[ts(type = "import(\"../../readonly.js\").ReadonlyBytes")]
    pub(super) attachment: Vec<u8>,
    #[wire(tag = 3, string)]
    pub(super) tags: Vec<String>,
}

impl SocketAccept {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            self.subprotocol
                .as_ref()
                .is_none_or(|protocol| valid_protocol(protocol))
                && self.attachment.len() <= MAX_SOCKET_ATTACHMENT_BYTES
                && valid_tags(&self.tags),
        )
    }

    /// Returns subprotocol.
    pub fn subprotocol(&self) -> &Option<String> {
        &self.subprotocol
    }
    /// Returns attachment.
    pub fn attachment(&self) -> &[u8] {
        &self.attachment
    }
    /// Returns tags.
    pub fn tags(&self) -> &[String] {
        &self.tags
    }
}

/// SocketReject data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketReject::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketReject.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketReject {
    #[wire(tag = 1, uint32)]
    pub(super) status: u32,
    #[wire(tag = 2, string)]
    pub(super) reason: String,
}

impl SocketReject {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require((400..=599).contains(&self.status) && self.reason.len() <= 123)
    }

    /// Returns status.
    pub fn status(&self) -> u32 {
        self.status
    }
    /// Returns reason.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// Send data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = Send::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/Send.ts")]
#[ts(rename_all = "camelCase")]
pub struct Send {
    #[wire(tag = 1, message)]
    pub(super) effect_id: EffectId,
    #[wire(tag = 2, string)]
    pub(super) connection_id: String,
    #[wire(oneof = "3,4")]
    #[ts(inline)]
    pub(super) payload: send::Payload,
}

impl Send {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            valid_socket_id(&self.connection_id) && self.payload.len() <= MAX_SOCKET_FRAME_BYTES,
        )
    }

    /// Returns effect id.
    pub fn effect_id(&self) -> &EffectId {
        &self.effect_id
    }
    /// Returns connection id.
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }
    /// Returns payload.
    pub fn payload(&self) -> &send::Payload {
        &self.payload
    }
}

/// Close data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = Close::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/Close.ts")]
#[ts(rename_all = "camelCase")]
pub struct Close {
    #[wire(tag = 1, message)]
    pub(super) effect_id: EffectId,
    #[wire(tag = 2, string)]
    pub(super) connection_id: String,
    #[wire(tag = 3, uint32)]
    pub(super) code: u32,
    #[wire(tag = 4, string)]
    pub(super) reason: String,
}

impl Close {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(valid_socket_id(&self.connection_id) && valid_close(self.code, &self.reason))
    }

    /// Returns effect id.
    pub fn effect_id(&self) -> &EffectId {
        &self.effect_id
    }
    /// Returns connection id.
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }
    /// Returns code.
    pub fn code(&self) -> u32 {
        self.code
    }
    /// Returns reason.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// SocketConnectionSet data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketConnectionSet::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketConnectionSet.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketConnectionSet {
    #[wire(tag = 1, string)]
    pub(super) connection_ids: Vec<String>,
}

impl SocketConnectionSet {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            !self.connection_ids.is_empty()
                && self.connection_ids.len() <= MAX_SOCKET_CONNECTIONS
                && self.connection_ids.iter().all(|id| valid_socket_id(id))
                && unique(&self.connection_ids),
        )
    }

    /// Returns connection ids.
    pub fn connection_ids(&self) -> &[String] {
        &self.connection_ids
    }
}

/// Broadcast data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = Broadcast::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/Broadcast.ts")]
#[ts(rename_all = "camelCase")]
pub struct Broadcast {
    #[wire(tag = 1, message)]
    pub(super) effect_id: EffectId,
    #[wire(tag = 2, message)]
    pub(super) target: SocketBroadcastTarget,
    #[wire(tag = 3, string)]
    pub(super) except: Vec<String>,
    #[wire(tag = 4, message)]
    pub(super) payload: SocketPayload,
}

impl Broadcast {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            self.except.len() <= MAX_SOCKET_CONNECTIONS
                && self.except.iter().all(|id| valid_socket_id(id))
                && unique(&self.except),
        )
    }

    /// Returns effect id.
    pub fn effect_id(&self) -> &EffectId {
        &self.effect_id
    }
    /// Returns target.
    pub fn target(&self) -> &SocketBroadcastTarget {
        &self.target
    }
    /// Returns except.
    pub fn except(&self) -> &[String] {
        &self.except
    }
    /// Returns payload.
    pub fn payload(&self) -> &SocketPayload {
        &self.payload
    }
}

/// SetAutoResponse data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SetAutoResponse.ts")]
#[ts(rename_all = "camelCase")]
pub struct SetAutoResponse {
    #[wire(tag = 1, message)]
    pub(super) effect_id: EffectId,
    #[wire(tag = 2, message)]
    pub(super) scope: SocketScope,
    #[wire(tag = 3, message)]
    pub(super) setting: SocketAutoResponseUpdate,
}

impl SetAutoResponse {
    /// Returns effect id.
    pub fn effect_id(&self) -> &EffectId {
        &self.effect_id
    }
    /// Returns scope.
    pub fn scope(&self) -> &SocketScope {
        &self.scope
    }
    /// Returns setting.
    pub fn setting(&self) -> &SocketAutoResponseUpdate {
        &self.setting
    }
}

/// SetTags data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SetTags::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SetTags.ts")]
#[ts(rename_all = "camelCase")]
pub struct SetTags {
    #[wire(tag = 1, message)]
    pub(super) effect_id: EffectId,
    #[wire(tag = 2, string)]
    pub(super) connection_id: String,
    #[wire(tag = 3, string)]
    pub(super) tags: Vec<String>,
}

impl SetTags {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(valid_socket_id(&self.connection_id) && valid_tags(&self.tags))
    }

    /// Returns effect id.
    pub fn effect_id(&self) -> &EffectId {
        &self.effect_id
    }
    /// Returns connection id.
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }
    /// Returns tags.
    pub fn tags(&self) -> &[String] {
        &self.tags
    }
}

/// SetAttachment data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SetAttachment::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SetAttachment.ts")]
#[ts(rename_all = "camelCase")]
pub struct SetAttachment {
    #[wire(tag = 1, message)]
    pub(super) effect_id: EffectId,
    #[wire(tag = 2, string)]
    pub(super) connection_id: String,
    #[wire(tag = 3, bytes)]
    #[ts(type = "import(\"../../readonly.js\").ReadonlyBytes")]
    pub(super) attachment: Vec<u8>,
}

impl SetAttachment {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            valid_socket_id(&self.connection_id)
                && self.attachment.len() <= MAX_SOCKET_ATTACHMENT_BYTES,
        )
    }

    /// Returns effect id.
    pub fn effect_id(&self) -> &EffectId {
        &self.effect_id
    }
    /// Returns connection id.
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }
    /// Returns attachment.
    pub fn attachment(&self) -> &[u8] {
        &self.attachment
    }
}

/// The common connect API accepts internal addresses and URLs.
pub mod connect {
    use super::*;
    /// A URL or an actor address with an optional application channel.
    #[acyclic_contract_derive::oneof(error = DomainError, file = ActorsFile)]
    #[derive(Clone, Debug, PartialEq, TS)]
    #[ts(
        tag = "case",
        content = "value",
        rename_all = "camelCase",
        export_to = "actors/ConnectTarget.ts",
        rename = "ConnectTarget"
    )]
    pub enum Target {
        /// Any WebSocket URL, including a platform actor URL.
        #[wire(tag = 3, string)]
        Url(String),
        /// Actor address resolved over the shared session.
        #[wire(tag = 6, message)]
        ActorAddress(ActorAddress),
    }
}

/// Application payload cases for Send.
pub mod send {
    use super::*;
    /// Exact text or binary frame; neither may be omitted on ingress.
    #[acyclic_contract_derive::oneof(error = DomainError, file = ActorsFile)]
    #[derive(Clone, Debug, PartialEq, TS)]
    #[ts(
        tag = "case",
        content = "value",
        rename_all = "camelCase",
        export_to = "actors/SendPayload.ts",
        rename = "SendPayload"
    )]
    pub enum Payload {
        /// UTF-8 text.
        #[wire(tag = 3, string)]
        Text(String),
        /// Unmodified binary bytes.
        #[wire(tag = 4, bytes)]
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
