use super::*;

/// SocketPortRange data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketPortRange::validate)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/SocketPortRange.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketPortRange {
    #[wire(tag = 1, uint32)]
    pub(super) first: u32,
    #[wire(tag = 2, uint32)]
    pub(super) last: u32,
}

impl SocketPortRange {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(self.first > 0 && self.first <= self.last && self.last <= 65535)
    }

    /// Returns first.
    pub fn first(&self) -> u32 {
        self.first
    }
    /// Returns last.
    pub fn last(&self) -> u32 {
        self.last
    }
}

/// SocketHostRule data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketHostRule::validate)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/SocketHostRule.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketHostRule {
    #[wire(tag = 1, string)]
    pub(super) host: String,
    #[wire(tag = 2, message)]
    pub(super) ports: Vec<SocketPortRange>,
}

impl SocketHostRule {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(valid_socket_host_rule(&self.host) && self.ports.len() <= 64)
    }

    /// Returns host.
    pub fn host(&self) -> &str {
        &self.host
    }
    /// Returns ports.
    pub fn ports(&self) -> &[SocketPortRange] {
        &self.ports
    }
}

/// SocketSecretHeader data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketSecretHeader::validate)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/SocketSecretHeader.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketSecretHeader {
    #[wire(tag = 1, string)]
    pub(super) name: String,
    #[wire(tag = 2, string)]
    pub(super) secret_binding: String,
}

impl SocketSecretHeader {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(valid_header_name(&self.name) && valid_socket_id(&self.secret_binding))
    }

    /// Returns name.
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Returns secret binding.
    pub fn secret_binding(&self) -> &str {
        &self.secret_binding
    }
}

/// SocketConnectCapability data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketConnectCapability::validate)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/SocketConnectCapability.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketConnectCapability {
    #[wire(tag = 1, string)]
    pub(super) binding: String,
    #[wire(tag = 2, message)]
    pub(super) rules: Vec<SocketHostRule>,
    #[wire(tag = 3, message)]
    pub(super) headers: Vec<SocketSecretHeader>,
}

impl SocketConnectCapability {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(
            valid_socket_id(&self.binding)
                && !self.rules.is_empty()
                && self.rules.len() <= 64
                && self.headers.len() <= 64
                && unique_by(&self.headers, |header| header.name.to_ascii_lowercase()),
        )
    }

    /// Returns binding.
    pub fn binding(&self) -> &str {
        &self.binding
    }
    /// Returns rules.
    pub fn rules(&self) -> &[SocketHostRule] {
        &self.rules
    }
    /// Returns headers.
    pub fn headers(&self) -> &[SocketSecretHeader] {
        &self.headers
    }
}

/// SocketReconnect data for runtime-owned hibernatable actor sockets.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SocketReconnect::validate)]
#[derive(Clone, Debug, PartialEq, TS)]
#[ts(export_to = "actors/SocketReconnect.ts")]
#[ts(rename_all = "camelCase")]
pub struct SocketReconnect {
    #[wire(tag = 1, enumeration)]
    pub(super) mode: SocketReconnectMode,
    #[wire(tag = 2, uint32)]
    pub(super) initial_ms: u32,
    #[wire(tag = 3, uint32)]
    pub(super) max_ms: u32,
    #[wire(tag = 4, double)]
    pub(super) multiplier: f64,
    #[wire(tag = 5, double)]
    pub(super) jitter: f64,
    #[wire(tag = 6, uint32)]
    pub(super) max_attempts: u32,
}

impl SocketReconnect {
    fn validate(&self) -> Result<(), DomainError> {
        socket_require(match self.mode {
            SocketReconnectMode::None => {
                self.initial_ms == 0
                    && self.max_ms == 0
                    && self.multiplier == 0.0
                    && self.jitter == 0.0
                    && self.max_attempts == 0
            }
            SocketReconnectMode::Backoff => {
                self.initial_ms > 0
                    && self.max_ms >= self.initial_ms
                    && self.max_ms <= 300_000
                    && self.multiplier.is_finite()
                    && (1.0..=16.0).contains(&self.multiplier)
                    && self.jitter.is_finite()
                    && (0.0..=1.0).contains(&self.jitter)
                    && self.max_attempts > 0
            }
            SocketReconnectMode::Unspecified => false,
        })
    }

    /// Returns mode.
    pub fn mode(&self) -> SocketReconnectMode {
        self.mode
    }
    /// Returns initial ms.
    pub fn initial_ms(&self) -> u32 {
        self.initial_ms
    }
    /// Returns max ms.
    pub fn max_ms(&self) -> u32 {
        self.max_ms
    }
    /// Returns multiplier.
    pub fn multiplier(&self) -> f64 {
        self.multiplier
    }
    /// Returns jitter.
    pub fn jitter(&self) -> f64 {
        self.jitter
    }
    /// Returns max attempts.
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }
}
