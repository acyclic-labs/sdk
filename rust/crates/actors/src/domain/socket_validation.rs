use super::*;
use std::net::IpAddr;

/// Maximum bytes in a socket application frame.
pub const MAX_SOCKET_FRAME_BYTES: usize = 1_048_576;
/// Maximum checkpointed attachment bytes per connection.
pub const MAX_SOCKET_ATTACHMENT_BYTES: usize = 2048;
/// Maximum runtime-owned connections per actor.
pub const MAX_SOCKET_CONNECTIONS: usize = 16_384;
/// Maximum separate broadcast tags per connection.
pub const MAX_SOCKET_TAGS: usize = 8;
/// Maximum UTF-8 bytes in each exact auto-response string.
pub const MAX_SOCKET_AUTO_RESPONSE_BYTES: usize = 2048;

pub(super) fn socket_require(valid: bool) -> Result<(), DomainError> {
    valid.then_some(()).ok_or(DomainError::InvalidSocket)
}
pub(super) fn valid_socket_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}
pub(super) fn unique<T: Eq + std::hash::Hash>(values: &[T]) -> bool {
    let mut seen = std::collections::HashSet::new();
    values.iter().all(|value| seen.insert(value))
}
pub(super) fn unique_by<T, K: Eq + std::hash::Hash>(values: &[T], key: impl Fn(&T) -> K) -> bool {
    let mut seen = std::collections::HashSet::new();
    values.iter().all(|value| seen.insert(key(value)))
}
pub(super) fn valid_tag(value: &str) -> bool {
    !value.is_empty() && value.len() <= 64 && !value.chars().any(char::is_control)
}
pub(super) fn valid_tags(values: &[String]) -> bool {
    values.len() <= MAX_SOCKET_TAGS && values.iter().all(|value| valid_tag(value)) && unique(values)
}
pub(super) fn valid_socket_host_rule(value: &str) -> bool {
    if value.parse::<IpAddr>().is_ok() || value.parse::<ipnet::IpNet>().is_ok() {
        return true;
    }
    let host = value.strip_prefix("*.").unwrap_or(value);
    !host.is_empty()
        && host.len() <= 253
        && host.is_ascii()
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
        && host.parse::<IpAddr>().is_err()
}
pub(super) fn valid_socket_url(value: &str) -> bool {
    value.len() <= 8192
        && url::Url::parse(value).is_ok_and(|url| {
            (url.scheme() == "wss" || (url.scheme() == "ws" && loopback_host(&url)))
                && url.host().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.fragment().is_none()
                && url.port_or_known_default().is_some_and(|port| port > 0)
        })
}
fn loopback_host(url: &url::Url) -> bool {
    match url.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        Some(url::Host::Domain(host)) => host == "localhost",
        None => false,
    }
}
pub(super) fn valid_protocol(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
}
pub(super) fn valid_subprotocols(values: &[String]) -> bool {
    values.len() <= 16 && values.iter().all(|value| valid_protocol(value)) && unique(values)
}
pub(super) fn valid_header_name(value: &str) -> bool {
    valid_protocol(value)
}
pub(super) fn valid_close(code: u32, reason: &str) -> bool {
    (matches!(code, 1000..=1003 | 1007..=1014) || (3000..=4999).contains(&code))
        && reason.len() <= 123
}

fn restricted(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_link_local()
                || ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_broadcast()
                || ip.is_multicast()
        }
        IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or_else(
            || {
                ip.is_loopback()
                    || ip.is_unique_local()
                    || ip.is_unicast_link_local()
                    || ip.is_unspecified()
                    || ip.is_multicast()
            },
            |ip| restricted(IpAddr::V4(ip)),
        ),
    }
}
impl SocketHostRule {
    fn permits_port(&self, port: u16) -> bool {
        if self.ports().is_empty() {
            return port == 443;
        }
        self.ports()
            .iter()
            .any(|range| (range.first()..=range.last()).contains(&u32::from(port)))
    }
    fn permits_ip(&self, ip: IpAddr, port: u16) -> bool {
        self.permits_port(port)
            && (self
                .host()
                .parse::<IpAddr>()
                .is_ok_and(|allowed| allowed == ip)
                || self
                    .host()
                    .parse::<ipnet::IpNet>()
                    .is_ok_and(|network| network.contains(&ip)))
    }
    fn permits_host(&self, host: &str, port: u16) -> bool {
        self.permits_port(port)
            && (self.host() == host
                || self.host().strip_prefix("*.").is_some_and(|suffix| {
                    host.strip_suffix(suffix)
                        .is_some_and(|prefix| prefix.ends_with('.') && prefix.len() > 1)
                }))
    }
}
impl SocketConnectCapability {
    /// Stable capability name in actor bindings.
    pub const CAPABILITY: &str = "websocket.connect";

    /// Validates a dial including every resolved IP. The dialer must use these checked
    /// addresses, and repeat this check for redirects and reconnects to prevent DNS rebinding.
    pub fn permits(&self, request: &Connect, resolved: &[IpAddr], local: bool) -> bool {
        if request.binding() != self.binding() || resolved.is_empty() {
            return false;
        }
        let connect::Target::Url(target) = request.target() else {
            return false;
        };
        let Ok(url) = url::Url::parse(target) else {
            return false;
        };
        if url.scheme() != "wss"
            && !(local
                && url.scheme() == "ws"
                && loopback_host(&url)
                && resolved.iter().all(IpAddr::is_loopback))
        {
            return false;
        }
        let Some(port) = url.port_or_known_default() else {
            return false;
        };
        let Some(host) = url.host() else {
            return false;
        };
        let allowed_host = match host {
            url::Host::Domain(host) => self
                .rules()
                .iter()
                .any(|rule| rule.permits_host(host, port)),
            url::Host::Ipv4(ip) => {
                resolved.iter().all(|value| *value == IpAddr::V4(ip))
                    && self
                        .rules()
                        .iter()
                        .any(|rule| rule.permits_ip(IpAddr::V4(ip), port))
            }
            url::Host::Ipv6(ip) => {
                resolved.iter().all(|value| *value == IpAddr::V6(ip))
                    && self
                        .rules()
                        .iter()
                        .any(|rule| rule.permits_ip(IpAddr::V6(ip), port))
            }
        };
        allowed_host
            && resolved.iter().all(|ip| {
                !restricted(*ip) || self.rules().iter().any(|rule| rule.permits_ip(*ip, port))
            })
    }
}
impl Default for SocketConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: MAX_SOCKET_FRAME_BYTES as u32,
            max_connections: 1024,
            inbound_buffer_bytes: 1_048_576,
            inbound_buffer_frames: 1024,
            actor_buffer_bytes: 16_777_216,
        }
    }
}
impl SocketConfig {
    /// Whether a frame fits this actor's configured maximum.
    pub fn admits_payload(&self, payload: &SocketPayload) -> bool {
        payload.len() <= self.max_frame_bytes() as usize
    }
}
/// Serializes a structured attachment under the canonical byte bound.
pub fn serialize_attachment<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, DomainError> {
    let bytes = serde_json::to_vec(value).map_err(|_| DomainError::InvalidSocket)?;
    socket_require(bytes.len() <= MAX_SOCKET_ATTACHMENT_BYTES)?;
    Ok(bytes)
}
/// Decodes a bounded structured attachment.
pub fn deserialize_attachment<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, DomainError> {
    socket_require(bytes.len() <= MAX_SOCKET_ATTACHMENT_BYTES)?;
    serde_json::from_slice(bytes).map_err(|_| DomainError::InvalidSocket)
}
