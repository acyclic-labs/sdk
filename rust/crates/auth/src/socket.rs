//! Typed external WebSocket destinations and host/port grant matching.
use crate::contract::WebsocketConnectGrant;
use std::net::IpAddr;

/// Transport-validated DNS name or literal IP. CIDRs match only literal IP facts.
#[derive(Clone, Copy, Debug)]
pub enum Host<'a> {
    /// ASCII DNS name (IDNs are converted to ASCII by the transport).
    Dns(&'a str),
    /// Literal IPv4 or IPv6 destination.
    Ip(IpAddr),
}

/// External connection facts, independent of resource/RPC names.
#[derive(Clone, Copy, Debug)]
pub struct Destination<'a> {
    /// Logical transport destination, prior to connection establishment.
    pub host: Host<'a>,
    /// Nonzero destination port.
    pub port: u16,
}

#[derive(Clone, Copy)]
enum Rule<'a> {
    Dns(&'a str),
    Suffix(&'a str),
    Ip(IpAddr),
    Network(IpAddr, u8),
}
fn dns(name: &str) -> Option<&str> {
    let name = dns_labels(name)?;
    let last = name.rsplit('.').next()?;
    let hex = last.strip_prefix("0x").or_else(|| last.strip_prefix("0X"));
    // URL/resolver IPv4 syntax also admits shorthand, octal, hex and integer
    // hosts. A numeric final label must never enter the DNS grant alternative.
    let numeric = last.bytes().all(|b| b.is_ascii_digit())
        || hex.is_some_and(|digits| digits.bytes().all(|b| b.is_ascii_hexdigit()));
    (!numeric).then_some(name)
}
fn dns_labels(name: &str) -> Option<&str> {
    let name = name.strip_suffix('.').unwrap_or(name);
    // The byte length check is constant-time and precedes every character scan.
    if name.is_empty() || name.len() > 253 {
        return None;
    }
    name.split('.')
        .all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
        .then_some(name)
}

/// Shared DNS admission for socket and egress host grants. IP literals belong
/// in an explicit IP/CIDR alternative, never a DNS name or wildcard suffix.
/// ASCII case and one trailing root dot retain their DNS equivalence.
pub fn valid_dns_name(name: &str) -> bool {
    dns(name).is_some()
}
fn parse_rule(host: &str) -> Option<Rule<'_>> {
    if let Some((network, prefix)) = host.split_once('/') {
        let network = network.parse::<IpAddr>().ok()?;
        if prefix.is_empty() || !prefix.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let prefix = prefix.parse::<u8>().ok()?;
        (prefix <= if network.is_ipv4() { 32 } else { 128 })
            .then_some(Rule::Network(network, prefix))
    } else if let Some(suffix) = host.strip_prefix("*.") {
        let suffix = dns(suffix)?;
        if suffix.parse::<IpAddr>().is_ok() {
            return None;
        }
        Some(Rule::Suffix(suffix))
    } else if let Ok(ip) = host.parse::<IpAddr>() {
        Some(Rule::Ip(ip))
    } else {
        Some(Rule::Dns(dns(host)?))
    }
}
/// Whether a descriptor host rule has an admitted DNS, suffix, IP or CIDR form.
pub fn valid_rule(host: &str) -> bool {
    parse_rule(host).is_some()
}
fn network_contains(network: IpAddr, prefix: u8, ip: IpAddr) -> bool {
    match (network, ip) {
        (IpAddr::V4(network), IpAddr::V4(ip)) => {
            let mask = u32::MAX.checked_shl(32 - u32::from(prefix)).unwrap_or(0);
            u32::from(network) & mask == u32::from(ip) & mask
        }
        (IpAddr::V6(network), IpAddr::V6(ip)) => {
            let mask = u128::MAX.checked_shl(128 - u32::from(prefix)).unwrap_or(0);
            u128::from(network) & mask == u128::from(ip) & mask
        }
        _ => false,
    }
}
fn host_matches(rule: Rule<'_>, host: Host<'_>) -> bool {
    match (rule, host) {
        (Rule::Dns(rule), Host::Dns(host)) => host.eq_ignore_ascii_case(rule),
        (Rule::Suffix(rule), Host::Dns(host)) => {
            host.len() > rule.len() + 1
                && host.is_char_boundary(host.len() - rule.len())
                && host[..host.len() - rule.len()].ends_with('.')
                && host[host.len() - rule.len()..].eq_ignore_ascii_case(rule)
        }
        (Rule::Ip(rule), Host::Ip(ip)) => rule == ip,
        (Rule::Network(network, prefix), Host::Ip(ip)) => network_contains(network, prefix, ip),
        _ => false,
    }
}
/// Union of valid destination rules; empty ports means 443. Grant sets from
/// different certificates and caveat blocks are intersected by authority::permits.
/// No DNS lookup, allocation, or mutable state occurs during matching.
pub fn allows(grant: &WebsocketConnectGrant, destination: Destination<'_>) -> bool {
    allows_rules(&grant.rules, destination, dns)
}
fn allows_rules<'a>(
    rules: &[crate::contract::SocketHostRule],
    destination: Destination<'a>,
    mut validate_dns: impl FnMut(&'a str) -> Option<&'a str>,
) -> bool {
    if destination.port == 0 {
        return false;
    }
    let host = match destination.host {
        Host::Dns(name) => match validate_dns(name) {
            Some(name) => Host::Dns(name),
            None => return false,
        },
        Host::Ip(ip) => Host::Ip(ip),
    };
    rules.iter().any(|rule| {
        let port = u32::from(destination.port);
        (if rule.ports.is_empty() {
            port == 443
        } else {
            rule.ports.iter().any(|range| {
                range.first > 0 && range.first <= port && port <= range.last && range.last <= 65535
            })
        }) && parse_rule(&rule.host).is_some_and(|rule| host_matches(rule, host))
    })
}

#[cfg(test)]
mod tests;
