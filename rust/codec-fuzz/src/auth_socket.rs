//! Exercise the grant parser at its actual destination-matching boundary.
use acyclic_auth::{
    contract::{
        SocketHostRule, SocketPortRange, WebsocketConnectGrant, WebsocketConnectGrantProto,
    },
    socket::{Destination, Host, allows, valid_dns_name, valid_rule},
};
use prost::Message;
use std::net::IpAddr;

fn rule(grant: &WebsocketConnectGrant, host: Host<'_>, port: u16) -> bool {
    allows(grant, Destination { host, port })
}

fn admitted(value: WebsocketConnectGrant) {
    for entry in value.rules {
        let port = entry.ports.first().map_or(443, |ports| {
            u16::try_from(ports.first).expect("admitted port fits the transport")
        });
        let grant = WebsocketConnectGrant { rules: vec![entry] };
        let entry = &grant.rules[0];
        let host = entry.host.strip_prefix("*.").unwrap_or(&entry.host);
        let network = host.split_once('/').map_or(host, |(network, _)| network);
        if let Ok(ip) = network.parse::<IpAddr>() {
            // An admitted network contains its own address, in both families.
            assert!(rule(&grant, Host::Ip(ip), port));
            assert!(!rule(&grant, Host::Dns(network), port));
            assert!(!rule(&grant, Host::Ip(ip), 0));
        } else if entry.host.starts_with("*.") {
            assert!(!rule(&grant, Host::Dns(host), port));
            let child = format!("child.{host}");
            if valid_dns_name(&child) {
                assert!(rule(&grant, Host::Dns(&child), port));
            }
        } else {
            assert!(rule(&grant, Host::Dns(host), port));
            let equivalent = format!("{}.", host.trim_end_matches('.').to_ascii_uppercase());
            assert!(rule(&grant, Host::Dns(&equivalent), port));
            assert!(!rule(&grant, Host::Dns(host), 0));
        }
        assert!(!rule(&grant, Host::Dns("bad/name"), port));
        let forbidden: u16 = if port == 443 { 80 } else { 443 };
        let restricted = WebsocketConnectGrant {
            rules: vec![SocketHostRule {
                host: entry.host.clone(),
                ports: vec![SocketPortRange {
                    first: u32::from(forbidden),
                    last: u32::from(forbidden),
                }],
            }],
        };
        let destination = network.parse().map_or(Host::Dns(host), Host::Ip);
        assert!(!rule(&restricted, destination, port));
    }
}

pub(super) fn check(bytes: &[u8]) {
    if let Ok(text) = std::str::from_utf8(bytes) {
        let _ = valid_rule(text);
        let grant = WebsocketConnectGrant {
            rules: vec![SocketHostRule {
                host: text.into(),
                ports: vec![],
            }],
        };
        assert_eq!(rule(&grant, Host::Dns(text), 443), valid_dns_name(text));
        assert!(!rule(&grant, Host::Dns(text), 0));
        assert!(!rule(&grant, Host::Dns(text), 80));
    }
    if let Ok(wire) = WebsocketConnectGrantProto::decode(bytes)
        && let Ok(value) = WebsocketConnectGrant::try_from(wire)
    {
        admitted(value);
    }
}

/// Produce real semantic-to-wire grant bytes, requiring positive readmission.
pub fn seeds() -> Vec<(String, Vec<u8>)> {
    let mut seeds = Vec::new();
    for (name, host) in [
        ("dns", "Example.COM."),
        ("suffix", "*.example.com"),
        ("ipv4", "192.0.2.1"),
        ("ipv6", "2001:db8::1"),
        ("cidr4", "192.0.2.129/24"),
        ("cidr6", "2001:db8::1/64"),
    ] {
        for explicit in [false, true] {
            let value = WebsocketConnectGrant {
                rules: vec![SocketHostRule {
                    host: host.into(),
                    ports: if explicit {
                        vec![SocketPortRange {
                            first: 1,
                            last: 65535,
                        }]
                    } else {
                        vec![]
                    },
                }],
            };
            let bytes = WebsocketConnectGrantProto::from(value.clone()).encode_to_vec();
            assert_eq!(
                WebsocketConnectGrant::try_from(
                    WebsocketConnectGrantProto::decode(bytes.as_slice()).unwrap()
                ),
                Ok(value)
            );
            check(&bytes);
            seeds.push((format!("socket-{name}-{}", u8::from(explicit)), bytes));
        }
    }
    seeds
}
