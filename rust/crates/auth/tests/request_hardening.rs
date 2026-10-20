//! Request canonicalization and unambiguous DNS/IP grant admission properties.
mod common;
#[path = "common/limits.rs"]
mod limits;
use acyclic_auth::{authority::*, contract::*, socket::*, valid_resource};
use common::*;
use limits::limits;
use proptest::prelude::*;
use prost::Message;
use std::net::{IpAddr, Ipv4Addr};

#[path = "common/dns_hardening.rs"]
mod dns_properties;
#[path = "common/request_hardening.rs"]
mod request_properties;
use dns_properties::numeric_name;
use request_properties::{invalid_name, request};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn noncanonical_resources_and_audiences_never_match(name in invalid_name(), child in any::<bool>()) {
        request_properties::request_property(&name, child, permits)?;
    }

    #[test]
    fn arbitrary_noncanonical_request_facts_are_typed_denials(resource in any::<String>(), audience in any::<String>()) {
        let blocks = vec![root()]; let attributes = RecordAttributes::default(); let usage = limits(None);
        let result = permits(&blocks, &request(&resource, &audience, &attributes, &usage), PolicyDecision::Allow);
        if !valid_resource(&resource) { prop_assert_eq!(result, Err(Deny::NonCanonicalResource)); }
        else if !valid_resource(&audience) { prop_assert_eq!(result, Err(Deny::NonCanonicalAudience)); }
    }

    #[test]
    fn syntax_validation_and_component_decoding_agree(component in any::<String>().prop_filter("single component", |value| !value.contains('/'))) {
        let resource = format!("fs:/{component}");
        prop_assert_eq!(valid_resource(&resource), acyclic_auth::decode_component(&component).is_ok());
    }

    #[test]
    fn numeric_dns_names_are_rejected_by_socket_and_egress_admission(
        name in numeric_name(),
    ) {
        dns_properties::dns_property(&name, valid_dns_name)?;
        // A plain canonical IP is an explicit IP rule; every other numeric
        // spelling must fail rather than falling through to the DNS branch.
        prop_assert_eq!(valid_rule(&name), name.parse::<IpAddr>().is_ok());
        for host in [format!("*.{name}"), format!("prefix.{name}")] {
            prop_assert!(!valid_rule(&host));
            prop_assert!(!valid_dns_name(host.strip_prefix("*.").unwrap_or(&host)));
            let decoded = SocketHostRule::try_from(SocketHostRuleProto { host, ports: vec![] });
            prop_assert!(decoded.is_err());
        }
    }

    #[test]
    fn canonical_dns_and_explicit_ips_round_trip(label in "[a-z][a-z0-9]{0,15}", ip in any::<u32>(), upper in any::<bool>(), trailing in any::<bool>()) {
        let name = format!("{label}.example.com");
        let name = if upper { name.to_ascii_uppercase() } else { name };
        let name = if trailing { format!("{name}.") } else { name };
        prop_assert!(valid_dns_name(&name));
        for host in [name, Ipv4Addr::from(ip).to_string(), Ipv4Addr::from(ip).to_ipv6_mapped().to_string()] {
            let value = SocketHostRule::try_from(SocketHostRuleProto { host, ports: vec![] }).unwrap();
            let bytes = SocketHostRuleProto::from(value.clone()).encode_to_vec();
            prop_assert_eq!(SocketHostRule::try_from(SocketHostRuleProto::decode(bytes.as_slice()).unwrap()), Ok(value));
        }
    }

    #[test]
    fn ipv4_mapped_hosts_require_explicit_ip_facts(ip in any::<u32>()) {
        let mapped = Ipv4Addr::from(ip).to_ipv6_mapped();
        let name = mapped.to_string();
        let grant = WebsocketConnectGrant { rules: vec![SocketHostRule { host: name.clone(), ports: vec![] }] };
        prop_assert!(!valid_dns_name(&name));
        prop_assert!(!allows(&grant, Destination { host: Host::Dns(&name), port: 443 }), "DNS facts cannot authorize a mapped IP");
        prop_assert!(allows(&grant, Destination { host: Host::Ip(IpAddr::V6(mapped)), port: 443 }), "explicit IPv6 must match");
        prop_assert!(!allows(&grant, Destination { host: Host::Ip(IpAddr::V4(Ipv4Addr::from(ip))), port: 443 }), "IP families must not collapse");
    }
}

#[test]
fn numeric_regression_spellings_never_enter_dns_grants() {
    for name in [
        "127.1",
        "0x7f.1",
        "2130706433",
        "0x7f000001",
        "0177.1",
        "0x",
        "example.0X",
        "::ffff:127.0.0.1",
        "[::ffff:127.0.0.1]",
    ] {
        assert!(!valid_dns_name(name), "{name}");
        assert!(!valid_rule(&format!("*.{name}")), "{name}");
    }
    assert!(valid_dns_name("0x1234.example"));
    assert!(valid_dns_name("127.example"));
}

#[test]
fn large_component_lists_validate_without_decoding_buffers() {
    let resource = format!("fs:/{}", vec!["a%2Eb"; 8192].join("/"));
    assert!(valid_resource(&resource));
    let attributes = RecordAttributes::default();
    let usage = limits(None);
    assert_eq!(
        permits(
            &[root()],
            &request(&resource, "workers:/app", &attributes, &usage),
            PolicyDecision::Deny
        ),
        Err(Deny::OwnerPolicy)
    );
    let invalid = format!("{resource}/%61");
    assert_eq!(
        permits(
            &[root()],
            &request(&invalid, "workers:/app", &attributes, &usage),
            PolicyDecision::Allow
        ),
        Err(Deny::NonCanonicalResource)
    );
}
